"""
服务注册表

管理所有外部服务的连接信息和状态。
"""

import asyncio
import logging
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

import aiohttp

from config import IntegrationConfig
from exceptions import AdapterException


class ServiceRegistry:
    """服务注册表"""
    
    def __init__(self, config: IntegrationConfig):
        """初始化服务注册表
        
        Args:
            config: 集成配置对象
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self._services = {}
        self._session: Optional[aiohttp.ClientSession] = None
        self._is_running = False
        
        # 初始化服务配置
        self._init_services()
    
    def _init_services(self):
        """初始化服务配置"""
        self._services = {
            'macro': {
                'name': 'macro',
                'url': self.config.service.macro_service_url,
                'health_endpoint': '/health',
                'api_prefix': '/api/v1/macro',
                'status': 'unknown',
                'last_check': None,
                'error_count': 0
            },
            'portfolio': {
                'name': 'portfolio',
                'url': self.config.service.portfolio_service_url,
                'health_endpoint': '/api/v1/health',
                'api_prefix': '/api/v1/portfolios',
                'status': 'unknown',
                'last_check': None,
                'error_count': 0
            },
            'execution': {
                'name': 'execution',
                'url': self.config.service.execution_service_url,
                'health_endpoint': '/health',
                'api_prefix': '/api/v1/execution',
                'status': 'unknown',
                'last_check': None,
                'error_count': 0
            },
            'flowhub': {
                'name': 'flowhub',
                'url': self.config.service.flowhub_service_url,
                'health_endpoint': '/health',
                'api_prefix': '/api/v1/jobs',
                'status': 'unknown',
                'last_check': None,
                'error_count': 0
            }
        }
    
    async def start(self):
        """启动服务注册表"""
        if self._is_running:
            return
        
        # 创建HTTP会话
        timeout = aiohttp.ClientTimeout(total=self.config.adapter.request_timeout)
        connector = aiohttp.TCPConnector(
            limit=self.config.adapter.pool_size,
            limit_per_host=10
        )
        
        self._session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector
        )
        
        self._is_running = True
        
        # 启动健康检查任务
        asyncio.create_task(self._health_check_loop())
        
        self.logger.info("Service registry started")
    
    async def stop(self):
        """停止服务注册表"""
        if not self._is_running:
            return
        
        self._is_running = False
        
        if self._session:
            await self._session.close()
            self._session = None
        
        self.logger.info("Service registry stopped")
    
    async def get_all_services(self) -> Dict[str, Any]:
        """获取所有服务信息
        
        Returns:
            Dict[str, Any]: 服务信息字典
        """
        # 将不可序列化的字段（如datetime）转换为可序列化格式
        services_list = []
        for s in self._services.values():
            sd = s.copy()
            lc = sd.get('last_check')
            if isinstance(lc, datetime):
                sd['last_check'] = lc.isoformat()
            services_list.append(sd)
        return {
            'services': services_list,
            'total_count': len(self._services),
            'healthy_count': len([s for s in self._services.values() if s['status'] == 'healthy']),
            'unhealthy_count': len([s for s in self._services.values() if s['status'] == 'unhealthy'])
        }

    async def get_service_status(self, service_name: str) -> Dict[str, Any]:
        """获取服务状态
        
        Args:
            service_name: 服务名称
            
        Returns:
            Dict[str, Any]: 服务状态信息
        """
        if service_name not in self._services:
            raise AdapterException(f"Service '{service_name}' not found")
        
        service = self._services[service_name]
        
        # 执行实时健康检查
        await self._check_service_health(service_name)

        # 返回可序列化拷贝
        sd = service.copy()
        lc = sd.get('last_check')
        if isinstance(lc, datetime):
            sd['last_check'] = lc.isoformat()
        return sd

    async def health_check_service(self, service_name: str) -> Dict[str, Any]:
        """执行服务健康检查
        
        Args:
            service_name: 服务名称
            
        Returns:
            Dict[str, Any]: 健康检查结果
        """
        if service_name not in self._services:
            raise AdapterException(f"Service '{service_name}' not found")
        
        return await self._check_service_health(service_name)
    
    async def reconnect_service(self, service_name: str) -> Dict[str, Any]:
        """重连服务
        
        Args:
            service_name: 服务名称
            
        Returns:
            Dict[str, Any]: 重连结果
        """
        if service_name not in self._services:
            raise AdapterException(f"Service '{service_name}' not found")
        
        service = self._services[service_name]
        service['error_count'] = 0
        service['status'] = 'unknown'
        
        # 执行健康检查
        result = await self._check_service_health(service_name)
        
        return {
            'service': service_name,
            'reconnected': True,
            'status': result['status'],
            'timestamp': datetime.utcnow().isoformat()
        }
    
    async def get_service_config(self, service_name: str) -> Dict[str, Any]:
        """获取服务配置
        
        Args:
            service_name: 服务名称
            
        Returns:
            Dict[str, Any]: 服务配置信息
        """
        if service_name not in self._services:
            raise AdapterException(f"Service '{service_name}' not found")
        
        service = self._services[service_name]
        return {
            'name': service['name'],
            'url': service['url'],
            'health_endpoint': service['health_endpoint'],
            'api_prefix': service['api_prefix']
        }
    
    async def _check_service_health(self, service_name: str) -> Dict[str, Any]:
        """检查单个服务健康状态
        
        Args:
            service_name: 服务名称
            
        Returns:
            Dict[str, Any]: 健康检查结果
        """
        service = self._services[service_name]
        health_url = f"{service['url']}{service['health_endpoint']}"
        
        try:
            start_time = datetime.utcnow()
            
            async with self._session.get(health_url) as response:
                response_time = (datetime.utcnow() - start_time).total_seconds() * 1000
                
                if response.status == 200:
                    service['status'] = 'healthy'
                    service['error_count'] = 0
                    result = {
                        'service': service_name,
                        'status': 'healthy',
                        'response_time': response_time,
                        'timestamp': datetime.utcnow().isoformat()
                    }
                else:
                    service['status'] = 'unhealthy'
                    service['error_count'] += 1
                    result = {
                        'service': service_name,
                        'status': 'unhealthy',
                        'error': f"HTTP {response.status}",
                        'response_time': response_time,
                        'timestamp': datetime.utcnow().isoformat()
                    }
        
        except Exception as e:
            service['status'] = 'unhealthy'
            service['error_count'] += 1
            result = {
                'service': service_name,
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.utcnow().isoformat()
            }
        
        service['last_check'] = datetime.utcnow()
        return result
    
    async def _health_check_loop(self):
        """健康检查循环"""
        while self._is_running:
            try:
                # 检查所有服务
                for service_name in self._services:
                    await self._check_service_health(service_name)
                
                # 等待下次检查
                await asyncio.sleep(self.config.adapter.health_check_interval)
                
            except Exception as e:
                self.logger.error(f"Health check loop error: {e}")
                await asyncio.sleep(10)  # 错误时短暂等待
