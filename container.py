"""
依赖注入容器

提供统一的依赖管理和注入机制，解决组件间的耦合问题。
"""

import asyncio
import inspect
import logging
from typing import Dict, Type, TypeVar, Callable, Any, Optional

try:
    from .interfaces import (
        ISystemCoordinator, ISignalRouter, IDataFlowManager
    )
    from .coordinators.system_coordinator import SystemCoordinator
    from .routers.signal_router import SignalRouter
    from .managers.data_flow_manager import DataFlowManager
except Exception:
    from interfaces import (
        ISystemCoordinator, ISignalRouter, IDataFlowManager
    )
    from coordinators.system_coordinator import SystemCoordinator
    from routers.signal_router import SignalRouter
    from managers.data_flow_manager import DataFlowManager

logger = logging.getLogger(__name__)

T = TypeVar('T')


class DIContainer:
    """依赖注入容器"""
    
    def __init__(self):
        self._services: Dict[str, Any] = {}
        self._factories: Dict[str, Callable] = {}
        self._singletons: Dict[str, Any] = {}
        self._interfaces: Dict[Type, Type] = {}
    
    def register_singleton(self, interface: Type[T], implementation: Type[T]) -> None:
        """注册单例服务"""
        self._interfaces[interface] = implementation
        
    def register_transient(self, interface: Type[T], factory: Callable[[], T]) -> None:
        """注册瞬态服务"""
        self._factories[interface.__name__] = factory
    
    def register_instance(self, interface: Type[T], instance: T) -> None:
        """注册实例"""
        self._services[interface.__name__] = instance
    
    async def resolve(self, interface: Type[T]) -> T:
        """解析依赖"""
        interface_name = interface.__name__
        
        # 检查是否已有实例
        if interface_name in self._services:
            return self._services[interface_name]
        
        # 检查是否为单例
        if interface in self._interfaces:
            if interface_name in self._singletons:
                return self._singletons[interface_name]
            
            implementation = self._interfaces[interface]
            instance = await self._create_instance(implementation)
            self._singletons[interface_name] = instance
            return instance
        
        # 检查是否有工厂方法
        if interface_name in self._factories:
            factory = self._factories[interface_name]
            return await self._call_factory(factory)
        
        raise ValueError(f"No registration found for {interface_name}")
    
    async def _create_instance(self, cls: Type[T]) -> T:
        """创建实例并注入依赖"""
        # 获取构造函数参数
        sig = inspect.signature(cls.__init__)
        kwargs = {}
        
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
                
            if param.annotation != inspect.Parameter.empty:
                dependency = await self.resolve(param.annotation)
                kwargs[param_name] = dependency
        
        return cls(**kwargs)
    
    async def _call_factory(self, factory: Callable) -> Any:
        """调用工厂方法"""
        if asyncio.iscoroutinefunction(factory):
            return await factory()
        else:
            return factory()


# 全局容器实例
container = DIContainer()


def inject(interface: Type[T]) -> T:
    """依赖注入装饰器"""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            dependency = await container.resolve(interface)
            return await func(dependency, *args, **kwargs)
        return wrapper
    return decorator


class DIServiceRegistry:
    """依赖注入服务注册表"""

    @staticmethod
    async def register_core_services(container: DIContainer, config):
        """注册核心服务"""

        # 注册配置
        container.register_instance(type(config), config)
        
        # 注册核心服务
        container.register_singleton(ISystemCoordinator, SystemCoordinator)
        container.register_singleton(ISignalRouter, SignalRouter)
        container.register_singleton(IDataFlowManager, DataFlowManager)
        
        logger.info("Core services registered successfully")


# 使用示例
async def setup_container(config):
    """设置依赖注入容器"""
    await DIServiceRegistry.register_core_services(container, config)
    return container
