# Brain Integration Service Dockerfile
# AutoTM三层金融交易系统集成协调服务

FROM python:3.13-slim AS base

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV TZ=Asia/Shanghai

# 安装系统依赖（当前无额外系统依赖）

# 构建阶段
FROM base AS builder

WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 升级pip并安装Python依赖
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir --user -r requirements.txt

# 复制本地共享库
COPY external/econdb/ ./external/econdb/
COPY external/asyncron/ ./external/asyncron/

# 安装本地共享库
RUN cd external/econdb && pip install --no-cache-dir --user .
RUN cd external/asyncron && pip install --no-cache-dir --user .

# 复制服务代码
COPY . ./

# 运行阶段
FROM base AS runtime

WORKDIR /app

# 复制Python包
COPY --from=builder /root/.local /root/.local

# 复制应用代码（从构建阶段复制）
COPY --from=builder /app/*.py ./
COPY --from=builder /app/adapters/ ./adapters/
COPY --from=builder /app/coordinators/ ./coordinators/
COPY --from=builder /app/handlers/ ./handlers/
COPY --from=builder /app/managers/ ./managers/
COPY --from=builder /app/monitors/ ./monitors/
COPY --from=builder /app/routers/ ./routers/
COPY --from=builder /app/routes/ ./routes/
COPY --from=builder /app/scheduler/ ./scheduler/
COPY --from=builder /app/validators/ ./validators/
COPY --from=builder /app/initializers/ ./initializers/

# 创建必要的目录
RUN mkdir -p logs data config

# 设置环境变量
ENV PATH=/root/.local/bin:$PATH
ENV PYTHONPATH=/app

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8088/health')"]

# 暴露端口
EXPOSE 8088

# 启动命令
CMD ["python", "main.py"]
