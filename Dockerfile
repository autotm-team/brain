# Brain Integration Service Dockerfile
# AutoTM三层金融交易系统集成协调服务

FROM python:3.13-slim AS build-base
ARG APT_MIRROR=deb.debian.org
ARG PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
ARG PIP_TRUSTED_HOST=pypi.tuna.tsinghua.edu.cn
ARG PIP_EXTRA_INDEX_URL=https://pypi.org/simple

# 设置环境变量
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV TZ=Asia/Shanghai
ENV PIP_INDEX_URL=${PIP_INDEX_URL}
ENV PIP_TRUSTED_HOST=${PIP_TRUSTED_HOST}
ENV PIP_EXTRA_INDEX_URL=${PIP_EXTRA_INDEX_URL}

# 安装系统依赖（Python 3.13 下部分科学计算包会触发源码构建，需编译器）
RUN set -eux; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      sed -i -E "s|https?://deb.debian.org/debian-security|https://${APT_MIRROR}/debian-security|g; s|https?://deb.debian.org/debian|https://${APT_MIRROR}/debian|g" /etc/apt/sources.list.d/debian.sources; \
    fi; \
    if [ -f /etc/apt/sources.list ]; then \
      sed -i -E "s|https?://deb.debian.org/debian-security|https://${APT_MIRROR}/debian-security|g; s|https?://deb.debian.org/debian|https://${APT_MIRROR}/debian|g" /etc/apt/sources.list; \
    fi; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get update; \
    apt-get install -y --no-install-recommends --fix-missing \
      build-essential \
      gcc \
      g++ \
      python3-setuptools \
      ca-certificates; \
    apt-get -f install -y; \
    rm -rf /var/lib/apt/lists/*

# 构建阶段
FROM build-base AS builder

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
RUN pip install --no-cache-dir --user ./external/econdb ./external/asyncron

# 复制服务代码
COPY . ./

# 运行阶段
FROM python:3.13-slim AS runtime
ARG APT_MIRROR=deb.debian.org

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONPATH=/app
ENV TZ=Asia/Shanghai

RUN set -eux; \
    if [ -f /etc/apt/sources.list.d/debian.sources ]; then \
      sed -i -E "s|https?://deb.debian.org/debian-security|https://${APT_MIRROR}/debian-security|g; s|https?://deb.debian.org/debian|https://${APT_MIRROR}/debian|g" /etc/apt/sources.list.d/debian.sources; \
    fi; \
    if [ -f /etc/apt/sources.list ]; then \
      sed -i -E "s|https?://deb.debian.org/debian-security|https://${APT_MIRROR}/debian-security|g; s|https?://deb.debian.org/debian|https://${APT_MIRROR}/debian|g" /etc/apt/sources.list; \
    fi; \
    export DEBIAN_FRONTEND=noninteractive; \
    apt-get update; \
    apt-get install -y --no-install-recommends --fix-missing ca-certificates libpq5; \
    rm -rf /var/lib/apt/lists/*

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
COPY --from=builder /app/validators/ ./validators/
COPY --from=builder /app/initializers/ ./initializers/
COPY --from=builder /app/scripts/ ./scripts/

# 创建必要的目录
RUN mkdir -p logs data config

# 设置环境变量
ENV PATH=/root/.local/bin:$PATH

# 健康检查
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD ["python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8088/health')"]

# 暴露端口
EXPOSE 8088

# 启动命令
CMD ["python", "main.py"]
