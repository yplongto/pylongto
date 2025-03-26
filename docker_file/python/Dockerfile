# 使用官方Python基础镜像（可根据需要选择版本）
FROM python:3.12.9-slim

# 设置工作目录
WORKDIR /app

# 复制依赖文件
COPY requirements.txt .

# 安装依赖（使用清华镜像加速）
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 设置环境变量（可选）
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app

# 启动Python交互环境
CMD ["python"]