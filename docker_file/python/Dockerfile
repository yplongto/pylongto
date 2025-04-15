# 使用官方Python基础镜像（可根据需要选择版本）
FROM python:3.12.9

# 设置工作目录
USER root
WORKDIR /usr/src/backend

# 复制依赖文件
COPY requirements.txt .

# 安装依赖（使用清华镜像加速）
RUN pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple

# 设置环境变量（可选）
ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/usr/src/backend

# 启动Python交互环境
#CMD ["python"]