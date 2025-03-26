----------------- 使用Dockerfile管理 ---------------------
# 1. 准备Python环境 生成当前环境的依赖列表
pip freeze > requirements.txt

# 2. 创建Dockerfile

# 3. 构建Docker镜像
docker build -t  .

# 4. 运行Python环境容器 基本运行方式 这会启动一个交互式Python shell
docker run -it --rm my-python-env
# 带卷挂载的运行方式（适合开发）这样可以将主机当前目录挂载到容器的/app目录
docker run -it --rm -v $(pwd):/app my-python-env
# 带端口映射的运行方式（如运行web应用）
docker run -it --rm -p 8000:8000 my-python-env

----------------- 使用docker-compose管理 ---------------------
# 创建docker-compose.yml文件
docker-compose up -d
docker-compose exec python-env python

#6. 验证环境 检查已安装的包
docker run --rm my-python-env pip list

# 检查Python版本：
docker run --rm my-python-env python --version