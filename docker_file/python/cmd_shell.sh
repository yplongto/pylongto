----------------- 使用Dockerfile管理 ---------------------
# 1. 准备Python环境 生成当前环境的依赖列表
pip freeze > requirements.txt

# 2. 创建Dockerfile

# 3. 构建Docker镜像
docker build -t python3.12.9 .

# 启动并挂载目录
docker run -v /宿主机路径:/容器路径 python:3.12.9
docker run -d --name python-app -v /opt/pylongto/app:/usr/src/backend python:3.12.9
# 挂载单个文件
docker run -v /宿主机文件:/容器文件 python:3.12.9
docker run -it --name python-app -v /opt/pylongto/app/config.json:/usr/src/backend/config.json python:3.12.9 bash
# 只读挂载	docker run --mount type=bind,source=...,target=...,readonly python:3.12.9
docker run -d \
  --name python-app \
  --mount type=bind,source=/opt/pylongto/app,target=/usr/src/backend,readonly \
  python:3.12.9
端口映射	docker run -p 8000:8000 python:3.12.9

#进入容器bash
docker exec -it python-app bash

----------------- 使用docker-compose管理 ---------------------
# 创建docker-compose.yml文件
docker-compose up -d
docker-compose exec python-env python

#6. 验证环境 检查已安装的包
docker run --rm my-python-env pip list

# 检查Python版本：
docker run --rm my-python-env python --version