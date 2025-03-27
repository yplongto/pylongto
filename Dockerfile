FROM harbor.digital.zjgc/yp/python:3.12.9

ENV TZ=Asia/Shanghai DEBIAN_FRONTEND=noninteractive

USER root
WORKDIR /usr/src/backend
COPY . .

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone && \
    pip install --upgrade pip -i https://mirror.sjtu.edu.cn/pypi/web/simple && \
    pip install --no-cache-dir -r requirements.txt -i https://mirror.sjtu.edu.cn/pypi/web/simple

# 单独安装修改包
RUN pip install package/python_ulid-2.7.0.1.tar.gz -i https://mirror.sjtu.edu.cn/pypi/web/simple

EXPOSE 50001

ENTRYPOINT ["sh", "./startup.sh"]
CMD ["RUN"]