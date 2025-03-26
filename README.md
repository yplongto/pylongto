# Pylongto

* NOTE: Pylongto服务

### 文件结构

```Text
alembic: 数据库迁移工具
alembic.ini: 数据库迁移工具相关文件
application: 服务开放接口
confs: 系统配置文件
engine: 与数据库（pg, tsdb）和资源接口(resource), 缓存数据（redis, ctg-cache）有关
models: PG数据库ORM映射模型
modules: 扩展的系统模块（Flask-redis）
service: 系统服务的各个模块的具体逻辑实现
static: 页面静态文件
utils: 服务于系统逻辑函数的工具包
env.py: 环境变量相关
run.py: web服务启动文件
gunrun.py: 主服务启动文件
```

**配置文件路径**

```Text
confs/config-pro.conf
confs/config-pre.conf
confs/config-dev.conf

日志配置路径
    confs/log/logging.conf 
```

**默认环境变量**

```shell
ENABLED_MONITOR=True
SERVICE_ENVIRONMENT=pro/pre/dev 
可选修改： DEV， PRE， PRO，根据配置读取config文件
```

**GUNICORN的启动参数，默认值初步给定，线上可调整**：

```shell
GUN_WORKER_CONNECTIONS=2000
WORKERS=4
THREADS=50
GUN_TIMEOUT=180
```

### 环境安装

> 说明：PostgreSQL client development headers (e.g. the libpq-dev package).
>
> sudo apt-get install libpq-dev -y  
> pip install -r requirements.txt



**环境说明**：

**手动迁移** 数据库表结构

参考alembic命令
文档: https://alembic.sqlalchemy.org/en/latest/

自动生成迁移表结构. 请检查生成的文件中 upgrade 选项中是否有 drop_table 操作,
如有请删除响应的drop_table操作，同时downgrade中删除create_table操作;
如没有，请忽略

```shell
alembic revision --autogenerate -m "init model"
```

更新至数据库

```shell
alembic upgrade head
```

初始化基础表数据

```shell
flask init-default-value
```

### **启动方式**

1. 主服务启动入口

```shell
环境变量配置：
export SERVICE_ENV=pro/pre/dev (your environment variable)

启动命令：
cd pylongto
python run.py
or
gunicorn -c gunrun.py run:app --preload
```

**开放端口**

```
50001
```

**接口文档路径**

```
http://localhost:50001/docs
```

### 项目说明

```Text
Pylongto服务
```

**维护相关**：

主服务接口文档与参数说明可备查；

后台任务执行状态与日志，可通过celery服务查看。

