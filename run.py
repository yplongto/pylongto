#!/usr/bin/env python3
# -*- coding: utf8 -*-
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

from application import create_app, create_table
from env import LOG_LEVEL

app = create_app()
origins = ['*']
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# create_table()


@app.get("/custom_openapi")
def custom_openapi(target_path: str):
    """
    获取单个接口的swagger json
    """
    if app.openapi_schema:
        return app.openapi_schema

    # 生成完整的 OpenAPI 文档
    openapi_schema = app.openapi()
    # 筛选所需的路径（例如："/items/{item_id}"）
    single_path_schema = {
        "openapi": openapi_schema["openapi"],
        "info": openapi_schema["info"],
        "paths": {target_path: openapi_schema["paths"][target_path]},
        "components": openapi_schema.get("components", {})
    }

    # 缓存并返回筛选后的文档
    app.openapi_schema = single_path_schema
    return app.openapi_schema


if __name__ == '__main__':
    uvicorn.run(app="run:app", host='0.0.0.0', port=50001, workers=4, reload=True, log_level=LOG_LEVEL)
