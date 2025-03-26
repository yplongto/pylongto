#!/usr/bin/env python3
# -*- coding: utf8 -*-
import logging
from sqlalchemy.exc import InvalidRequestError, IllegalStateChangeError

from fastapi import Header, HTTPException, Request
from application.external import postgres_conn
from utils.globals import request_provider


logger = logging.getLogger('crontab')


async def get_token_header(x_token: str = Header()):
    if x_token != "fake-super-secret-token":
        raise HTTPException(status_code=400, detail="X-Token header invalid")


async def get_request_header(request: Request):
    request_provider.request = request


async def get_query_token(token: str):
    if token != "jessica":
        raise HTTPException(status_code=400, detail="No Jessica token provided")


def get_db_session():
    with postgres_conn.session_factory() as session:
        try:
            yield session
            # session.commit()
        except IllegalStateChangeError as e:
            session.rollback()
            logger.warning('IllegalStateChangeError -> ', exc_info=True)
        except InvalidRequestError as e:
            logger.warning('InvalidRequestError -> ', exc_info=True)
        except Exception as e:
            session.rollback()
            logger.warning('ExceptionError -> ', exc_info=True)
            raise
    # finally:
    #     print(dir(session))
    #     if hasattr(session, 'close'):
    #         session.close()
