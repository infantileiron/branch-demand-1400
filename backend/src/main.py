import os,logging
from contextlib import asynccontextmanager
from fastapi import FastAPI,Request
from fastapi.responses import JSONResponse
from sqlalchemy import inspect
from src.config.database import Base,engine,Session
from src.models import entities
from src.routes.api import router
from src.middlewares.request import RequestId,SecurityHeaders
def reconcile_done_runs():
 # 启动核对：结果文件与库内保存值逐日比对，脱节的预测标记 corrupt 并暂停入口；不改动保存值
 from src.models.entities import ForecastRun
 from src.services.recovery import verify_artifact
 with Session() as s:
  for r in s.query(ForecastRun).filter_by(state='done').all():
   ok,d=verify_artifact(r);r.verification='verified' if ok else 'corrupt';r.verify_detail=d
  s.commit()
@asynccontextmanager
async def lifespan(app):
 Base.metadata.create_all(engine)
 # 轻量在线迁移：旧库补新增列，避免手工 DDL（演示项目无在线迁移器）
 inspector=inspect(engine);cols={c['name'] for c in inspector.get_columns('forecast_runs')}
 with engine.begin() as conn:
  for ddl in ("ALTER TABLE forecast_runs ADD COLUMN run_token VARCHAR(40) DEFAULT ''",
              "ALTER TABLE forecast_runs ADD COLUMN verification VARCHAR(20) DEFAULT 'none'",
              "ALTER TABLE forecast_runs ADD COLUMN verify_detail JSON DEFAULT '{}'",
              "ALTER TABLE forecast_runs ADD COLUMN attempts INTEGER DEFAULT 0"):
   col=ddl.split('ADD COLUMN ')[1].split(' ')[0]
   if col not in cols:conn.exec_driver_sql(ddl)
  staff_cols={c['name'] for c in inspector.get_columns('staffings')}
  if 'request_key' not in staff_cols:
   conn.exec_driver_sql("ALTER TABLE staffings ADD COLUMN request_key VARCHAR(64)")
   conn.exec_driver_sql("UPDATE staffings SET request_key='legacy-'||id WHERE request_key IS NULL")
   conn.exec_driver_sql("ALTER TABLE staffings ALTER COLUMN request_key SET NOT NULL")
   conn.exec_driver_sql("ALTER TABLE staffings ADD CONSTRAINT uq_staffing_request_key UNIQUE (request_key)")
 reconcile_done_runs()
 if os.getenv('SEED_DEMO')=='true':
  try:
   from scripts.seed_demo import seed
   seed()
  except Exception:logging.exception('seed failed')
 yield
app=FastAPI(title='服务网点需求预估',lifespan=lifespan)
app.add_middleware(RequestId);app.add_middleware(SecurityHeaders)
app.include_router(router)
@app.get('/health')
def health():
 from sqlalchemy import text
 import redis
 with engine.connect() as c:c.execute(text('SELECT 1'))
 redis.Redis.from_url(os.environ['REDIS_URL']).ping()
 return {'status':'ok'}
@app.exception_handler(Exception)
async def errors(request:Request,exc:Exception):
 logging.exception('request failed');return JSONResponse(status_code=500,content={'detail':'服务暂时异常，请稍后重试'})
