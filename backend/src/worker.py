import os,time,logging
from pathlib import Path
import redis
from src.config import database as dbmod
from src.models.entities import ForecastRun
from src.constants.enums import QUEUE_KEY
from src.services.analysis import forecast
from src.services.recovery import claim_run,finalize_run,fail_run

r=redis.Redis.from_url(os.environ['REDIS_URL'])

def recover():
 # 中断恢复：同一预测按原 id 重新入队，不新建预测。多个 worker 同时启动时重复投递由接单围栏去重
 with dbmod.Session() as recovery:
  pending=recovery.query(ForecastRun).filter(ForecastRun.state.in_(['queued','running'])).all()
  for p in pending:
   p.state='queued';p.run_token=''
  recovery.commit()
  for p in pending:r.rpush(QUEUE_KEY,p.id)
  if pending:logging.warning('恢复 %d 个未完成预测：%s',len(pending),[p.id for p in pending])

def handle(run_id):
 with dbmod.Session() as s:
  row,token=claim_run(s,run_id)
  if row is None:return  # 重复投递或已被其他进程接单/确认
  snapshot=row.result['snapshot']
 try:
  result={**forecast(snapshot,row.backtest_days,row.alpha,row.beta),'snapshot':snapshot}
  with dbmod.Session() as s2:
   row2=s2.get(ForecastRun,run_id)
   finalize_run(s2,row2,token,result)
 except Exception as e:
  logging.exception('forecast run %s failed',run_id)
  with dbmod.Session() as s3:
   row3=s3.get(ForecastRun,run_id)
   if row3 and row3.run_token==token:fail_run(s3,row3,token,str(e))

if __name__=='__main__':
 recover()
 while True:
  Path('/tmp/heartbeat').touch()
  try:
   item=r.blpop(QUEUE_KEY,timeout=3)
   if not item:continue
   handle(int(item[1]))
  except Exception:
   logging.exception('worker');time.sleep(2)
