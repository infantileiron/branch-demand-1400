import os,json,uuid,math,logging
from pathlib import Path
from fastapi import HTTPException
from sqlalchemy import update
from src.models.entities import ForecastRun
from src.services.analysis import forecast
from src.constants.enums import QUEUE_KEY

def artifact_dir():return Path(os.environ.get('ARTIFACT_DIR','/artifacts'))
def artifact_path(run_id):return artifact_dir()/(str(run_id)+'.json')

def atomic_write_json(path,payload):
 # 临时文件同目录落盘并 fsync，再原子 rename：任何中断都不会留下半写结果文件
 path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
 tmp=path.with_name(path.name+'.tmp.'+uuid.uuid4().hex)
 with open(tmp,'w',encoding='utf-8') as f:
  json.dump(payload,f,ensure_ascii=False);f.flush();os.fsync(f.fileno())
 os.replace(tmp,path)

def claim_run(s,run_id):
 """并发接单围栏：仅 queued 可被接单，条件更新保证多个计算进程只有一个抢到。
 返回 (row,token)；重复投递、已被接单或已确认的预测返回 (None,None)。"""
 token=uuid.uuid4().hex
 # 条件 UPDATE 依赖行锁，SQLite/PostgreSQL 均保证同 run 同时只有一个条件更新命中
 result=s.execute(update(ForecastRun).where(ForecastRun.id==run_id,ForecastRun.state=='queued')
                  .values(state='running',run_token=token,attempts=ForecastRun.attempts+1))
 s.commit()
 if result.rowcount!=1:return None,None
 row=s.get(ForecastRun,run_id)
 return row,token

def finalize_run(s,row,token,result):
 """写回结果。token 不匹配说明本进程已被恢复流程取代（迟到旧进程），拒绝覆盖。
 先原子写文件，再条件更新数据库，保证文件不会晚于完成状态缺失。"""
 if not row or row.run_token!=token or row.state!='running':
  logging.warning('run %s 迟到写回被拒绝（token/state 不匹配）',getattr(row,'id',None));return False
 path=artifact_path(row.id)
 payload={k:v for k,v in result.items() if k!='snapshot'}
 atomic_write_json(path,payload)
 updated=s.execute(update(ForecastRun).where(ForecastRun.id==row.id,ForecastRun.state=='running',ForecastRun.run_token==token)
                   .values(result=result,state='done',error='',verification='pending',verify_detail={}))
 s.commit()
 if updated.rowcount!=1:
  logging.warning('run %s 完成写库被围栏拦截，文件保留待校验',row.id);return False
 fresh=s.get(ForecastRun,row.id)
 ok,detail=verify_artifact(fresh)
 fresh.verification='verified' if ok else 'corrupt';fresh.verify_detail=detail
 s.commit();return ok

def fail_run(s,row,token,message):
 # 失败写回同样受围栏保护，迟到进程不得改写已确认预测
 updated=s.execute(update(ForecastRun).where(ForecastRun.id==row.id,ForecastRun.state=='running',ForecastRun.run_token==token)
                   .values(state='failed',error=message[:2000]))
 s.commit();return updated.rowcount==1

def _close(a,b):
 if isinstance(a,float) or isinstance(b,float):
  try:return math.isclose(float(a),float(b),rel_tol=1e-12,abs_tol=1e-12)
  except (TypeError,ValueError):return False
 return a==b

def _compare(expect,actual,path,diffs):
 if type(expect)!=type(actual):diffs.append(f'{path}: 类型不符');return
 if isinstance(expect,dict):
  for k in set(expect)|set(actual):
   if k not in actual:diffs.append(f'{path}.{k}: 文件缺失')
   elif k not in expect:diffs.append(f'{path}.{k}: 文件多余')
   else:_compare(expect[k],actual[k],f'{path}.{k}',diffs)
 elif isinstance(expect,list):
  if len(expect)!=len(actual):diffs.append(f'{path}: 长度不符 库{len(expect)}/文件{len(actual)}');return
  for i,(a,b) in enumerate(zip(expect,actual)):_compare(a,b,f'{path}[{i}]',diffs)
 elif not _close(expect,actual):diffs.append(f'{path}: 库={expect!r} 文件={actual!r}')

def verify_artifact(row):
 """逐日核对两种预测的文件内容与库内保存值；返回 (ok, detail)。"""
 if not row or not row.result or 'snapshot' not in row.result:
  return False,{'reason':'库内冻结结果缺失或不完整'}
 path=artifact_path(row.id)
 if not path.exists():return False,{'reason':'结果文件缺失','path':str(path)}
 try:
  with open(path,encoding='utf-8') as f:on_disk=json.load(f)
 except (json.JSONDecodeError,OSError) as e:return False,{'reason':f'结果文件损坏无法读取：{e}'}
 expect={k:v for k,v in row.result.items() if k!='snapshot'}
 diffs=[]
 # 顶层键集合必须一致：文件缺字段或多字段都视为损坏
 for k in set(expect)|set(on_disk):
  if k not in on_disk:diffs.append(f'{k}: 文件缺失')
  elif k not in expect:diffs.append(f'{k}: 文件多余')
 # 先核对冻结 checksum，再逐日核对回测与两种模型未来预测
 _compare(expect.get('checksum'),on_disk.get('checksum'),'checksum',diffs)
 for section in ('backtest','history'):_compare(expect.get(section,[]),on_disk.get(section,[]),section,diffs)
 for model in ('seasonal_naive','holt'):
  ef=expect.get('future',{}).get(model,[]);af=on_disk.get('future',{}).get(model)
  if af is None:diffs.append(f'future.{model}: 文件缺失');continue
  if len(ef)!=len(af):diffs.append(f'future.{model}: 天数不符 库{len(ef)}/文件{len(af)}');continue
  for i,(e,a) in enumerate(zip(ef,af)):
   for key in ('day','prediction','raw','clipped'):_compare(e.get(key),a.get(key),f'future.{model}[{i}].{key}',diffs)
 for key in ('metrics','selected_model','n','backtest_days','alpha','beta','backtest_clipped','train_initial'):
  _compare(expect.get(key),on_disk.get(key),key,diffs)
 if diffs:return False,{'reason':'文件与库内保存值不符','differences':diffs[:20]}
 return True,{'reason':'逐日核对相符','future_days':len(expect.get('future',{}).get('holt',[])),'checksum':expect.get('checksum')}

def ensure_verified(row):
 """导出与排班起草入口的统一门控。"""
 if not row:raise HTTPException(404,'预测版本不存在')
 if row.state!='done':raise HTTPException(409,'预测完成后才能开放该入口')
 if row.verification=='corrupt':raise HTTPException(409,'结果文件缺失或损坏，入口已暂停，请先修复原预测')
 if row.verification!='verified':raise HTTPException(409,'预测正在逐日核对，请稍后刷新')
 return row

def repair_run(s,run_id,queue):
 """修复原预测，不另建预测。
 queued/running/failed：重置为 queued 并按原 run id 重新入队，按冻结快照与参数重算；
 done：优先用库内冻结保存值重写文件；库内值也损坏时按冻结快照重算并先与保存值比对，同号异内容拒绝。"""
 row=s.get(ForecastRun,run_id)
 if not row:raise HTTPException(404,'预测版本不存在')
 if row.state in ('queued','running','failed'):
  row.state='queued';row.run_token='';row.verification='none';row.verify_detail={};row.error=''
  s.commit();queue.rpush(QUEUE_KEY,row.id)
  return {'action':'requeued','run_id':row.id,'state':'queued'}
 # done：文件与保存值脱节的修复
 ok,detail=verify_artifact(row)
 if ok:
  row.verification='verified';row.verify_detail=detail;s.commit();return {'action':'noop','run_id':row.id,'verification':'verified'}
 snapshot=(row.result or {}).get('snapshot')
 if not snapshot:raise HTTPException(409,'缺少冻结快照，无法修复原预测')
 recomputed={**forecast(snapshot,row.backtest_days,row.alpha,row.beta),'snapshot':snapshot}
 if row.result and 'checksum' in row.result:
  # 同号异内容保护：重算结果必须与已确认保存值一致，否则拒绝覆盖
  same_checksum=recomputed['checksum']==row.result.get('checksum')
  same_backtest=recomputed.get('backtest')==row.result.get('backtest')
  same_future=recomputed.get('future')==row.result.get('future')
  if not (same_checksum and same_backtest and same_future):
   row.verification='corrupt';row.verify_detail={'reason':'重算结果与已确认保存值不一致，拒绝覆盖（同号异内容）'}
   s.commit();raise HTTPException(409,'冻结参数重算结果与已确认预测不符，已拒绝覆盖，请人工核对')
 row.result=recomputed;row.verification='pending';row.verify_detail={};s.commit()
 atomic_write_json(artifact_path(row.id),{k:v for k,v in recomputed.items() if k!='snapshot'})
 ok,detail=verify_artifact(row)
 row.verification='verified' if ok else 'corrupt';row.verify_detail=detail;s.commit()
 if not ok:raise HTTPException(409,'修复后校验仍未通过：'+detail.get('reason','未知原因'))
 return {'action':'rewritten','run_id':row.id,'verification':'verified'}
