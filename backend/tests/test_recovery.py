import os,sys,json,uuid
from pathlib import Path
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import fakeredis
from src.config import database as db_mod
from src.models.entities import Base,ForecastRun,Staffing,User
from src.services.analysis import forecast
from src.services.recovery import (claim_run,finalize_run,fail_run,verify_artifact,
                                   ensure_verified,repair_run,artifact_path,atomic_write_json)
from src.services.staffing import draft
from src.schemas.business import StaffingInput

@pytest.fixture()
def setup(tmp_path,monkeypatch):
 monkeypatch.setenv('ARTIFACT_DIR',str(tmp_path/'artifacts'))
 engine=create_engine('sqlite:///'+str(tmp_path/'t.db'))
 Base.metadata.create_all(engine)
 Session=sessionmaker(bind=engine,expire_on_commit=False)
 db_mod.Session=Session;db_mod.engine=engine
 rng_data=_series()
 with Session() as s:
  snap=[{'branch':'川北','day':d,'volume':v} for d,v in rng_data]
  run=ForecastRun(name='测试预测',branch='川北',backtest_days=14,alpha=.6,beta=.2,
                  state='queued',result={'snapshot':snap},verification='none')
  s.add(run);s.commit();run_id=run.id
 q=fakeredis.FakeStrictRedis()
 class Ctx:pass
 c=Ctx();c.Session=Session;c.run_id=run_id;c.q=q;c.snap=snap;c.tmp=tmp_path
 yield c

def _series():
 # 49 个连续日量（满足 14+28=42 的最小窗口之上），确定性
 from datetime import date,timedelta
 base=date(2026,9,6);rows=[]
 for i in range(49):
  d=base+timedelta(days=i);wd=d.weekday()
  rows.append((d.isoformat(),max(0,round(25+i*.1+(10 if wd<5 else -6)+((i*7)%5)-2))))
 return rows

def _compute(c):
 return {**forecast(c.snap,14,.6,.2),'snapshot':c.snap}

# ---------- 1. 正常接单→写文件→校验通过 ----------
def test_happy_path_verified(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id)
  assert row is not None and token
  ok=finalize_run(s,row,token,_compute(c))
  assert ok is True
  r=s.get(ForecastRun,c.run_id)
  assert r.state=='done' and r.verification=='verified'
  assert artifact_path(c.run_id).exists()
  on_disk=json.loads(artifact_path(c.run_id).read_text(encoding='utf-8'))
  assert 'snapshot' not in on_disk  # 文件只放结果，快照在库内冻结
  assert len(on_disk['future']['holt'])==7 and len(on_disk['future']['seasonal_naive'])==7

# ---------- 2. 双进程争抢同一任务：只有一个接单成功，重复投递不重复处理 ----------
def test_concurrent_claim_only_one_wins(setup):
 c=setup
 with c.Session() as s1:
  row1,t1=claim_run(s1,c.run_id)
  assert row1 is not None
  with c.Session() as s2:
   row2,t2=claim_run(s2,c.run_id)  # 第二个进程重复投递
   assert row2 is None and t2 is None
  # 迟到的第二者持空 token 不得写回
  assert finalize_run(s1,row1,'stale-token',_compute(c)) is False
  r=s1.get(ForecastRun,c.run_id);assert r.state=='running' and r.run_token==t1

# ---------- 3. 接单后中断（state=running 时崩溃）：恢复重投后同 run 重算，不新建 ----------
def test_interrupt_after_claim_recovers_same_run(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id)
 # 模拟 worker 崩溃：行停留在 running，无文件
 assert not artifact_path(c.run_id).exists()
 # worker 启动恢复逻辑
 from src import worker as worker_mod
 worker_mod.r=c.q
 worker_mod.recover()
 assert c.q.lrange('demand:runs',0,-1)==[str(c.run_id).encode()]
 with c.Session() as s:
  r=s.get(ForecastRun,c.run_id);assert r.state=='queued' and r.run_token==''
 # 重新接单处理（同一 id）
 item=c.q.blpop('demand:runs')[1]
 worker_mod.handle(int(item))
 with c.Session() as s:
  r=s.get(ForecastRun,c.run_id)
  assert r.state=='done' and r.verification=='verified'
  assert s.query(ForecastRun).count()==1  # 没有另建预测

# ---------- 4. 文件写完后、写库前中断：恢复重算，结果一致并通过校验 ----------
def test_interrupt_after_file_write_before_commit(setup,monkeypatch):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id)
 result=_compute(c)
 # 先写文件，然后在 DB 提交前“崩溃”
 atomic_write_json(artifact_path(c.run_id),{k:v for k,v in result.items() if k!='snapshot'})
 # 库里仍是 running、无 result 完成值
 with c.Session() as s:
  r=s.get(ForecastRun,c.run_id);assert r.state=='running'
 from src import worker as worker_mod
 worker_mod.r=c.q;worker_mod.recover();worker_mod.handle(c.run_id)
 with c.Session() as s:
  r=s.get(ForecastRun,c.run_id)
  assert r.state=='done' and r.verification=='verified'
  assert r.result['checksum']==result['checksum']  # 确定性重算，内容一致

# ---------- 5. 旧进程迟到写回不覆盖已确认预测（fencing） ----------
def test_late_stale_worker_cannot_overwrite(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id)
  finalize_run(s,row,token,_compute(c))
  confirmed=s.get(ForecastRun,c.run_id);confirmed_token=confirmed.run_token
 # 恢复后新进程接单
 with c.Session() as s:
  stale=s.get(ForecastRun,c.run_id)
  # 旧进程拿着旧 token 试图再次写回
  out=finalize_run(s,stale,token,{'snapshot':c.snap,'checksum':'tampered'})
  assert out is False
  r=s.get(ForecastRun,c.run_id)
  assert r.run_token==confirmed_token and r.result['checksum']!='tampered'
  assert r.verification=='verified'

# ---------- 6. 逐日核对：文件缺失/损坏/被篡改均判 corrupt ----------
def test_verification_detects_missing_corrupt_tampered(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
  r=s.get(ForecastRun,c.run_id)
  p=artifact_path(c.run_id)
  # 缺失
  p.rename(p.with_suffix('.bak'))
  ok,d=verify_artifact(r);assert ok is False and '缺失' in d['reason']
  # 损坏（半写 JSON）
  p.write_text('{broken',encoding='utf-8')
  ok,d=verify_artifact(r);assert ok is False and '损坏' in d['reason']
  # 篡改两种模型之一的逐日预测值
  good=json.loads(p.with_suffix('.bak').read_text(encoding='utf-8'))
  tampered=json.loads(json.dumps(good));tampered['future']['holt'][0]['prediction']+=999
  p.write_text(json.dumps(tampered,ensure_ascii=False),encoding='utf-8')
  ok,d=verify_artifact(r)
  assert ok is False and d['differences'] and any('future.holt[0]' in x for x in d['differences'])
  # 篡改 checksum 同样被抓
  tampered2=json.loads(json.dumps(good));tampered2['checksum']='deadbeef'
  p.write_text(json.dumps(tampered2,ensure_ascii=False),encoding='utf-8')
  ok,d=verify_artifact(r);assert ok is False and any('checksum' in x for x in d['differences'])
  p.with_suffix('.bak').rename(p)

# ---------- 7. 门控：未 verified 暂停导出/排班；verified 放行 ----------
def test_gates_block_until_verified(setup):
 c=setup
 with c.Session() as s:
  row=ForecastRun(name='q',branch='川北',backtest_days=14,alpha=.6,beta=.2,
                  state='done',result=_compute(c),verification='corrupt',
                  verify_detail={'reason':'结果文件缺失'})
  s.add(row);s.commit();rid=row.id
  from fastapi import HTTPException
  with pytest.raises(HTTPException) as ei:ensure_verified(s.get(ForecastRun,rid))
  assert ei.value.status_code==409
  data=StaffingInput(run_id=rid,capacity=12,model='holt',note='轮休约束说明足够长',request_key=uuid.uuid4())
  with pytest.raises(HTTPException) as e2:draft(s,s.get(ForecastRun,rid),data)
  assert e2.value.status_code==409

# ---------- 8. 修复：文件损坏后用库内冻结值重写，原 run 恢复 verified，不新建 ----------
def test_repair_done_run_rewrites_artifact(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
  rid=c.run_id
 # 外部损坏文件
 p=artifact_path(rid);p.write_text('{half-written',encoding='utf-8')
 with c.Session() as s:
  r=s.get(ForecastRun,rid);ok,_=verify_artifact(r);assert ok is False
  out=repair_run(s,rid,c.q)
  assert out['action']=='rewritten' and out['verification']=='verified'
  assert s.query(ForecastRun).count()==1
  good=json.loads(p.read_text(encoding='utf-8'));assert good['checksum']==_compute(c)['checksum']

# ---------- 9. 修复 queued/running/failed：重新入队同 run 重算 ----------
def test_repair_unfinished_requeues(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id)  # running
 with c.Session() as s:
  res=repair_run(s,c.run_id,c.q)
  assert res['action']=='requeued' and res['state']=='queued'
  r=s.get(ForecastRun,c.run_id);assert r.run_token==''
 assert c.q.lrange('demand:runs',0,-1)==[str(c.run_id).encode()]
 with c.Session() as s:assert s.query(ForecastRun).count()==1

# ---------- 10. 排班重试幂等：同一 request_key 重试不重复创建 ----------
def test_staffing_idempotent_retry(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
 key=uuid.uuid4()
 data=StaffingInput(run_id=c.run_id,capacity=12,model='holt',note='周末轮休与技能约束说明',request_key=key)
 r1,created1=draft(s,s.get(ForecastRun,c.run_id),data)
 assert created1 is True
 r2,created2=draft(s,s.get(ForecastRun,c.run_id),data.model_copy())
 assert created2 is False and r2.id==r1.id
 assert s.query(Staffing).count()==1
 # 人数与依据固化
 assert all('staff' in d for d in r1.schedule) and r1.schedule[0]['staff']>=1

# ---------- 11. 同号异内容拒绝 ----------
def test_staffing_same_key_different_content_rejected(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
  key=uuid.uuid4()
  d1=StaffingInput(run_id=c.run_id,capacity=12,model='holt',note='原始排班约束说明内容',request_key=key)
  draft(s,s.get(ForecastRun,c.run_id),d1)
  from fastapi import HTTPException
  for d2 in [
   StaffingInput(run_id=c.run_id,capacity=8,model='holt',note='原始排班约束说明内容',request_key=key),  # 容量变
   StaffingInput(run_id=c.run_id,capacity=12,model='seasonal_naive',note='原始排班约束说明内容',request_key=key),  # 模型变
   StaffingInput(run_id=c.run_id,capacity=12,model='holt',note='完全不同的排班约束说明内容X',request_key=key)]:  # 说明变
   with pytest.raises(HTTPException) as ei:draft(s,s.get(ForecastRun,c.run_id),d2)
   assert ei.value.status_code==409
  assert s.query(Staffing).count()==1  # 拒绝后无新增

# ---------- 12. 预测修复后已有排班保持原人数与依据 ----------
def test_existing_staffing_unchanged_after_repair(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
  key=uuid.uuid4()
  data=StaffingInput(run_id=c.run_id,capacity=12,model='holt',note='保持原人数与依据的排班说明',request_key=key)
  st,_=draft(s,s.get(ForecastRun,c.run_id),data)
  frozen=json.loads(json.dumps(st.schedule));frozen_note=st.note;frozen_cap=st.capacity
 # 损坏并修复原预测
 artifact_path(c.run_id).write_text('{broken',encoding='utf-8')
 with c.Session() as s:
  repair_run(s,c.run_id,c.q)
  st2=s.get(Staffing,st.id)
  assert st2.schedule==frozen and st2.note==frozen_note and st2.capacity==frozen_cap

# ---------- 13. 失败围栏：旧 token 不能把已完成预测改 failed ----------
def test_fail_run_respects_fencing(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
  ok=fail_run(s,row,'old-token','迟到的失败')  # 旧进程
  assert ok is False
  r=s.get(ForecastRun,c.run_id);assert r.state=='done'

# ---------- 14. 重算确定性：同一冻结快照多次计算 checksum 完全一致 ----------
def test_deterministic_recompute(setup):
 c=setup
 a=forecast(c.snap,14,.6,.2);b=forecast(c.snap,14,.6,.2)
 assert a['checksum']==b['checksum'] and a['future']==b['future'] and a['backtest']==b['backtest']

# ---------- 15. 计算失败后修复：同一 run 重新入队并成功 ----------
def test_failed_then_repair_requeues_and_succeeds(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id)
  assert fail_run(s,row,token,'模拟计算异常') is True
  r=s.get(ForecastRun,c.run_id);assert r.state=='failed'
 with c.Session() as s:
  out=repair_run(s,c.run_id,c.q);assert out['action']=='requeued'
 from src import worker as worker_mod
 worker_mod.r=c.q;worker_mod.handle(int(c.q.blpop('demand:runs')[1]))
 with c.Session() as s:
  r=s.get(ForecastRun,c.run_id)
  assert r.state=='done' and r.verification=='verified' and r.attempts==2
  assert s.query(ForecastRun).count()==1

# ---------- 16. 文件多出未知字段/少一天未来预测均判损坏 ----------
def test_verification_detects_extra_and_missing_day(setup):
 c=setup
 import json as _json
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
  r=s.get(ForecastRun,c.run_id);p=artifact_path(c.run_id)
  good=_json.loads(p.read_text(encoding='utf-8'))
  extra=_json.loads(_json.dumps(good));extra['unexpected']={'x':1}
  p.write_text(_json.dumps(extra,ensure_ascii=False),encoding='utf-8')
  ok,d=verify_artifact(r);assert ok is False and any('unexpected' in x for x in d['differences'])
  missing=_json.loads(_json.dumps(good));missing['future']['holt'].pop()
  p.write_text(_json.dumps(missing,ensure_ascii=False),encoding='utf-8')
  ok,d=verify_artifact(r);assert ok is False and any('future.holt' in x and '天数不符' in x for x in d['differences'])

# ---------- 17. 已完成且文件正常时修复为 noop，不重写不新建 ----------
def test_repair_healthy_is_noop(setup):
 c=setup
 with c.Session() as s:
  row,token=claim_run(s,c.run_id);finalize_run(s,row,token,_compute(c))
  before=artifact_path(c.run_id).read_text(encoding='utf-8')
  out=repair_run(s,c.run_id,c.q)
  assert out['action']=='noop' and out['verification']=='verified'
  assert artifact_path(c.run_id).read_text(encoding='utf-8')==before
  assert s.query(ForecastRun).count()==1
