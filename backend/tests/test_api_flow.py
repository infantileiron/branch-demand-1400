import json,uuid,threading
from datetime import date,timedelta
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from fastapi.testclient import TestClient
import fakeredis
from src.config import database as dbmod
from src.models.entities import Base,User,DailyVolume,ForecastRun
from src.main import app
from src.utils.security import hash_password
from src import worker as worker_mod
from src.routes import api as routes

@pytest.fixture()
def client(tmp_path,monkeypatch):
 monkeypatch.setenv('ARTIFACT_DIR',str(tmp_path/'artifacts'))
 engine=create_engine('sqlite:///'+str(tmp_path/'api.db'))
 Base.metadata.create_all(engine)
 Session=sessionmaker(bind=engine,expire_on_commit=False)
 dbmod.Session=Session;dbmod.engine=engine
 fake=fakeredis.FakeStrictRedis()
 monkeypatch.setattr(routes,'queue',lambda:fake)
 monkeypatch.setattr(worker_mod,'r',fake)
 with Session() as s:
  for role in ('admin','analyst','viewer'):
   s.add(User(email=role+'@demo.local',role=role,password=hash_password('Demo123!')))
  base=date(2026,9,6)
  for i in range(49):
   d=base+timedelta(days=i);wd=d.weekday()
   s.add(DailyVolume(branch='川北',day=d.isoformat(),
                     volume=max(0,round(25+i*.1+(10 if wd<5 else -6)+((i*7)%5)-2)),note='日结'))
  s.commit()
 with TestClient(app) as c:
  def login(role):
   r=c.post('/auth/login',json={'email':role+'@demo.local','password':'Demo123!'})
   return {'Authorization':'Bearer '+r.json()['token']}
  c.h={r_:login(r_) for r_ in ('admin','analyst','viewer')}
  c.Session=Session;c.fake=fake
  yield c

def _submit(c):
 r=c.post('/runs',headers=c.h['analyst'],
          json={'name':'接口端到端预测','branch':'川北','backtest_days':14,'alpha':0.6,'beta':0.2})
 assert r.status_code==200;return r.json()['id']

def test_full_flow_gates_export_and_staffing(client):
 c=client;rid=_submit(c)
 # 完成前导出与排班都被门控暂停
 assert c.get(f'/runs/{rid}/export',headers=c.h['viewer']).status_code==409
 # worker 接单处理（同一进程内调用 handle 模拟消费）
 worker_mod.handle(c.fake.blpop('demand:runs')[1].decode())
 detail=c.get(f'/runs/{rid}',headers=c.h['viewer']).json()
 assert detail['state']=='done' and detail['verification']=='verified'
 # 核对接口
 v=c.post(f'/runs/{rid}/verify',headers=c.h['viewer']).json()
 assert v['verification']=='verified' and v['detail']['future_days']==7
 # 导出开放
 exp=c.get(f'/runs/{rid}/export',headers=c.h['viewer'])
 assert exp.status_code==200 and exp.headers['content-disposition'].startswith('attachment')
 body=exp.json();assert body['id']==rid and body['verification']=='verified' and len(body['result']['future']['holt'])==7
 # 排班：带幂等键
 key=str(uuid.uuid4())
 payload={'run_id':rid,'capacity':12,'model':'holt','note':'接口测试的轮休与技能约束','request_key':key}
 r1=c.post('/staffings',headers=c.h['analyst'],json=payload)
 r2=c.post('/staffings',headers=c.h['analyst'],json=payload)  # 中断后重试同一请求
 assert r1.status_code==200 and r2.status_code==200 and r1.json()['id']==r2.json()['id']
 assert len(c.get('/staffings',headers=c.h['viewer']).json())==1
 # 同号异内容拒绝
 bad=dict(payload,capacity=8)
 assert c.post('/staffings',headers=c.h['analyst'],json=bad).status_code==409
 # viewer 不能起草
 assert c.post('/staffings',headers=c.h['viewer'],json=payload).status_code==403
 # 签核一次后不可重复
 sid=r1.json()['id']
 assert c.post(f'/staffings/{sid}/approve',headers=c.h['admin']).status_code==200
 assert c.post(f'/staffings/{sid}/approve',headers=c.h['admin']).status_code==409

def test_corrupt_artifact_blocks_then_repair_restores(client):
 c=client;rid=_submit(c);worker_mod.handle(c.fake.blpop('demand:runs')[1].decode())
 from src.services.recovery import artifact_path
 p=artifact_path(rid)
 # 模拟文件被截断（写完后损坏）
 p.write_text('{half',encoding='utf-8')
 detail=c.get(f'/runs/{rid}',headers=c.h['analyst']).json()
 assert detail['verification']=='corrupt'
 assert c.get(f'/runs/{rid}/export',headers=c.h['viewer']).status_code==409
 key=str(uuid.uuid4())
 payload={'run_id':rid,'capacity':12,'model':'holt','note':'损坏期间排班入口应被暂停的说明','request_key':key}
 assert c.post('/staffings',headers=c.h['analyst'],json=payload).status_code==409
 # 修复原预测（不新建）
 out=c.post(f'/runs/{rid}/repair',headers=c.h['analyst'])
 assert out.status_code==200 and out.json()['action']=='rewritten'
 runs=c.get('/runs',headers=c.h['viewer']).json();assert len(runs)==1
 assert c.get(f'/runs/{rid}',headers=c.h['viewer']).json()['verification']=='verified'
 assert c.get(f'/runs/{rid}/export',headers=c.h['viewer']).status_code==200
 # viewer 无修复权限
 p.write_text('{half',encoding='utf-8')
 assert c.post(f'/runs/{rid}/repair',headers=c.h['viewer']).status_code==403

def test_two_workers_race_only_one_processes(client):
 c=client;rid=_submit(c)
 # 队列里因重复投递存在两条同 id 消息，两个“worker”并发消费
 c.fake.rpush('demand:runs',rid)  # 制造重复投递
 results=[]
 def consume():
  item=c.fake.blpop('demand:runs',timeout=1)
  if item:worker_mod.handle(int(item[1]));results.append(int(item[1]))
 t1=threading.Thread(target=consume);t2=threading.Thread(target=consume)
 t1.start();t2.start();t1.join();t2.join()
 assert sorted(results)==[rid,rid]  # 两条消息都被取走
 with c.Session() as s:
  r=s.get(ForecastRun,rid)
  assert r.state=='done' and r.attempts==1  # 只真正处理一次
  assert s.query(ForecastRun).count()==1

def test_interrupt_running_then_worker_restart_recovers_same_run(client):
 c=client;rid=_submit(c)
 # 消费一条并在接单后模拟崩溃（停在 running）
 item=c.fake.blpop('demand:runs')[1].decode()
 with c.Session() as s:
  from src.services.recovery import claim_run
  row,token=claim_run(s,int(item));assert row is not None
 # 重启 worker：恢复 queued/running 并重新入队同一 id
 worker_mod.recover()
 worker_mod.handle(c.fake.blpop('demand:runs')[1].decode())
 detail=c.get(f'/runs/{rid}',headers=c.h['viewer']).json()
 assert detail['state']=='done' and detail['verification']=='verified'
 assert len(c.get('/runs',headers=c.h['viewer']).json())==1
