import os,json
import redis
from fastapi import APIRouter,Depends,HTTPException,Response
from src.config.database import db
from src.models.entities import User,DailyVolume,ForecastRun,Staffing,Audit
from src.schemas.business import Login,DailyInput,ForecastInput,StaffingInput
from src.middlewares.auth import current,editor,admin
from src.utils.security import verify,token
from src.services.business import audit,dump
from src.services.volumes import save_volume
from src.services.staffing import draft
from src.services.recovery import verify_artifact,ensure_verified,repair_run,artifact_path
from src.controllers.business import ensure_run
from src.constants.enums import QUEUE_KEY
router=APIRouter()
def queue():return redis.Redis.from_url(os.environ['REDIS_URL'])
@router.post('/auth/login')
def login(data:Login,s=Depends(db)):
 u=s.query(User).filter_by(email=data.email).first()
 if not u or not verify(data.password,u.password):raise HTTPException(401,'账号或密码错误')
 return {'token':token(u),'user':dump(u)}
@router.get('/me')
def me(u=Depends(current)):return dump(u)
@router.get('/volumes')
def volumes(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(DailyVolume).order_by(DailyVolume.day,DailyVolume.branch).all()]
@router.post('/volumes')
def create(data:DailyInput,u=Depends(editor),s=Depends(db)):
 r=save_volume(s,data.model_dump(mode='json'));audit(s,u,'录入日量 '+str(r.id));return dump(r)
@router.put('/volumes/{i}')
def edit(i:int,data:DailyInput,u=Depends(editor),s=Depends(db)):
 r=save_volume(s,data.model_dump(mode='json'),i);audit(s,u,'修订日量 '+str(r.id));return dump(r)
@router.get('/runs')
def runs(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(ForecastRun).order_by(ForecastRun.id.desc()).all()]
@router.post('/runs')
def run(data:ForecastInput,u=Depends(editor),s=Depends(db)):
 # 仅用户显式提交时创建预测；中断恢复与修复都复用同一 run id，不另建预测
 rows=[dump(r) for r in s.query(DailyVolume).filter_by(branch=data.branch).order_by(DailyVolume.day).all()]
 r=ForecastRun(**data.model_dump(),result={'snapshot':rows});s.add(r);s.commit()
 try:queue().rpush(QUEUE_KEY,r.id)
 except redis.RedisError:r.state='failed';r.error='队列暂不可用，请恢复后重试';s.commit();raise HTTPException(503,r.error)
 audit(s,u,'提交预测 '+str(r.id));return dump(r)
@router.get('/runs/{i}')
def detail(i:int,u=Depends(current),s=Depends(db)):
 r=ensure_run(s,i)
 # 查看时顺带核对文件：页面因此能看到最新恢复/校验状态，且不改动已确认保存值
 if r.state=='done':
  ok,d=verify_artifact(r)
  new_v='verified' if ok else 'corrupt'
  if r.verification!=new_v or r.verify_detail!=d:r.verification=new_v;r.verify_detail=d;s.commit()
 return dump(r)
@router.post('/runs/{i}/verify')
def reverify(i:int,u=Depends(current),s=Depends(db)):
 r=ensure_run(s,i)
 if r.state!='done':raise HTTPException(409,'仅完成的预测可核对文件')
 ok,d=verify_artifact(r);r.verification='verified' if ok else 'corrupt';r.verify_detail=d;s.commit()
 return {'run_id':i,'verification':r.verification,'detail':d}
@router.post('/runs/{i}/repair')
def repair(i:int,u=Depends(editor),s=Depends(db)):
 try:out=repair_run(s,i,queue())
 except redis.RedisError:raise HTTPException(503,'队列暂不可用，请恢复后重试修复')
 audit(s,u,'修复预测 '+str(i)+' '+out['action']);return out
@router.get('/runs/{i}/export')
def export(i:int,u=Depends(current),s=Depends(db)):
 r=ensure_verified(ensure_run(s,i))  # 损坏/未核对预测暂停导出
 path=artifact_path(i)
 if not path.exists():
  r.verification='corrupt';r.verify_detail={'reason':'结果文件缺失'};s.commit();raise HTTPException(409,'结果文件缺失，入口已暂停，请修复原预测')
 return Response(json.dumps(dump(r),ensure_ascii=False,indent=2),media_type='application/json',headers={'Content-Disposition':f'attachment; filename="branch-demand-{i}.json"'})
@router.get('/staffings')
def staffings(u=Depends(current),s=Depends(db)):return [dump(r) for r in s.query(Staffing).order_by(Staffing.id.desc()).all()]
@router.post('/staffings')
def staffing(data:StaffingInput,u=Depends(editor),s=Depends(db)):
 r,created=draft(s,ensure_run(s,data.run_id),data)
 if created:audit(s,u,'起草排班 '+str(r.id))
 return dump(r)
@router.post('/staffings/{i}/approve')
def approve(i:int,u=Depends(admin),s=Depends(db)):
 r=s.get(Staffing,i)
 if not r:raise HTTPException(404,'排班建议不存在')
 if r.status!='draft':raise HTTPException(409,'不可重复签核')
 r.status='approved';s.commit();audit(s,u,'签核排班 '+str(i));return dump(r)
@router.get('/audits')
def audits(u=Depends(admin),s=Depends(db)):return [dump(r) for r in s.query(Audit).order_by(Audit.id.desc()).limit(100)]
