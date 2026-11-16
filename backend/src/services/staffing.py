import math
from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError
from src.models.entities import Staffing
from src.services.recovery import ensure_verified

def build_schedule(run,data):
 # 人数与依据在创建时固化进 schedule，预测后续修复不改变已生成排班
 return [{**p,'staff':math.ceil(p['prediction']/data.capacity)} for p in run.result['future'][data.model]]

def draft(s,run,data):
 ensure_verified(run)  # 损坏/未核对预测暂停排班入口
 request_key=str(data.request_key)
 existing=s.query(Staffing).filter_by(request_key=request_key).one_or_none()
 if existing:
  # 重试同一排班请求：同号同内容幂等返回原排班，不重复创建；同号异内容拒绝
  same=(existing.run_id==run.id and existing.capacity==data.capacity and existing.model==data.model
        and existing.note==data.note and existing.schedule==build_schedule(run,data))
  if not same:raise HTTPException(409,f'请求编号 {request_key} 已用于不同排班内容，拒绝同号异内容（已存在 run#{existing.run_id}/容量{existing.capacity}/{existing.model}）')
  return existing,False
 row=Staffing(run_id=run.id,capacity=data.capacity,model=data.model,note=data.note,
              schedule=build_schedule(run,data),request_key=request_key)
 s.add(row)
 try:s.commit()
 except IntegrityError:
  s.rollback()
  raced=s.query(Staffing).filter_by(request_key=request_key).one()
  if (raced.run_id==run.id and raced.capacity==data.capacity and raced.model==data.model and raced.note==data.note):
   return raced,False
  raise HTTPException(409,'请求编号已用于不同排班内容，拒绝同号异内容')
 return row,True
