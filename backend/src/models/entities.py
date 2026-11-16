from sqlalchemy import Column,Integer,String,Float,JSON,Text,ForeignKey,UniqueConstraint
from src.config.database import Base
class User(Base):
 __tablename__='users'
 id=Column(Integer,primary_key=True);email=Column(String(100),unique=True);password=Column(String(200));role=Column(String(20))
class DailyVolume(Base):
 __tablename__='daily_volumes';__table_args__=(UniqueConstraint('branch','day',name='uq_branch_day'),)
 id=Column(Integer,primary_key=True);branch=Column(String(30),nullable=False);day=Column(String(10),nullable=False);volume=Column(Integer,nullable=False);note=Column(Text,default='')
class ForecastRun(Base):
 __tablename__='forecast_runs'
 id=Column(Integer,primary_key=True);name=Column(String(100));branch=Column(String(30));backtest_days=Column(Integer);alpha=Column(Float);beta=Column(Float)
 # state=计算生命周期；verification=结果文件与库内保存值的逐日核对状态；run_token=接单围栏，迟到旧进程写回时不匹配即拒绝
 state=Column(String(20),default='queued');result=Column(JSON);error=Column(Text,default='')
 run_token=Column(String(40),default='');verification=Column(String(20),default='none');verify_detail=Column(JSON,default=dict);attempts=Column(Integer,default=0)
class Staffing(Base):
 __tablename__='staffings';__table_args__=(UniqueConstraint('request_key',name='uq_staffing_request_key'),)
 id=Column(Integer,primary_key=True);run_id=Column(Integer,ForeignKey('forecast_runs.id'));capacity=Column(Integer);model=Column(String(30));schedule=Column(JSON);note=Column(Text);status=Column(String(20),default='draft')
 # request_key 由调用方生成并随重试原样重发：同号同内容幂等返回原排班，同号异内容拒绝
 request_key=Column(String(64),nullable=False)
class Audit(Base):
 __tablename__='audits'
 id=Column(Integer,primary_key=True);actor=Column(String(100));action=Column(String(200));created=Column(String(40))
