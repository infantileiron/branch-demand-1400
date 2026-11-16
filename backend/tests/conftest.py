import os
# 必须在导入应用代码前设置：security 读取 JWT_SECRET，worker 读取 REDIS_URL
os.environ.setdefault('JWT_SECRET','test-secret-3109')
os.environ.setdefault('REDIS_URL','redis://localhost:6379/0')
os.environ.setdefault('DATABASE_URL','sqlite:///./test.db')
os.environ.setdefault('ARTIFACT_DIR','/tmp/test-artifacts')
