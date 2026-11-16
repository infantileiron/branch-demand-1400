ROLES=('admin','analyst','viewer')
STATES=('queued','running','done','failed')
# verification 是文件与库内保存值逐日核对的结果，独立于计算状态 state：
# none=尚未写入文件/未校验；pending=文件已写待核对；verified=逐日相符；corrupt=缺失或损坏，入口暂停
VERIFICATIONS=('none','pending','verified','corrupt')
BRANCHES=('川北','川南','东河','西浦','新城','临港','山前','湖滨')
MODELS=('seasonal_naive','holt')
QUEUE_KEY='demand:runs'
ARTIFACT_DIR='/artifacts'
