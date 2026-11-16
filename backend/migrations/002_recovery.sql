-- 中断恢复 / 并发围栏 / 文件逐日校验 / 排班幂等
-- 在线轻量迁移由 src/main.py lifespan 执行；本文件保留权威 DDL 供建库参考。

-- forecast_runs 新增围栏与校验列
ALTER TABLE public.forecast_runs ADD COLUMN run_token character varying(40) DEFAULT '';
ALTER TABLE public.forecast_runs ADD COLUMN verification character varying(20) DEFAULT 'none';
ALTER TABLE public.forecast_runs ADD COLUMN verify_detail json DEFAULT '{}';
ALTER TABLE public.forecast_runs ADD COLUMN attempts integer DEFAULT 0;

-- staffings 新增幂等键：同号同内容重试不重复创建，同号异内容由应用层拒绝
ALTER TABLE public.staffings ADD COLUMN request_key character varying(64);
UPDATE public.staffings SET request_key='legacy-'||id WHERE request_key IS NULL;
ALTER TABLE public.staffings ALTER COLUMN request_key SET NOT NULL;
ALTER TABLE public.staffings ADD CONSTRAINT uq_staffing_request_key UNIQUE (request_key);
