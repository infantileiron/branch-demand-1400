export const BRANCHES=['川北','川南','东河','西浦','新城','临港','山前','湖滨'];
export const MODELS={seasonal_naive:'周季节朴素',holt:'Holt线性趋势'};
export const STATES=['queued','running','done','failed'];
export const ROLES=['admin','analyst','viewer'];
// 文件与库内保存值逐日核对状态
export const VERIFICATIONS={
 none:'未核对',
 pending:'待核对',
 verified:'逐日校验相符',
 corrupt:'文件缺失或损坏·入口暂停'
};
export const STATE_LABELS={queued:'排队中',running:'计算中',done:'已完成',failed:'计算失败'};
