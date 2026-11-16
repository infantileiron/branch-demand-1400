import {esc} from '../../utils/html.js';import {VERIFICATIONS,STATE_LABELS} from '../../constants/enums.js';
export const badge=s=>`<span class="tag tag-state-${esc(s)}">${esc(STATE_LABELS[s]||s)}</span>`;
// 校验徽标：独立于计算状态，展示文件与保存值的逐日核对结果
export const verifyBadge=v=>`<span class="tag tag-v-${esc(v||'none')}">${esc(VERIFICATIONS[v||'none']||v||'未核对')}</span>`;
