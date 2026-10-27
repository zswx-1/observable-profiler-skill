# observable-profiler-analysis

用于读取和分析 Minecraft [Observable](https://observable.tas.sh/) 性能报告的只读工具。它把报告解释为**对象级包络计时 + 异步采样 trace**，提取时间窗口、tick 数、有效 TPS、block-side/entity-side 对象开销、维度与类型聚合、调用路径和数据质量信息，并可与 Spark 做时间对齐。

Observable 的数字不能直接当作全局 MSPT，也不能与 Spark 的 owner、self、inclusive、family 或 trace count 相加。这个工具的作用是把持续出现的对象和调用路径定位出来，再为后续的单变量 A/B 验证提供可复核证据。

## 能做什么

- 从 Observable viewer URL、`/v1/get/<id>` endpoint 或裸报告 ID 获取原始 JSON。
- 保留来源 URL、实际 endpoint、抓取时间、字节数和 SHA-256。
- 检查 `data`、`diagnostics`、`blocks`、`entities`、`traces` 的基本结构。
- 提取报告起始时间、持续时间、tick 数、有效 TPS、平均 tick 周期和 Observable/Minecraft/loader 版本。
- 按活跃 ticks 加权计算 block-side 和 entity-side 的 `ms/tick`，避免把短暂对象的裸 `rate` 直接相加。
- 按 group、type 和 `type` 的 namespace 前缀聚合对象开销，并列出 top entries、位置、活跃 ticks 和单对象最大值。
- 审计未知字段、字段类型错误、非法位置、异常 trace 节点和受限遍历情况。
- 区分 global trace 与 per-entry trace，输出方法、完整路径、采样覆盖率和截断原因。
- 比较两个 Observable 窗口的 gap、overlap、环境指纹、对象变化、实体 ID churn 和聚合差异。
- 将 Spark probe/analysis JSON 与 Observable 窗口对齐，输出 overlap、gap、containment 和证据等级提示。

## 运行要求

- Node.js 18 或更高版本。
- 只使用 Node.js 标准库，不需要 `npm install`。
- 在线执行 `fetch` 需要能够访问 `observable.tas.sh`；本地 JSON 的 `probe`、`summarize`、`compare` 和 `align/joint` 不需要网络。

## 快速开始

在本目录执行：

```bash
# 1. 从 viewer URL 抓取 /v1/get JSON，并保存来源元数据
node scripts/observable.mjs fetch https://observable.tas.sh/p/<id> --out work/observable-<id>.json

# 2. 查看文件、哈希、窗口、对象聚合、trace 和 schema 摘要
node scripts/observable.mjs probe work/observable-<id>.json

# 3. 只输出分析摘要，不重复打印文件信息
node scripts/observable.mjs summarize work/observable-<id>.json
```

已有本地 `/v1/get` JSON 时，直接运行：

```bash
node scripts/observable.mjs probe path/to/observable.json
node scripts/observable.mjs summarize path/to/observable.json
```

比较两个窗口：

```bash
node scripts/observable.mjs compare \
  work/observable-a.json \
  work/observable-b.json
```

与 Spark 对齐：

```bash
node scripts/observable.mjs align \
  work/spark-probe.json \
  work/observable-a.json
```

`joint` 是 `align` 的同义命令：

```bash
node scripts/observable.mjs joint work/spark-analysis.json work/observable-a.json
```

## 命令

### `fetch`

```bash
node scripts/observable.mjs fetch <viewer|endpoint|id> --out <file.json>
```

输入可以是：

- `https://observable.tas.sh/p/<id>` viewer URL；
- `https://observable.tas.sh/v1/get/<id>` endpoint；
- 裸报告 ID。

viewer 页面通常只是前端 shell。脚本会把 viewer URL 或裸 ID 转成：

```text
https://observable.tas.sh/v1/get/<id>
```

成功后写出两个文件：

- `<file.json>`：接口返回的原始 JSON 字节，不改写 payload；
- `<file.json>.source.json`：输入、实际 endpoint、抓取时间、字节数和 SHA-256。

脚本会检查 HTTP 状态、JSON content type、JSON 解析结果，以及 `data`、`diagnostics`、`data.entities`、`data.blocks`、`data.traces` 是否存在。HTML、空响应、非 JSON、截断 JSON 或缺少必要字段时会失败，不继续做热点归因。

`fetch` 面向在线 endpoint；本地文件不经过 `fetch` 复制，直接交给 `probe`、`summarize` 或其他分析命令。

### `probe`

```bash
node scripts/observable.mjs probe <file.json>
```

先输出文件绝对路径、字节数、原始 SHA-256 和 `diagnostics.user` 是否存在，再输出完整摘要。摘要包括：

- `diagnostics.start`、`diagnostics.duration`；
- `data.ticks` 及其来源：`observed` 或缺失时的 `inferred`；
- 有效 TPS 和平均 tick 周期；
- 数据质量计数器与 schema 审计；
- blocks/entities 的加权 `ms/tick`；
- group、namespace candidate、type 聚合；
- top entries；
- global/per-entry trace 统计；
- overrange 条目审计；
- Observable、Minecraft 和 loader 版本；
- Mods、System Report、environment 和 schema 指纹。

### `summarize`

```bash
node scripts/observable.mjs summarize <file.json>
```

输出与 `probe` 相同的分析摘要，但不额外打印文件路径、文件字节数、原始 SHA-256 和 user presence。适合直接把分析结果重定向到文本文件：

```bash
node scripts/observable.mjs summarize work/observable-a.json > work/observable-a.summary.txt
```

### `compare`

```bash
node scripts/observable.mjs compare <file-a.json> <file-b.json>
```

比较前后两个窗口，并输出：

- A/B 的半开时间区间、ticks 来源、gap、overlap 和 union；
- user、Minecraft/loader、Observable 版本是否一致；
- Mods、完整 System Report、稳定环境字段和 schema 指纹；
- A/B 数据质量与 schema 审计；
- overrange 审计；
- block/entity group 的 `ms/tick` 差异；
- 对象 key 的 added/removed 数量；
- namespace candidate 的差异；
- entity exact ID 的 stable/added/removed 和 stable ratio；
- entity `(group,type)` 数量变化；
- A/B 的 per-entry trace 覆盖率和路径统计。

实体 exact ID 的变化不能直接解释为生成或销毁。若 stable ratio 较低，应优先使用 `(group,type)` 聚合和加权时间判断持续性。

### `align` / `joint`

```bash
node scripts/observable.mjs align <spark-json> <observable.json>
node scripts/observable.mjs joint <spark-json> <observable.json>
```

第一个输入可以是 Spark `probe --json` 输出或 analysis JSON。脚本会尝试读取 Spark 的 `start_time`、`end_time`、`sampling.seconds`、`perWindow`、聚合器和线程转储器等字段，再与 Observable 的：

```text
[diagnostics.start, diagnostics.start + diagnostics.duration)
```

进行半开区间对齐，输出：

- Spark 和 Observable 的起止时间；
- `gap`、`overlap`、`containment` 和 `union`；
- Spark aggregator、thread dumper 和 Observable tick source；
- Spark 与 Observable 分母保持独立的提示；
- 有重叠时的 L3 same-window cross-check 候选，否则的 L1/L2 证据提示。

这个命令只做时间和分母对照，不会把两类报告的耗时合并成一个 MSPT。

## 计算口径

### tick 数、有效 TPS 和 tick 周期

脚本优先使用 `data.ticks`：

- 数值型或可解析的正值：标记为 `data.ticks (observed)`；
- 缺失或无效时：使用所有合法 entry 中最大的 `entry.ticks`，标记为 `max(entry.ticks) (inferred)`；
- 两者都不可用：ticks、TPS 和 tick 周期显示为 `N/A`。

只有 ticks 和 duration 可用时，才计算：

```text
effective TPS = total_window_ticks / (duration_ms / 1000)
tick period    = duration_ms / total_window_ticks
```

这两个值只描述 Observable 窗口，不替代 Spark 的 1m、5m 或 15m MSPT。`data.ticks` 为推断值时，报告必须保留 `inferred` 标记。

### 对象加权

entry 中的 `rate` 是该对象已记录的一次 tick 的平均纳秒开销，通常来自：

```text
rate = data.time / data.ticks
```

对象对整个窗口的加权贡献为：

```text
entry weighted ns/tick = rate × entry.ticks / total_window_ticks
entry weighted ms/tick = entry weighted ns/tick / 1,000,000
```

同一 group 或 type 的聚合先完成上述加权，再累加对象贡献。输出还会给出对象数、active ticks、总 active ns 和单对象最大值。

不能直接把所有 entry 的裸 `rate` 相加。短暂出现的 item、player 或 entity 可能只有少量 active ticks，直接相加会夸大窗口级负载。

### block-side 与 entity-side

`data.blocks` 统一称为 `block-side entries`，因为它可能混合：

- block entity tick；
- random block tick；
- fluid tick。

`data.entities` 与 `data.blocks` 是报告返回的对象明细，不是服务器全量 census。`group` 可能是普通维度，也可能是 Compact Machines、Holo Hive 等虚拟世界键，报告时应保留原值。

实体优先按 `(group, entityId)` 匹配，位置只作为描述；方块按 `(group, position, type)` 检查重复 key。重复 key 不会被静默去重。

### namespace candidate

脚本把 `type` 第一个 `:` 之前的部分作为 namespace candidate，例如：

```text
create:mechanical_bearing -> create
```

它只是对象 ID 的前缀候选，不是 CPU owner，也不是 Mod self time。Mod 归因需要结合 trace、Spark 调用树和时间窗口交叉验证。

## Trace 的解释

Observable 有两类 trace，必须分开阅读：

- `data.traces`：global 异步 stack sample 调用树；
- entry 内的 `traces`：某个对象条目的 per-entry trace。

`count` 是采样出现次数，不是方法耗时、CPU 时间、调用次数或对象 `rate`。父子节点存在嵌套关系，不能相加；同一路径出现在多个上下文中，也不能把 count 当作总调用数。

脚本会输出：

- 方法级最大节点 count；
- global trace 的完整路径最大 count；
- per-entry trace 的路径最大节点 count 及覆盖条目数；
- trace 对象覆盖率、非零 count、零 count、childless 和 invalid 数量；
- global trace 的深度、节点数、malformed 节点、malformed children 和截断原因。

trace 遍历有深度、节点和 per-entry 路径预算。出现 `pathLimited`、`depthLimited` 或 `nodeLimited` 时，只能把结果当作受限摘要。没有 trace 只能说明该条目没有可用的 trace 数据，不能解释成“没有执行路径”。

异步根节点可能是 `java.lang.Thread.hash`、`tickBlockEntities` 或其他采样时线程上下文。相邻窗口的根节点不同，不等于业务热点新增或消失；比较时应从共同的业务子树开始。

## 数据质量与 schema 审计

工具保留异常值并报告问题，不用 `Number(...) || 0` 把缺失数据静默变成零。常见计数包括：

- invalid group、row、rate、ticks；
- 缺失 rate 或 ticks；
- numeric string rate/ticks；
- fractional ticks；
- 重复对象 key；
- ticks 超过窗口 total ticks 的 overrange 条目。

schema audit 会记录：

- unknown top-level/data/diagnostics/additionalDiagnostics 字段；
- unknown row、position 和 trace 字段；
- 字段类型错误；
- 非法 position；
- 非法 trace node 或 children；
- trace inspection 是否达到审计上限。

未知字段只产生告警，不会被改写成已知语义。

### overrange

当 `data.ticks` 为 observed，且某条 entry 满足：

```text
entry.ticks > data.ticks
```

该条目会被标为 overrange，并从正式加权总量排除。摘要会给出：

- overrange 条目数；
- active-tick 超额和 `ticks / total_ticks` 比例；
- 按 group/type 汇总的 full-reported 影响；
- clamp-to-window 的诊断估计；
- 最多 50 个示例条目。

full-reported 与 clamp-to-window 都是诊断估计，不能混入正式 totals，也不能直接当作实际窗口成本。

## 双窗口与持续性

把结果分成三类：

- **持续负载**：两个窗口都出现，group/type、对象数和坐标基本稳定，加权 `ms/tick` 接近；
- **局部波动**：只在一个窗口出现，或对象 active ticks 很少、player/item 变化明显；
- **待验证链**：trace 与某个机器、网络、自动化或序列化路径共现，但 object rate 尚不能证明因果。

比较前应确认：

- 起止时间、duration、ticks 和窗口 gap；
- 是否与 Spark 重叠；
- Mods、稳定环境字段和 schema fingerprint 是否一致；
- 是否存在非法 ticks、重复 key、unknown schema 字段或 trace 截断；
- entity exact ID 是否 churn，`(group,type)` 聚合是否稳定。

同一 diagnostics user id 只能作为弱证据，不能单独确认两个窗口属于同一实例。完整 System Report 可能包含运行时动态字段，应同时区分完整文本差异和稳定环境指纹差异。

## 与 Spark 联合分析

两类报告的职责不同：

- Spark：Server thread 的 self、inclusive、owner、family、TICKED/SIMPLE 分母和调用树；
- Observable：对象、group、type、namespace candidate、对象外围计时和对象 trace。

只有时间窗重叠时，才能把同一时段的路径作为交叉证据。以下数字永远不能相加或互换：

- Observable block-side/entity-side weighted `ms/tick`；
- Observable trace count；
- Spark MSPT、owner、family、self、inclusive；
- Spark blocking、scheduler 和 mixin 统计。

`park`、`wait`、`sleep` 表示等待延迟，不是可回收 CPU；`join`、`invoke`、`unpark`、`scan` 等方法需要结合调用上下文判断。Spark 的 inclusive 高而 self 低时，只能把它当作触发链候选，不能直接承诺可回收收益。

## 证据等级与验证

建议使用 L0-L4：

- **L0 schema/metadata**：版本、字段、来源、指纹和审计结果；
- **L1 单窗口直接观测**：窗口、ticks、加权对象时间和 trace 字段；
- **L2 多窗口稳定**：对象族群、坐标、entry 数和加权时间重复出现；
- **L3 同时间交叉证据**：Observable 路径与 Spark 调用树在重叠窗口一致；
- **L4 单变量 A/B 因果证据**：干预后指标变化，且 workload 和功能状态可比。

每条优化建议都应写清：对象或方法、当前 `ms/tick` 或占比、证据等级、可逆性、验证指标和停止/回滚条件。A/B 前先 warm-up，固定玩家位置、机器输入、版本和采样设置，尽量做至少 3 组成对窗口。

如果停用机器导致 entry 数或 active ticks 下降，必须记录 workload change，不能把被停掉的工作直接写成优化收益。没有观察到某条路径，不等于证明它不存在。

## 报告建议结构

中文性能报告可以按以下顺序组织：

1. 结论速览；
2. 报告来源、SHA-256、时间窗和环境；
3. 窗口对齐与稳定性；
4. block-side/entity-side 聚合；
5. group、namespace、type 和对象热点；
6. global/per-entry trace 路径与限制；
7. Spark 交叉验证（如有）；
8. 按收益排序的单变量 A/B 建议；
9. 方法、schema、采样和数据质量局限。

建议同时记录：

- viewer URL、`/v1/get` endpoint、raw SHA-256；
- Observable、Minecraft、loader、Java、CPU、heap、swap；
- `data.ticks` 是 observed 还是 inferred；
- blocks/entities 的对象数、active ticks、weighted `ms/tick` 和 overrange；
- trace coverage、zero-count/childless 比例和截断原因；
- Spark 的线程、聚合器、阈值、included ticks 和时间窗；
- gap、overlap、containment 以及不能相加的分母。

默认脱敏 `diagnostics.user`、绝对主机路径、主机标识和不必要的系统隐私信息；保留版本、必要资源指标、报告 ID 和 raw SHA-256。

## 常见失败处理

- viewer URL 返回 HTML：确认脚本能访问 `https://observable.tas.sh/v1/get/<id>`，或改用本地 `/v1/get` JSON；
- HTTP 403/404、DNS、超时或非 JSON：保留输入和错误，不继续归因；
- JSON 截断或缺少 `data`、`diagnostics`、`entities`、`blocks`、`traces`：先修复输入，不发布热点结论；
- `data.ticks`、entry ticks 或 rate 非法：查看 data quality 和 schema audit，不能用零替代；
- overrange 条目过多：先报告排除影响和诊断估计，不能把估计混进 totals；
- trace coverage 很低或被截断：只把已有路径作为关联证据，不能解释成完整调用图；
- Spark 与 Observable 不重叠：只能写相邻窗口的交叉观察，不能写同时间验证，也不能合并百分比或毫秒。

## 目录结构

```text
observable-profiler-analysis/
├── SKILL.md
├── README.md
├── scripts/
│   └── observable.mjs
└── references/
    ├── data-model.md
    └── report-template.zh.md
```

- `scripts/observable.mjs`：标准库实现的抓取、校验、摘要、窗口比较和 Spark 对齐；
- `references/data-model.md`：Observable 字段、加权公式、trace 和 Spark 边界；
- `references/report-template.zh.md`：中文报告骨架；
- `SKILL.md`：面向代理执行 Observable 分析任务的工作流与约束。

## 只读边界

本工具只抓取、保存、哈希和分析报告，不修改服务器、世界、配置、模组、脚本或 skill 文件，不自动重载/重启，也不执行修改性 A/B。若要停用机器、改变配置、调整布局或执行其他修改，必须先单独确认，并用可回滚、可比较的实验记录验证结果。
