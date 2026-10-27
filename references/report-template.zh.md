# Observable 报告模板

## 一、结论速览

- 一句话结论：
- 持续负载：
- 局部尖峰：
- 不支持作为主因的因素：

| 指标 | 窗口 A | 窗口 B | 口径 |
|---|---:|---:|---|
| duration |  |  | diagnostics |
| ticks |  |  | diagnostics |
| 有效 TPS |  |  | ticks / duration |
| 平均 tick 周期 |  |  | duration / ticks |
| block-side weighted ms/tick |  |  | rate × active ticks |
| entity-side weighted ms/tick |  |  | rate × active ticks |

如果 total ticks 是由 entry `ticks` 最大值推断，明确写 `inferred`，不要伪装成 diagnostics 直接提供的字段。

## 二、来源与环境

- viewer URL：
- `/v1/get` endpoint：
- raw bytes / SHA-256：
- Observable、Minecraft、loader、Java、CPU、heap、swap：
- 同一实例证据：diagnostics user id、Mods hash：

## 三、时间对齐

| 报告 | start | end | duration | ticks | 与其他窗口关系 |
|---|---|---|---:|---:|---|
| A |  |  |  |  |  |
| B |  |  |  |  |  |

写明 gap、overlap，以及是否与 Spark 重叠。不能跨不同分母相加。

## 四、聚合热点

### Dimension/group

| group | A ms/tick | B ms/tick | entry 数 | 稳定性 |
|---|---:|---:|---:|---|
|  |  |  |  |  |

### Namespace/type

| type 或 namespace | A ms/tick | B ms/tick | 主要坐标/数量 | 证据等级 |
|---|---:|---:|---|---|
|  |  |  |  |  |

同时给对象数、活跃 ticks、总 active ns、加权 ms/tick、单对象 max。解释对象数量效应；不要仅凭单个对象的 rate 或 namespace 指控一个模组。

## 五、Trace

列出两个窗口重复的完整路径。说明 count 是异步采样次数，不是时间、调用总数或 CPU；父子 count 不相加；根节点变化不等于热点变化；少量深入路径只能是强关联或待验证假设。全局 trace 与 per-entry trace 分开说明。

## 六、Spark 交叉验证

如果有 Spark，单列：

- Spark 时间窗、线程、TICKED/SIMPLE、阈值、included ticks；
- self 与 inclusive；
- Observable 对象路径如何落入 Spark 的调用树；
- 不相加的数字和原因。

## 七、验证建议

每次只改一个变量，至少记录：

- Spark MSPT median/P95/max；
- TICKED 纳入刻比例；
- Observable 目标 group/type weighted ms/tick；
- entry 数、坐标和功能状态。

每条建议写证据等级、可逆性、停止/回滚条件。

## 八、局限

说明：对象外围计时、block-side 混合类型、trace count 非耗时、缺少 loaded/forced chunk 完整计数、窗口短、服务器状态变化、与 Spark 分母不同、总 ticks 是否 inferred、重复 key/非法字段检查等。

