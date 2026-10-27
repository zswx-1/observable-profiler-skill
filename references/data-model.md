# Observable 数据模型与审计规则

## 获取接口

Observable viewer 页面通常是前端 shell。给定 `/p/<id>` 时，数据接口是：

```text
https://observable.tas.sh/v1/get/<id>
```

响应通常为：

```json
{
  "data": {
    "entities": {},
    "blocks": {},
    "traces": {},
    "ticks": 331
  },
  "diagnostics": {
    "start": 0,
    "duration": 30000,
    "minecraftVersion": "1.21.1",
    "modLoader": "neoforge",
    "observableVersion": "5.4.4",
    "additionalDiagnostics": {
      "System Report": "...",
      "Mods": "..."
    }
  }
}
```

缺失字段必须报告为 `N/A`，不能填 0。`null`、非有限、负数和超范围值应作为数据质量问题保留，不得静默转为 0。

## diagnostics

- `start`：Unix epoch milliseconds。
- `duration`：报告持续时间 milliseconds。
- `ticks / duration`：仅在 total ticks 已知时可推导该窗口有效 TPS；若 total ticks 由 entry `ticks` 最大值推断，必须标注 `inferred window TPS` 和推断依据。`duration / ticks` 同理。
- `additionalDiagnostics["System Report"]`：Java、CPU、heap、swap、JVM flags 等环境文本。
- `additionalDiagnostics["Mods"]`：Mod 清单；同一 diagnostics user id 和相同 Mod 清单 hash 可支持“同一实例连续窗口”的判断。

Observable diagnostics 通常没有 loaded chunks 或 forced chunks 的完整计数。单次 `ForcedChunkManager` trace 只能说明该路径被采样到，不能推导区块数量。

## blocks 与 entities

每个 group 对应一个报告返回的 dimension/虚拟世界键，值是 entry 数组；它不是服务器全量 census。entry 常见字段：

```json
{
  "position": {"x": 0, "y": 0, "z": 0},
  "type": "namespace:block_or_entity",
  "rate": 1234567.0,
  "ticks": 331,
  "traces": {
    "className": "...",
    "methodName": "...",
    "children": []
  }
}
```

在 Observable 5.4.4 / 当前 payload schema 中，源码核对的 `ProfilingData.Entry.rate` 语义是：

```text
rate = data.time.toDouble() / data.ticks
```

外围 mixin 在 `entity.tick`、`TickingBlockEntity.tick`、`BlockState.tick`、`FluidState.tick` 等路径用 `System.nanoTime()` 累加，因此 `rate` 是该对象**已记录 tick 的平均纳秒/tick**。转换：

```text
entry_ms_per_active_tick = rate / 1_000_000
window_ms_per_tick = rate × entry.ticks / total_window_ticks / 1_000_000
```

`data.blocks` 可能混合 block entity、随机方块 tick 和 fluid tick，因此报告中优先使用 `block-side entries`。对象外围计时可能包含嵌套调用；它不等于 Spark self time。

未来 Observable 版本可能改变字段或计时实现；遇到未知 schema 时先报告版本和未知字段，不要把当前实现细节当作永久 API。

稀疏对象必须保留。实体优先按 `(group, entityId)` 匹配，位置只作描述，因为实体会移动；方块按 `(group, position, type)` 检查重复 key。位置不应用来跨 group 比较。

## traces

`data.traces` 是异步 stack sample 的树：

- `count` 是采样出现次数，不是纳秒、CPU 时间、每刻成本或调用总数；
- 根节点可能是 `java.lang.Thread.hash`、`tickBlockEntities` 等异步上下文；
- 相邻窗口根节点不同不等于新增/消失了业务热点；
- 父子节点的 count 有嵌套关系，不要相加；同一路径的多种上下文不要把 `total count` 当总调用数；
- 只能使用重复的完整子路径作为关联证据；
- 少量深入某个 mod 的路径不能代表同类 entry 的全部执行成本。
- synthetic root（例如 `java.lang.Thread.hash`、`tickBlockEntities`）保留原始路径，但归责从共同业务子树开始。

## 聚合

对窗口内每个 entry 按活跃 ticks 加权：

```text
weighted_sum_ns_per_tick =
  Σ(entry.rate × entry.ticks / total_window_ticks)

weighted_ms_per_tick = weighted_sum_ns_per_tick / 1_000_000
```

按 `(group,type)` 聚合时，在加权后再分组；同时输出对象数、活跃 ticks、总 active ns、weighted ns/tick 和单对象 max。namespace 通常取 `type` 第一个 `:` 之前的部分，只是 ID 前缀候选，不等于 CPU owner；`block.`、无 namespace 或特殊 id 应保留原值并注明规则。

不要把：

- 所有 entry 的裸 `rate`；
- 不同窗口的加权总和；
- block-side 与 entity-side 与 Spark MSPT；
- Observable trace count 与 object rate；

当成同一个分母。

## 窗口判断

- 同一 type/group 在两个相邻窗口中 entry 数、坐标和加权 ms/tick 都接近：持续负载候选。
- 只在一个窗口出现，或 `ticks` 很少的 item/player：局部波动候选。
- 两个窗口 root trace 不同，但子树稳定：优先认为是采样根上下文差异。

## 与 Spark 的边界

Spark 的 self 是 sampled thread 上的 `inclusive - children`；Spark owner/family 是不同归因视图。Observable entry rate 是对象外围计时，trace count 是采样次数。三者不可相加。联合报告必须说明：

- Spark 的线程、聚合器、阈值和 included ticks；
- Observable 的 duration/ticks 和加权规则；
- 时间窗 overlap/gap；
- 每个结论属于直接观测、强关联还是待验证假设。

推荐把结论标成 L0-L4：L0 schema/metadata，L1 单窗口直接观测，L2 多窗口稳定，L3 Spark 同时间交叉证据，L4 单变量 A/B 因果证据。

