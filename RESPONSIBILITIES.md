# 职责说明

`scan.py` 只是兼容入口，把 `scan(source, predicate, projections, batch_size)`
原样转发给 `driver.scan`；不保留旧执行器，也不做旁路。

- `driver.py` — 读取驱动：校验 `batch_size`、遍历 `groups()`、按批切行、
  持有源对象并执行实际 `read`、按投影顺序求值与逐行交付。它只消费需求核
  给出的交换对象，不遍历表达式、不判断算子名。
- `demand.py` — 需求调度核：独占共享图的需求传播、按节点身份与行号的行缓存
  以及未完成求值的暂停/恢复。它只看图结构（哪些操作数是 payload、哪些是
  子节点），从不检查具体算子名或单元值，也不接触源、列适配器或读取回调。
- `vectors.py` — 无状态向量算子：只接收算子名、字面量/列名、需求行和已有
  子向量，返回 `Done` / `NeedChild` / `NeedColumn`。不接收 `Expr`、不遍历图、
  不递归求值、无跨调用缓存、不做 I/O。

依赖方向为 `driver -> demand -> vectors`，`vectors` 无业务模块依赖，
`demand` 不依赖 `driver`/`columns`，生产模块不导入 `reference`。

## 缓存归属与需求补算
每个批次 `driver` 新建一个 `DemandKernel`，行缓存 `{节点对象: {行号: 值}}`
由需求核持有，因此共享节点在相交但不同的行集上只补算尚未计算的行。
“未计算”与“已计算为 NULL”通过行号键的存在性区分：NULL 是正常缓存值，
不会触发重算。节点只有本轮局部运算全部成功后才写入缓存。
空需求在进入算子/子节点前直接返回 `Complete([])`，不产生列需求。

## 暂停交接
`DemandKernel.evaluate` 返回 `Complete` 或 `Paused(column, rows, frames)`；
`Paused` 是显式列需求（组由驱动持有，无需重复传入）。驱动取回向量后调用
`resume(paused, values)`，核把值并入该叶节点缓存并沿同一栈继续。
`vectors.step` 以 `NeedChild(index, rows)` / `NeedColumn(column, rows)` /
`Done(values)` 表达局部需求，`and`/`or` 只向 1 号子节点要非短路行，
`if` 先要 True 行、再要其余行；这些子向量由核推进，调用者在同一轮算子里
不重新遍历。

## 异常传播
异常不包裹：`source.read` 抛出的异常对象原样穿过驱动与转发入口；源先记录
再报错时该次调用保留。算子层的结果只在整轮成功后交由核写入，失败节点不
缓存，但其已完成子节点的单元仍保留。比较/除法先完成左右两个子向量再逐行
运算，因此 `if` 的冷分支和零除数不会提前触发读取或除零。
