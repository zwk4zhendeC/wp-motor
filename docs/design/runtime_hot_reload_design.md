# WPL / OML / KnowDB 运行时更新设计

## 背景

当前引擎中，`WPL`、`OML`、`KnowDB` 都属于“启动时装配、运行时长期持有”的资源：

- `WPL` 会在 parser worker 启动前编译为 `WplEvaluator`
- `OML` 会在资源构建阶段加载，并被 sink 运行链路直接引用
- `KnowDB` 会在初始化阶段构建 authority DB，并通过全局 provider 与线程本地副本提供查询

因此，模型文件变化后，现网运行态默认不会自动看到新版本。

业务侧已经明确，本问题的核心不是“是否做到教科书式热更新”，而是：

1. 更新窗口有多长
2. `PID` 是否保持不变
3. 正常路径下是否尽量不丢数据
4. 当优雅 drain 超时后，是否允许用更简单的强切方案结束更新

当前结论是：

- 更新必须发生在**同一进程内**
- **不重启进程，不变更 PID**
- `source` 保留，`parse / sink` 允许运行时重建
- 正常情况下优先优雅 drain
- 如果 drain 超时，**允许强制替换，并接受该场景下的丢数据**

基于这一结论，当前实现已经从最初讨论的 `P0`，提升为 `P1`。

## 当前实现结论

当前代码已实现的方案是：

- **方案级别**：`P1`
- **触发入口**：控制面 `CommandType::LoadModel`
- **进程语义**：进程不退出，`PID` 保持不变
- **保留边界**：`source / picker` 留在当前进程内
- **替换边界**：`parse / sink / infra / maintainer` 作为一组可替换 processing tasks 重建
- **切换方式**：先预构建新资源与新 processing，再隔离旧链路并切换
- **超时策略**：优先 graceful drain，超时后 fallback 到 force replace

这意味着本文档不再把 `P0` 作为主实施方案，而是把它保留为对比基线。

## 目标

1. 支持统一更新 `WPL`、`OML`、`KnowDB`
2. 更新过程保持在同一进程内完成
3. `source` 不销毁，`parse / sink` 可重建
4. 将实际中断窗口尽量压缩到可接受范围，目标仍为 **10 秒内**
5. 新资源预构建失败时，不破坏旧运行态
6. 优先保证正常路径不丢数据；仅在超时兜底路径接受丢数据

## 非目标

当前设计不追求：

1. 自动文件监听与自动触发 reload
2. `WPL / OML / KnowDB` 的独立局部热补丁接口
3. parser / sink 双代长期并存
4. 对所有 source 类型都提供绝对无损保证

## 运行时边界

当前运行时需要分清“保留对象”和“可替换对象”。

### 保留对象

保留对象在 reload 前后不重建：

- 当前进程本身
- `PID`
- `source`
- `picker`
- 控制面、监控面等外层运行框架

这里的关键语义不是“source 代码停掉并重启”，而是：

- source 仍在当前进程内
- reload 期间通过 `picker/source` 隔离控制，停止继续向旧 processing 投递
- 切换完成后恢复向新 processing 投递

### 可替换对象

当前实现将以下对象视为一组可替换 processing：

- parser workers
- sink workers
- infra sink workers
- maintainer task

这组对象会在 reload 时整体换代。

## P0 与 P1 的差别

### P0：严格 drain 后再构建

流程是：

1. 停止 source 继续取数
2. 等待旧 `parse / sink` 全量排空
3. 排空完成后再构建新资源
4. 重建并恢复处理

优点：

- 语义直接
- 理论上更容易给出“先排空、再切换”的无损解释

缺点：

- 更新窗口更长
- 构建时间全部落在停顿窗口内
- 一旦重建时间偏长，用户体感更明显

### P1：预构建后切换

流程是：

1. 在旧 processing 仍运行时预构建新资源
2. 预启动新的 processing task set
3. 真正切换时只做隔离、drain、安装新 processing、恢复

优点：

- 中断窗口更短
- 构建失败不会影响旧 processing
- 更符合“十秒内无感”的目标

代价：

- 切换编排比 P0 略复杂
- 需要明确 reload 期间的 pending / timeout 语义

**当前已实现方案：`P1`。**

## P1 详细设计

### 1. 触发与作用范围

当前只提供统一入口：

```rust
CommandType::LoadModel
```

一次触发同时覆盖：

- `WPL`
- `OML`
- `KnowDB`
- 与它们绑定的 parse / sink processing runtime

当前不提供：

- `ReloadWpl`
- `ReloadOml`
- `ReloadKnowdb`

原因很直接：当前实现的切换边界是 processing task set，而不是单资源局部替换。

### 2. 预构建阶段

reload 开始后，先在旧 processing 仍可服务时完成以下动作：

1. 重新加载 `WPL / OML / KnowDB`
2. 构建新的 `EngineResource`
3. 基于新资源启动新的 processing task set
4. 暂不把这组新 processing 接入 picker 分发路径

这一阶段若失败：

- 直接返回失败
- 旧 processing 保持原样继续工作

这也是当前实现选择 `P1` 的核心收益。

### 3. 切换阶段

当前切换顺序如下：

```text
control plane
    -> LoadModel
    -> prebuild new resource
    -> start new processing tasks
    -> isolate picker
    -> disconnect parse router (mark reloading)
    -> wait old parser/sink/infra drained
    -> if timeout: force stop old processing
    -> install new processing tasks
    -> replace parse router
    -> resume picker
```

关键点如下：

1. `picker` 先被 isolate，避免继续向旧 parser 投递
2. parse router 在 reload 期间进入 `Reloading` 状态
3. reload 窗口内未投递成功的数据保留在 picker pending 中，等待新 parser 接回
4. 旧 processing 优先走 graceful drain
5. graceful drain 超时后，再进入 force replace

### 4. graceful drain 与 force replace

当前实现采用“两段式”结束旧 processing。

#### 第一阶段：优雅 drain

优先等待以下角色处理完成：

- parser
- sink
- infra

`maintainer` 在 drain 收尾阶段停止。

这一阶段的目标是：

- 尽量把旧链路中已经进入处理通道的数据消费完成
- 在不引入双代长期并存的前提下，争取无损切换

#### 第二阶段：超时兜底

如果 graceful drain 在限定时间内没有完成：

1. 记录 warning
2. 对旧 processing 执行 `force_stop_processing()`
3. 安装并切到新的 processing
4. 恢复 picker

这条路径下，**允许丢失旧 processing 中仍在途的数据**。

这是当前产品决策已经接受的语义，不再把“超时后必须继续等待直到绝对无损”作为实现约束。

### 5. 数据一致性语义

当前设计按两种路径定义。

#### 正常路径：graceful drain 成功

语义为：

1. reload 前已进入旧 processing 的数据，由旧 processing 完成处理
2. reload 期间 picker 不再向旧 parser 投递
3. reload 期间新到数据保留在 source/picker 侧可恢复路径中
4. 新 processing 安装完成后，后续数据按新 `WPL / OML / KnowDB` 处理

在该路径下，系统目标是**不主动丢数**。

#### 兜底路径：graceful drain 超时

语义为：

1. 旧 processing 中未完成的数据可能被放弃
2. force replace 后立即恢复新 processing
3. 用更短的恢复时间换取实现复杂度与可控性

这一路径下，系统明确接受：

- parser 在途数据可能丢失
- sink 在途数据可能丢失
- 是否进一步扩散到 source 侧，取决于具体 source 的缓冲/重放语义

### 6. 故障处理

当前实现需要保证“新构建失败不污染旧运行态”，并避免 reload 过程留下孤儿任务。

主要处理原则如下：

1. 新资源构建失败：旧 processing 不切换
2. 新 processing 预启动成功，但后续 `isolate_picker()` 失败：回收未安装的新 processing
3. graceful drain 失败且 `force_stop_processing()` 也失败：回收未安装的新 processing，并返回错误
4. parser 新任务启动过程中若部分成功、部分失败：回收已启动的 parser 任务后再返回错误

目标是把所有失败都收敛为：

- 旧 processing 继续跑，或
- reload 明确失败退出当前动作

而不是进入“半切换、半失联”的未知状态。

## 可观测性

建议围绕以下阶段打日志与指标：

- `reload requested`
- `reload prebuild begin`
- `reload prebuild ready`
- `picker isolated`
- `reload drain begin`
- `reload drain done`
- `reload force replace`
- `reload install new processing`
- `reload resume picker`
- `reload done total_ms=...`
- `reload failed stage=... reason=...`

建议观测指标：

- `runtime_reload_total`
- `runtime_reload_success_total`
- `runtime_reload_force_replace_total`
- `runtime_reload_fail_total`
- `runtime_reload_prebuild_ms`
- `runtime_reload_drain_ms`
- `runtime_reload_switch_ms`
- `runtime_reload_total_ms`

## 测试要求

### 单元测试

1. parse router 在 `begin_reload()` 后返回 `Reloading`
2. picker 在 `Reloading` 状态下保留 pending，而不是把它当成永久关闭
3. parser 启动部分失败时会回收已启动任务

### 集成测试

1. 启动旧运行态并处理旧样本
2. 修改模型
3. 触发 `LoadModel`
4. 验证新样本进入新 parser / sink 路径
5. 验证 graceful drain 超时后会进入 force replace 分支

### 回归测试

1. 未触发 reload 时，steady-state 不增加额外热点路径成本
2. reload 失败时旧 processing 不被破坏
3. 连续多次 reload 时，router / picker 不进入错误状态

## 当前状态

截至当前版本，设计与实现已经对齐到以下状态：

- `P1` 已实现
- `P0` 仅保留为更保守的基线方案，不作为当前落地主线
- reload 入口为 `CommandType::LoadModel`
- reload 采用“预构建 -> isolate -> graceful drain -> 超时强切 -> 恢复”的编排
- `PID` 保持不变
- graceful drain 超时后允许丢数据

后续如果业务重新要求“超时后也绝不丢数据”，则不能在现有语义上做小修补，而需要重新收紧以下约束：

1. source 必须具备明确的暂停/回放保障
2. sink 必须具备更强的完成确认语义
3. reload 总耗时与 drain 超时策略需要重新设计
