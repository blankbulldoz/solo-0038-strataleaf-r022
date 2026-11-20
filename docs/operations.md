# 恢复与观测

备份必须调用 archives.backup，不能只拷贝活动主文件。backup 与 restore 都不覆盖存在目标；恢复使用新路径，先校验来源 SHA-256，再打开新句柄。主库恢复生成新身份，副本恢复绑定来源。恢复清理原机保留点与消费者；消费方重新注册时须根据已恢复的 floor/head 判断是否需要重建。

diagnostics.inspect(deep=True) 检查 SQLite 完整性、外键、提交连续性、当前索引与文档投影；fingerprint 比较指定读视图的键、文档版本、内容及到期事实。前者不能证明所有业务数据正确，后者相同也不能代替模式与索引定义比对。checkpoint 返回 SQLite 的 busy、log、checkpointed 信息，并不承诺截断 WAL。inspect 同时核对每个消费者位置是否仍在 floor/head 之间，以及字段级订阅是否指向现存集合。

字段级订阅的运维要点：`feed.register_diff` 的 `since` 决定首次基线所在的读版本，因此应在 pin 或确认水位允许的范围内选择；基线随首次投递返回，消费者应先用基线建立状态再按 `events` 增量应用。`feed.read_diff(..., baseline=True)` 可在任何时刻重新取一份所选状态用于对账，`feed.rebuild` 则从当前 ack 起重建并返回 `revision` 与 `more`，分页消费时循环到 `more` 为假。模式演进后 `schema_changed` 为真并附带新模式与新基线，消费者应据此决定是否重新选择字段；确认越过该点后不再重复提示。重开数据库后消费者行、基线版本和确认位置都持久在 consumers 表中，重建结果与关闭前一致。

examples.observations 的 contention 会让独立连接暂持 IMMEDIATE 锁，记录第二句柄的 busy 超时及释放后的成功；retention_pressure 记录读保留点和消费者分别阻止水位推进的现象。它们用独立临时文件，不接触生产路径。观察包含真实修订和异常，适合复核运行边界。

关闭所有调用方句柄再移动数据目录；不支持共享网络盘或多主写入。在空间紧张时先核实 pin/消费者，推进已完成的确认后再回收；回收不自动缩小文件，物理容量由 SQLite 空闲页复用。到期清理只在显式调用时执行，不能把墙钟到期等同于读视图变动。
