# 任务版本回填优化方案

## 问题分析

* 现有问题：TextMap的变更导致任务创建版本比实际大

* 根本原因：当前实现依赖textMap的版本信息来推断任务创建版本，但textMap的变更可能不反映任务的实际创建时间

* 影响范围：所有任务的版本信息准确性

## 优化目标

* 提高任务创建版本的准确性

* 保持回填过程的性能

* 最小化对现有代码的修改

## 优化方案

### [ ] 任务1：实现基于Git提交历史的任务版本回溯
- **Priority**: P0
- **Depends On**: None
- **Description**:
  - 创建新函数 `backfill_quest_versions_from_git_history`，专门用于回溯Git提交历史获取任务的真实创建版本
  - 针对每个任务，从最早提交开始回溯，找到任务首次出现的提交版本
  - 使用二分查找策略提高回溯效率
  - 实现临时缓存机制（内存缓存 + 临时表），用于在回溯过程中减少重复的Git查询
- **Success Criteria**:
  - 能够准确找到任务首次出现的Git提交版本
  - 回溯过程高效，避免全量扫描所有提交
  - 缓存机制能够有效减少重复查询
- **Test Requirements**:
  - `programmatic` TR-1.1: 对于已知创建版本的任务，能正确回溯到该版本
  - `programmatic` TR-1.2: 回溯过程的时间复杂度为O(log n)，其中n是提交数量
  - `programmatic` TR-1.3: 缓存机制能够减少80%以上的重复Git查询
- **Notes**:
  - 利用Git的二分查找功能加速版本定位
  - 使用内存缓存和临时表相结合的方式存储中间结果
  - 实现Git查询的错误处理和重试机制

### [ ] 任务2：优化现有任务版本回填逻辑
- **Priority**: P1
- **Depends On**: 任务1
- **Description**:
  - 修改 `backfill_quest_versions_from_history` 函数，集成基于Git的版本回溯
  - 对于TextMap变更导致的版本错误，优先使用Git回溯结果
  - 保持原有textMap推断逻辑作为 fallback
  - 修改 `quest` 表的版本推断逻辑，直接使用Git回溯结果更新 `created_version_id` 列
  - 不需要添加新列，直接更新现有 `created_version_id` 列
- **Success Criteria**:
  - 任务创建版本的准确性得到显著提升
  - 回填过程的性能不劣于现有实现
  - 不需要修改数据库结构，保持向后兼容
- **Test Requirements**:
  - `programmatic` TR-2.1: 任务创建版本与实际Git提交历史一致
  - `programmatic` TR-2.2: 回填过程的执行时间不超过现有实现的1.5倍
  - `programmatic` TR-2.3: 不需要修改数据库结构，现有功能正常运行
- **Notes**:
  - 仅对有问题的任务（创建版本明显不合理的）进行Git回溯
  - 对于正常任务，继续使用现有textMap推断逻辑
  - 实现智能判断机制，自动识别需要Git回溯的任务

### \[ ] 任务3：实现任务版本验证机制

* **Priority**: P1

* **Depends On**: 任务2

* **Description**:

  * 创建 `validate_quest_versions` 函数，验证任务版本的合理性

  * 检测并标记版本异常的任务

  * 提供版本修复建议

* **Success Criteria**:

  * 能够准确识别版本异常的任务

  * 验证过程高效，不影响系统性能

* **Test Requirements**:

  * `programmatic` TR-3.1: 能够识别90%以上的版本异常任务

  * `programmatic` TR-3.2: 验证过程的执行时间不超过10秒

* **Notes**:

  * 基于版本号的合理性（如任务创建版本不应晚于更新版本）

  * 基于任务ID的连续性和版本分布

### \[ ] 任务4：添加命令行工具支持

* **Priority**: P2

* **Depends On**: 任务3

* **Description**:

  * 在 `DBBuild.py` 中添加新的命令行选项，支持单独运行任务版本验证和修复

  * 提供详细的版本异常报告

  * 支持批量修复版本异常的任务

* **Success Criteria**:

  * 命令行工具能够正确执行版本验证和修复

  * 报告内容清晰、准确

* **Test Requirements**:

  * `programmatic` TR-4.1: 命令行工具能够正确执行所有功能

  * `human-judgement` TR-4.2: 报告内容易于理解，包含必要的信息

* **Notes**:

  * 提供详细的使用说明和示例

  * 支持不同级别的详细程度

### \[ ] 任务5：性能优化和缓存机制

* **Priority**: P2

* **Depends On**: 任务1

* **Description**:

  * 实现Git提交历史的缓存机制，避免重复查询

  * 优化数据库查询，减少IO操作

  * 并行处理多个任务的版本回溯

* **Success Criteria**:

  * 缓存机制能够显著减少Git查询次数

  * 并行处理能够提高回溯效率

* **Test Requirements**:

  * `programmatic` TR-5.1: 缓存机制能够减少50%以上的Git查询

  * `programmatic` TR-5.2: 并行处理能够提高30%以上的回溯速度

* **Notes**:

  * 使用内存缓存和磁盘缓存相结合的方式

  * 合理控制并行度，避免系统资源过载

## 预期效果

* 任务创建版本的准确性提高到95%以上

* 回填过程的性能保持在可接受范围内

* 提供清晰的版本验证和修复机制

* 最小化对现有代码的修改，降低引入新问题的风险

## 实现风险

* Git仓库访问权限和性能问题

* 并行处理可能导致的资源竞争

* 缓存机制可能导致的内存使用问题

* 对现有功能的影响

## 缓解措施

* 实现Git访问的错误处理和重试机制

* 合理控制并行度和缓存大小

* 充分测试，确保对现有功能无负面影响

* 提供详细的日志和监控

