# TextMap版本写入逻辑修复计划

## 问题分析
当前的textmap版本写入逻辑存在一个问题：早期commit（如104c21c6530885e450975b13830639e9ca649799）中的textmap文件名使用小写格式（如textCHS.json），而当前的文件名解析逻辑只支持大写开头的文件名（如TextMapCHS.json）。

## 根因分析
1. `textmap_name_utils.py` 中的 `parse_textmap_file_name` 函数使用正则表达式 `_TEXTMAP_BASE_STEM_RE` 匹配文件名
2. 该正则要求文件名以 `TextMap` 或 `Text` 开头（大写），不支持 `text` 开头的文件名
3. 早期commit中的文件名使用小写格式，导致解析失败，文件被跳过
4. 虽然版本管理是基于commit的，但如果早期文件被跳过，可能会导致版本信息不完整

## 任务分解

### [x] 任务1：修改文件名解析逻辑
- **Priority**: P0
- **Depends On**: None
- **Description**: 
  - 修改 `textmap_name_utils.py` 中的 `_TEXTMAP_BASE_STEM_RE` 正则表达式，使其支持小写开头的文件名
  - 确保解析逻辑能够正确处理 `textCHS.json` 等早期文件名格式
- **Success Criteria**:
  - 正则表达式能够匹配 `textCHS.json` 等早期文件名格式
  - 解析后的base_name与当前格式一致（如 `textCHS.json` 解析为 `TextMapCHS.json`）
- **Test Requirements**:
  - `programmatic` TR-1.1: 编写测试用例验证 `parse_textmap_file_name` 函数能够正确解析 `textCHS.json`、`textJP.json` 等早期文件名
  - `programmatic` TR-1.2: 验证解析结果与当前格式的文件名解析结果一致

### [x] 任务2：测试历史数据回填
- **Priority**: P1
- **Depends On**: 任务1
- **Description**:
  - 运行 `backfill_textmap_versions_from_history` 函数，测试对早期commit的处理
  - 验证早期文件名的textmap数据能够正确导入并关联到相应版本
- **Success Criteria**:
  - 早期commit中的textmap数据能够被正确导入
  - 版本信息能够正确关联到相应的commit
- **Test Requirements**:
  - `programmatic` TR-2.1: 运行历史数据回填命令，验证无错误发生
  - `programmatic` TR-2.2: 检查数据库中是否包含早期commit的textmap数据

### [x] 任务3：验证版本管理逻辑
- **Priority**: P2
- **Depends On**: 任务2
- **Description**:
  - 验证版本创建和更新逻辑不会因为文件名差异而被重写
  - 确保不同文件名格式的相同内容会被正确识别为同一版本
- **Success Criteria**:
  - 版本ID不会因为文件名差异而重复创建
  - 相同内容的textmap数据不会因为文件名变化而被错误更新
- **Test Requirements**:
  - `programmatic` TR-3.1: 验证相同内容的textmap数据在不同文件名格式下版本信息一致
  - `programmatic` TR-3.2: 验证版本ID的唯一性和一致性

## 实现方案
1. 修改 `textmap_name_utils.py` 中的正则表达式，添加对小写开头文件名的支持
2. 确保解析逻辑能够正确处理早期文件名格式，并映射到与当前格式一致的base_name
3. 运行历史数据回填测试，验证修复效果
4. 检查版本管理逻辑，确保版本信息的一致性

## 预期结果
- 早期commit中的textmap数据能够被正确导入
- 版本信息能够正确关联到相应的commit
- 不同文件名格式的相同内容会被正确识别为同一版本
- 版本创建和更新逻辑不会因为文件名差异而被重写