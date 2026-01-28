# 数据处理流水线 - 完整文档

## 项目概述

本数据处理流水线用于将Excel量表数据和对话文本数据合并，生成符合要求的结构化JSON数据集。按照用户需求，实现了三个核心功能的解耦：
1. **txt读取模块** - 读取和解析对话文本文件
2. **xlsx读取模块** - 读取和解析Excel量表数据
3. **数据合并模块** - 将两者合并为指定格式的JSON

## 生成的数据集格式

生成的JSON数据集格式如下：

```json
{
  "dataset_id": "outpatient_depression_v1",
  "patient_id": "P10086", 
  "demographics": {
    "age": 34,
    "gender": "female",
    "occupation": "teacher"  // 如果Excel里有就填，没有就null
  },
  "longitudinal_data": [  // 纵向数据的核心：列表里放多次就诊记录
    {
      "session_id": "P10086_visit_1",
      "visit_type": "initial",  // 初诊
      "date": "2023-10-01",     // 如果文件名或Excel里有日期
      "phq9_assessment": {      // 来自 Excel 的数据
        "total_score": 18,
        "severity": "Moderately severe", // 可以写逻辑自动根据分数生成
        "items": [3, 2, 1, 3, ...]       // 如果Excel里有细项分
      },
      "dialogue": [             // 参考 OpenAI 格式，方便未来训练
        {
          "turn_id": 1,
          "role": "doctor",
          "content": "最近睡眠情况怎么样？"
        },
        {
          "turn_id": 2,
          "role": "patient",
          "content": "非常糟糕，入睡很困难。"
        }
      ]
    },
    {
      "session_id": "P10086_visit_2",
      "visit_type": "follow_up", // 复诊
      "phq9_assessment": {
        "total_score": 12        // 分数下降，体现疗效
      },
      "dialogue": [ ... ]
    }
  ]
}
```

## 文件结构

```
Data_Pipeline/
├── txt_reader.py              # 对话文本读取模块
├── excel_reader_fixed.py      # Excel数据读取模块（简化版）
├── data_merger.py             # 数据合并与转换模块
├── main_pipeline.py           # 主数据处理流水线
├── test_pipeline_simple.py    # 简单测试脚本
├── process_script.py          # 原始处理脚本（参考）
├── README_COMPLETE.md         # 本文档
├── raw_data/                  # 原始数据目录
│   ├── scores.xlsx           # Excel量表数据
│   └── dialogues/            # 对话文本文件目录
│       ├── P10086_visit_1.txt
│       ├── P10086_visit_2.txt
│       └── ...
└── processed_data/           # 处理后的数据输出目录
    └── test_output/          # 测试输出目录
```

## 模块说明

### 1. txt_reader.py - 对话文本读取模块

**功能**：
- 读取单个或多个txt文件
- 解析对话文本，识别说话者角色（医生/病人）
- 提取对话轮次并添加turn_id
- 支持多种对话格式（中文/英文标识）
- 从文件名中提取会话信息

**主要类**：
- `TXTReader`: 主读取器类
- `DialogueTurn`: 对话轮次数据类

**使用方法**：
```python
from txt_reader import TXTReader

reader = TXTReader(encoding="utf-8")
content = reader.read_file("P10086_visit_1.txt")
dialogue = reader.parse_dialogue(content)
file_info = reader.extract_session_info("P10086_visit_1.txt")
```

### 2. excel_reader_fixed.py - Excel数据读取模块

**功能**：
- 读取Excel文件，支持多种格式（xlsx, xls）
- 解析PHQ-9量表数据，计算严重程度
- 提取患者人口统计学信息
- 数据清洗和验证
- 转换为结构化字典格式

**PHQ-9严重程度分级**：
- 0-4: Minimal
- 5-9: Mild
- 10-14: Moderate
- 15-19: Moderately severe
- 20-27: Severe

**使用方法**：
```python
from excel_reader_fixed import ExcelReader

reader = ExcelReader(engine='openpyxl')
patients_data = reader.read_and_process("./raw_data/scores.xlsx")
```

### 3. data_merger.py - 数据合并与转换模块

**功能**：
- 合并Excel数据和对话数据
- 构建纵向就诊记录结构
- 转换数据格式为指定的JSON结构
- 处理数据缺失和异常情况
- 生成符合训练要求的数据集

**主要类**：
- `DataMerger`: 数据合并器类

**使用方法**：
```python
from data_merger import merge_and_convert, save_dataset

# 合并和转换数据
final_data = merge_and_convert(excel_data, dialogue_data, dataset_id="outpatient_depression_v1")

# 保存数据
save_dataset(final_data, "./processed_data/output.json", format="json")
```

### 4. main_pipeline.py - 主数据处理流水线

**功能**：
- 整合所有模块，提供完整的处理流程
- 命令行参数支持
- 日志记录和错误处理
- 数据统计和验证

**命令行用法**：
```bash
# 基本用法（使用默认路径）
python main_pipeline.py

# 指定文件路径
python main_pipeline.py --excel ./raw_data/scores.xlsx --dialogues ./raw_data/dialogues/

# 指定输出格式和数据集ID
python main_pipeline.py --format jsonl --dataset-id outpatient_depression_v2

# 启用详细日志
python main_pipeline.py --verbose
```

**命令行参数**：
- `--excel`, `-e`: Excel文件路径（默认: ./raw_data/scores.xlsx）
- `--dialogues`, `-d`: 对话目录路径（默认: ./raw_data/dialogues/）
- `--output`, `-o`: 输出目录路径（默认: ./processed_data/）
- `--dataset-id`, `-i`: 数据集标识符（默认: outpatient_depression_v1）
- `--format`, `-f`: 输出格式，json或jsonl（默认: json）
- `--encoding`, `-c`: 文本文件编码（默认: utf-8）
- `--verbose`, `-v`: 启用详细日志输出

## 快速开始

### 1. 准备数据

1. **Excel数据**：将PHQ-9量表数据保存为 `./raw_data/scores.xlsx`
   - 必须包含 `patient_id` 列
   - 可选列：`age`, `gender`, `occupation`, `phq9_total` 等

2. **对话数据**：将对话文本文件放入 `./raw_data/dialogues/` 目录
   - 文件名格式：`{patient_id}_{visit_type}.txt`，如 `P10086_visit_1.txt`
   - 文件内容格式：
     ```
     医生：最近睡眠情况怎么样？
     病人：非常糟糕，入睡很困难。
     医生：这种情况持续多久了？
     病人：大概有两个月了。
     ```

### 2. 运行数据处理流水线

```bash
# 进入Data_Pipeline目录
cd Data_Pipeline

# 运行主流水线
python main_pipeline.py

# 或使用完整参数
python main_pipeline.py --excel ./raw_data/scores.xlsx --dialogues ./raw_data/dialogues/ --output ./processed_data/ --format json
```

### 3. 查看输出

处理完成后，输出文件将保存在 `./processed_data/` 目录下：
- `outpatient_depression_v1_YYYYMMDD_HHMMSS.json` - 主数据文件
- `outpatient_depression_v1_YYYYMMDD_HHMMSS_stats.json` - 统计信息文件

## 测试

### 运行测试脚本

```bash
# 运行简单测试
python test_pipeline_simple.py

# 测试结果将显示：
# - 模块导入测试
# - 各模块功能测试
# - 示例数据创建
# - 完整测试结果摘要
```

### 测试内容

1. **模块导入测试**：验证所有模块能否正确导入
2. **txt_reader测试**：测试对话文本读取和解析功能
3. **excel_reader测试**：测试Excel数据读取和解析功能
4. **data_merger测试**：测试数据合并和转换功能
5. **主流水线测试**：测试完整流水线配置和运行
6. **示例数据创建**：创建测试用的示例数据文件

## 数据格式要求

### Excel文件格式要求

| 列名 | 说明 | 是否必需 |
|------|------|----------|
| patient_id | 患者ID | 是 |
| age | 年龄 | 否 |
| gender | 性别 | 否 |
| occupation | 职业 | 否 |
| phq9_total | PHQ-9总分 | 否 |
| phq9_1 到 phq9_9 | PHQ-9各项目分数 | 否 |
| visit_date | 就诊日期 | 否 |

### 对话文件格式要求

1. **文件名格式**：`{patient_id}_{visit_type}.txt`
   - 示例：`P10086_visit_1.txt`, `P10086_follow_up.txt`
   - 支持：`initial`, `follow_up`, `first`, `second` 等就诊类型

2. **文件内容格式**：
   ```
   医生：{对话内容}
   病人：{对话内容}
   医生：{对话内容}
   ```
   或英文格式：
   ```
   Doctor: {dialogue content}
   Patient: {dialogue content}
   Doctor: {dialogue content}
   ```

## 错误处理

### 常见错误及解决方法

1. **文件不存在错误**
   ```
   FileNotFoundError: Excel文件不存在: ./raw_data/scores.xlsx
   ```
   **解决方法**：确保文件路径正确，或使用 `--excel` 参数指定正确路径。

2. **编码错误**
   ```
   UnicodeDecodeError: 'utf-8' codec can't decode byte...
   ```
   **解决方法**：使用 `--encoding` 参数指定正确的编码，如 `gbk`。

3. **Excel读取错误**
   ```
   ValueError: 文件格式不支持
   ```
   **解决方法**：确保Excel文件格式正确，或安装必要的依赖库。

4. **数据合并错误**
   ```
   KeyError: 'patient_id'
   ```
   **解决方法**：检查Excel文件中是否包含 `patient_id` 列。

### 日志文件

每次运行主流水线时，会自动生成日志文件：
- `data_pipeline_YYYYMMDD_HHMMSS.log` - 详细运行日志

## 扩展和自定义

### 添加新的数据字段

要添加新的数据字段，需要修改以下文件：

1. **excel_reader_fixed.py**：
   - 在 `DEMOGRAPHIC_COLUMN_MAPPING` 中添加新的列名映射
   - 在 `extract_patient_info` 方法中添加提取逻辑

2. **data_merger.py**：
   - 在 `convert_to_final_format` 方法中添加新字段的处理逻辑

### 支持新的对话格式

要支持新的对话格式，需要修改：

1. **txt_reader.py**：
   - 在 `role_patterns` 中添加新的角色识别模式
   - 在 `_identify_role_and_content` 方法中添加新的格式处理逻辑

### 修改输出格式

要修改输出格式，需要修改：

1. **data_merger.py**：
   - 修改 `convert_to_final_format` 方法中的数据结构
   - 修改 `build_longitudinal_data` 方法中的就诊记录结构

## 依赖库

### 必需库
- `pandas` - 数据处理和分析
- `openpyxl` - Excel文件读取（xlsx格式）

### 安装依赖
```bash
pip install pandas openpyxl
```

## 性能优化建议

1. **大数据处理**：对于大量数据，建议使用分批处理
2. **内存优化**：使用生成器而不是列表处理大量对话文件
3. **并行处理**：可以考虑使用多进程处理多个患者数据
4. **缓存机制**：对于重复读取的数据，可以添加缓存机制

## 版本历史

- **v1.0.0** (2026-01-28): 初始版本，完成所有核心功能
  - 实现三个解耦的模块：txt读取、xlsx读取、数据合并
  - 生成符合要求的JSON数据集格式
  - 提供完整的测试脚本和文档
  - 支持命令行参数和日志记录

## 作者

数据管道团队

## 许可证

本项目遵循MIT许可证。