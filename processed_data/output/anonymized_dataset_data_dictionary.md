# Mind-Echo 匿名门诊心理对话数据集（Dataset Card）

> 文档版本：`dataset-card-v1`（覆盖时间：2026-02-20）

## 1. 数据集概览

| 项 | 值 |
|---|---|
| 数据集文件 | `processed_data/output/anonymized_dataset.json` |
| 生成脚本 | `processed_data/build_dataset_adult.py` |
| 对话来源 | `processed_data/anonymized_dialogues/**/*.txt` |
| 量表来源 | `raw_data/diagram/score.csv` |
| 组织方式 | 患者中心（patient-centered） |
| Schema 版本 | `0.2` |
| 日期处理 | 关闭（`date_processing=disabled`） |

本数据集用于门诊心理相关对话与量表联合研究，支持：
- 患者级建模（患者画像、量表与纵向就诊）
- 对话级建模（按 visit 的对话轮次）
- 量表级建模（GAD-7 / PHQ-9 监督信号）

---

## 2. 统计字段说明（来自 `stats`）

| 统计项 | 含义 |
|---|---|
| `total_files` | 扫描到的文本文件总数 |
| `converted_files` | 成功转换数 |
| `failed_files` | 转换失败数 |
| `patients_with_keywords` | 含关键词字段的患者数 |
| `patients_with_gender` | 从文件名解析出性别的患者数 |
| `patients_with_age` | 从文件名解析出年龄的患者数 |
| `patients_with_scales` | 能匹配到量表的患者数 |
| `total_visits` | 切分出的总 visit 数 |
| `errors` | 失败文件与报错详情 |

---

## 3. 数据结构总览

```text
root
├─ dataset_meta
├─ stats
└─ patients[]
   ├─ patient_id
   ├─ name / gender / age / keywords?
   ├─ scales[]
   │  ├─ GAD-7.items[7], GAD-7.total
   │  └─ PHQ-9.items[9], PHQ-9.total
   └─ visits[]
      ├─ visit_id
      └─ dialogue
         ├─ source_file
         ├─ content
         └─ turns[]
            ├─ role
            ├─ text
            └─ speaker_note?
```

---

## 4. 顶层字段

### 4.1 `dataset_meta`

| 字段 | 类型 | 必填 | 说明 | 示例 |
|---|---|---|---|---|
| `schema_version` | string | 是 | 结构版本 | `"0.2"` |
| `patient_centered` | bool | 是 | 是否患者中心 | `true` |
| `source_dir` | string | 是 | 对话源目录 | `"processed_data\\anonymized_dialogues"` |
| `date_processing` | string | 是 | 日期处理策略 | `"disabled"` |

### 4.2 `stats`

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `total_files` | int | 是 | 扫描文件数 |
| `converted_files` | int | 是 | 成功转换数 |
| `failed_files` | int | 是 | 失败数 |
| `patients_with_keywords` | int | 是 | 含关键词患者数 |
| `patients_with_gender` | int | 是 | 含性别患者数 |
| `patients_with_age` | int | 是 | 含年龄患者数 |
| `patients_with_scales` | int | 是 | 含量表患者数 |
| `total_visits` | int | 是 | visit 总数 |
| `errors` | array<object> | 是 | 失败明细 |

---

## 5. 患者字段（`patients[]`）

| 字段 | 类型 | 必填 | 说明 | 来源 |
|---|---|---|---|---|
| `patient_id` | string | 是 | 患者唯一标识，格式 `P-xxxxxx` | 文件名前缀序号 |
| `name` | string | 是 | 脱敏姓名或标题尾部名称 | 文件名解析 |
| `gender` | string | 否 | `男`/`女` | 文件名解析 |
| `age` | int | 否 | 年龄（岁） | 文件名解析 |
| `keywords` | array<string> | 否 | 患者关键词列表 | 文本头部关键词区块 |
| `scales` | array<object> | 是 | 量表记录列表 | score 表按序号匹配 |
| `visits` | array<object> | 是 | 就诊片段列表 | 对话切分 |

---

## 6. 量表字段（`patients[].scales[]`）

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `respondent_role` | string | 是 | 当前固定 `self` |
| `GAD-7.items` | array<int> | 是 | 7 个题目分值 |
| `GAD-7.total` | int | 是 | `items` 求和 |
| `PHQ-9.items` | array<int> | 是 | 9 个题目分值 |
| `PHQ-9.total` | int | 是 | `items` 求和 |

---

## 7. 就诊与对话字段（`patients[].visits[]`）

### 7.1 Visit

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `visit_id` | string | 是 | 格式 `V-xxxxxx-n` |
| `dialogue` | object | 是 | 对话信息 |

### 7.2 Dialogue

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `source_file` | string | 是 | 原始文件名 |
| `content` | string | 是 | 当前 visit 原文 |
| `turns` | array<object> | 是 | 结构化轮次 |

### 7.3 Turn

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `role` | string | 是 | 规范化角色 |
| `text` | string | 是 | 发言文本 |
| `speaker_note` | string | 否 | 标签中的备注 |

角色取值：`doctor` / `caregiver` / `patient` / `other`

---

## 8. 解析规则

### 8.1 文件名解析
基础模式：`^(\d+(?:，\d+)*)\s*(.+)$`

元数据提取优先级：
1. `姓名 性别 年龄岁`（或 `姓名 性别 年龄`）
2. `姓名 性别`
3. 回退：尾部整体作为 `name`

### 8.2 关键词提取
- 匹配：`关键词:` 或 `关键词：`
- 分隔符：`、`、`，`
- 提取后写入患者级 `keywords`

### 8.3 Visit 切分
以下任一情况会切分新 visit：
- 单独一行：`（...）`
- 单独一行：`(...)`
- 单独一行：`OUT`
- 单独一行：`出去`

### 8.4 Speaker 解析
支持 `【...】` 与 `[...]` 两种标签。

---

## 9. 已知限制
- 部分源文本存在历史编码问题，可能出现乱码，结构可转但文本质量受影响。
- 同一文件中多家庭成员对话，当前不拆成多个患者对象。
- 量表填写者身份（本人/家长）当前未自动判别。

## 10. 演进建议
1. 增加编码回退读取（`utf-8` -> `gb18030`）并输出质量标志。  
2. 增加 `patient_type`（如 `adult`、`child_with_caregiver`）。  
3. 增加 `scale_visit_link`（量表记录到 visit 的可选关联）。
