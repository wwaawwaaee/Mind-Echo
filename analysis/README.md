# Mind-Echo 心理对话数据集分析

基于 Auto_CLIWC 的中文 LIWC 词典，对门诊心理对话数据进行语言特征分析。

## 目录结构

```
analysis/
├── liwc_analyzer.py          # LIWC 特征提取核心模块（基于 jieba 分词）
├── dataset_comparator.py      # 两个数据集对比分析
├── correlation_analyzer.py    # 量表相关性分析
├── visualizer.py             # 可视化（散点图、热力图）
├── run_analysis.py           # 主入口脚本
├── requirements.txt          # 依赖包
└── README.md                # 本文件
```

## 功能特性

### 1. jieba 分词引擎
- 使用 jieba 中文分词库进行精准分词
- 匹配率从 ~10% 提升至 ~55%
- 准确识别多字词（焦虑、担心、紧张等）

### 2. LIWCAnalyzer (`liwc_analyzer.py`)

基于 Auto_CLIWC 的中文 LIWC 词典进行文本特征提取。

**主要类别**:
- `affect`: 情感词 (posemo, negemo, anx, anger, sad)
- `social`: 社交词 (family, friend, humans)
- `cogmech`: 认知词 (insight, cause, discrep)
- `bio`: 生理词 (body, health)
- `percept`: 感知词 (see, hear, feel)

### 3. DatasetComparator (`dataset_comparator.py`)

对比 `with_caregiver` 和 `without_caregiver` 两个数据集:
- 基础统计对比
- 轮次分布对比
- 主要叙述者语言特征对比
- 护理人员特征分析

### 4. CorrelationAnalyzer (`correlation_analyzer.py`)

分析 LIWC 特征与量表(GAD-7, PHQ-9)得分的相关性:
- Pearson 相关系数
- 显著性检验
- 假设验证（H1-H4）
- 量表效度验证

### 5. ScatterVisualizer (`visualizer.py`)

生成可视化图表:
- 语言特征与量表相关性散点图
- 相关性热力图

## 使用方法

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 运行完整分析

```bash
python run_analysis.py
```

### 3. 自定义参数

```bash
python run_analysis.py \
    --input-dir ../processed_dataset/output \
    --liwc-dict ../Auto_CLIWC/Auto_CLIWC/datasets/sc_liwc.dic \
    --output-dir ./results
```

## 输出结果

分析结果保存在 `results/` 目录:

```
results/
├── analysis_results_YYYYMMDD_HHMMSS.json  # 详细结果
├── summary_latest.json                     # 最新摘要
├── analysis_report.md                     # Markdown 报告
└── figures/
    ├── scatter_correlation_combined.png   # 散点图
    └── correlation_heatmap.png             # 热力图
```

## 假设验证

| 假设 | 描述 | 预期 |
|------|------|------|
| H1 | 量表得分高 → 负向情感词(negemo)多 | 正相关 |
| H2 | 量表得分高 → 正向情感词(posemo)少 | 负相关 |
| H3 | 量表得分高 → 焦虑词(anx)多 | 正相关 |
| H4 | 量表得分高 → 悲伤词(sad)多 | 正相关 |

## 分析指标

| 指标类别 | 具体指标 | 说明 |
|----------|----------|------|
| 情感 | negemo, posemo, anx, anger, sad | 情感词密度 |
| 认知 | insight, cause, discrep | 认知过程词密度 |
| 健康 | health, body | 健康相关词密度 |
| 社交 | humans, family | 社交参考词密度 |
| 量表 | GAD-7, PHQ-9 | 焦虑/抑郁量表得分 |

## 数据集说明

| 数据集 | 场景 | 主要叙述者 | 分析角色 |
|--------|------|-----------|----------|
| with_caregiver | 儿科（家长代述） | caregiver | caregiver 语言 |
| without_caregiver | 成人（患者自述） | patient | patient 语言 |

## 依赖

- scipy
- numpy
- matplotlib
- jieba
