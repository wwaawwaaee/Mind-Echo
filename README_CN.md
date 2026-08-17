# Mind-Echo

[简体中文](README_CN.md) | [English](README.md)

## 研究用途与范围警示

> **仅限科研使用，不可用于临床使用、诊断、分诊或部署。**
> Mind-Echo 当前应被定位为一个基于**单中心、探索性、概念验证**数据的仓库，研究对象是**文本化门诊对话记录**。现有仓库**不**构成经过临床验证的产品，不支持真实部署结论，也不提供经过验证的队列级音频或多模态结果。

## 项目概览

Mind-Echo 研究门诊医患家属对话中的文本特征，是否与问卷式焦虑、抑郁风险标签存在关联。仓库包含数据处理脚本、描述性统计、类 LIWC 文本分析、基础与修订版机器学习实验，以及面向论文的矢量图生成流程。

对当前仓库最稳妥的表述是，**这是一个关于文本化门诊对话的单中心探索性概念验证研究仓库**。它更适合作为方法和复现实验仓库，而不是临床就绪系统的证据。

## 已核实的数据快照

以下信息来自 `basic_status_summary/patient_basic_stats_summary.json` 与增量修订实验元数据。

| 项目 | 已核实数量 | 单位 | 说明 |
| --- | ---: | --- | --- |
| 对话源文件 | 84 | 文件 | 当前处理快照中，每个患者记录对应一个源文件。 |
| 患者记录 | 84 | 条记录 | 不能据此推断为 84 个唯一家属。 |
| 就诊片段 | 131 | 个 visit segment | 分布在 84 条患者记录中的就诊级对话片段。 |
| 配对 GAD-7/PHQ-9 量表记录 | 109 | 组配对量表记录 | 这是配对量表条目，不是独立家属身份数。 |
| 修订版机器学习队列 | 78 | 条就诊记录 | 用于增量修订 OOF 实验。 |
| 修订版机器学习队列 | 63 | 名患者 | 仅按 `patient_id` 分组。 |
| 对话总轮次 | 5,531 | 轮 | 可做描述统计，但**不能**视为独立样本。 |
| 角色轮次分布 | doctor 2,824，caregiver 1,889，patient 818 | 轮 | 基于处理后对话角色统计。 |

## 仓库内容与功能

- `processed_dataset/`：对话构建、说话人标注、匿名化、按 caregiver 拆分等脚本。
- `basic_status_summary/`：患者级计数、对话轮次汇总、描述性 markdown 与图表。
- `analysis/`：类 LIWC 语言分析、相关性计算、汇总报告与图形生成。
- `analysis/emotion_model/`：基础实验代码、增量修订实验与 runner 脚本。
- `analysis/paper_figures/english_vector_pipeline/`：面向论文的英文矢量图流程，以及增量修订图输出。
- `raw_data/`、`patient's_ocr/`、`ocr_results/`：本地原始或中间工作目录。敏感源材料及已忽略的中间产物不属于可复现的 Git 跟踪发布内容。

## 项目结构

```text
Mind-Echo/
├── LICENSE
├── README.md
├── README_CN.md
├── requirements.txt
├── adenoid_analysis.py
├── basic_status_summary/
│   ├── patient_basic_stats.py
│   ├── patient_basic_stats_summary.json
│   ├── caregiver_chart.py
│   ├── caregiver_statistics.md
│   └── adenoid_hypertrophy_stats.md
├── processed_dataset/
│   ├── anonymize_names.py
│   ├── anonymized_dialogues/
│   ├── build_dataset.py
│   ├── count_dialogue_prefix_ids.py
│   ├── label_speakers.py
│   ├── output/
│   │   ├── anonymized_dataset.json
│   │   ├── anonymized_dataset_with_caregiver.json
│   │   ├── anonymized_dataset_without_caregiver.json
│   │   └── anonymized_dataset_data_dictionary.md
│   └── split_by_caregiver.py
├── analysis/
│   ├── README.md
│   ├── basic_statistics.py
│   ├── correlation_analyzer.py
│   ├── dataset_comparator.py
│   ├── liwc_analyzer.py
│   ├── run_analysis.py
│   ├── visualizer.py
│   ├── results/
│   ├── emotion_model/
│   │   ├── README_experiments.md
│   │   ├── core/
│   │   ├── experiments/
│   │   │   ├── mind_echo_revision_minimal/
│   │   │   ├── with_caregiver_feature_v2_cv/
│   │   │   ├── with_caregiver_label_comparison/
│   │   │   ├── window_comparison/
│   │   │   └── direct_llm_zero_shot_testset/
│   │   ├── revision/
│   │   │   └── tests/
│   │   └── runners/
│   └── paper_figures/
│       └── english_vector_pipeline/
├── raw_data/
├── patient's_ocr/
└── ocr_results/
```

## 安装与环境准备

在仓库根目录执行：

```bash
pip install -r requirements.txt
```

根目录依赖文件已经覆盖文档流程所需的通用数据处理、统计、绘图、分词与机器学习依赖。LIWC 相关实验仍需要下文所述的外部词典。

### 外部 LIWC 词典要求

增量修订 runner 默认指向一个**仓库外部**的 LIWC 词典路径：

`D:\vscode-project\git-project\Auto_CLIWC\Auto_CLIWC\datasets\sc_liwc.dic`

该路径定义在 `analysis/emotion_model/revision/paths.py` 中，**并未随本仓库提供**。如果你的本地环境不同，请通过 runner 的 CLI 传入你自己的词典路径，或按本地情况调整配置。

## 复现实验命令

除特别说明外，以下命令都在仓库根目录 `D:\vscode-project\git-project\Mind-Echo` 下执行。

### 修订版工具测试

```bash
python analysis/emotion_model/revision/tests/test_revision_utils.py
```

### 增量修订 OOF 实验

```bash
python analysis/emotion_model/runners/run_mind_echo_revision_main_v2_oof.py
python analysis/emotion_model/runners/run_mind_echo_revision_window_oof.py
```

### 论文矢量图生成

```bash
python analysis/paper_figures/english_vector_pipeline/generate_academic_figures.py --figure all
python analysis/paper_figures/english_vector_pipeline/build_revision_figures.py
```

如果先进入 `analysis/paper_figures/english_vector_pipeline/` 目录，也可以执行：

```bash
python build_revision_figures.py
```

## 修订实验方法说明

`analysis/emotion_model/experiments/mind_echo_revision_minimal/` 下的增量修订流程，是当前仓库中最清晰、最可复现的机器学习协议。

- 队列规模：**78 条就诊记录，63 名患者**。
- 分组方式：按 `patient_id` 做 **五折 `GroupKFold`**。
- 泄漏控制：交叉验证声明**仅限患者分组**。
- 折内预处理：TF-IDF、SVD、scaling 都只在各训练折内拟合。
- 汇总方式：基于**汇总的 out-of-fold 预测**报告结果。
- 基线：每折训练集多数类基线，以及**带随机种子的患病率随机基线**。
- 指标：**BACC、F1、recall、specificity、ROC-AUC、PR-AUC**。
- 不确定性：使用 **2,000 次 patient bootstrap** 计算 **95% 置信区间**。
- 窗口实验：比较 `4`、`6`、`8`、`10`、`12`、`15` 与 `full_text` 等上下文窗口，并使用固定特征模式保持可比性。

## 关键输出及其位置

### 描述统计与分析输出

- `basic_status_summary/patient_basic_stats_summary.json`：根目录层面最可靠的计数快照。
- `analysis/results/summary_latest.json`：英文矢量图流程复用的源汇总文件。

### 增量修订实验输出

- `analysis/emotion_model/experiments/mind_echo_revision_minimal/manifest/`：manifest 与患者折映射。
- `analysis/emotion_model/experiments/mind_echo_revision_minimal/main_v2_oof/summary.json`：主修订 OOF 结果。
- `analysis/emotion_model/experiments/mind_echo_revision_minimal/window_oof/summary.json`：上下文窗口 OOF 结果。

### 图形输出

- `analysis/paper_figures/english_vector_pipeline/outputs/`：通用学术图输出目录。
- `analysis/paper_figures/english_vector_pipeline/outputs_revision_guide/`：增量修订图指南输出目录。

针对增量修订图指南：

- **Figure 1 修订版只有文本与表格**，文件为 `figure1_1.md` 与 `figure1_2.csv`。
- **Figure 3 输出**编号为 `figure3_1`、`figure3_2`、`figure3_3`。
- **Figure 4 输出**编号为 `figure4_1`、`figure4_2`、`figure4_3`。
- **SVG 是规范的跟踪型矢量格式**。PNG 与 PDF 预览可以在本地重新生成。

## 当前证据与保守解读

当前仓库能够支持的有限结论是，在这个单中心数据集上，可以把门诊对话文本特征组织成探索性的统计分析与机器学习实验，并且能用带不确定性估计的患者分组修订实验进行复现。

它**不**支持更强的结论，例如临床有效性、部署成熟度、医生话语的因果效应、可泛化患病率、多模态优越性，或实时精神健康监测。当前窗口实验同样**不能**证明 **6 到 8 轮** 是最优上下文长度。

## 局限性

- 单中心。
- 数据规模较小。
- 没有外部验证队列。
- 当前经过验证的队列级修订流程以文本分析为主。
- 现有仓库证据中，没有经过验证的队列级音频或多模态结果。
- caregiver presence 是根据对话角色**推导**得到的。
- 当前数据中**没有结构化 `caregiver_id` 或 `family_id`**。
- 交叉验证声明**仅限患者分组**。
- 当前处理记录无法独立核验量表填写者身份。
- 5,531 个对话轮次不能被解释为 5,531 个独立样本。
- 量表阈值阳性不应被解释为临床诊断，也不应被解释为总体患病率估计。
- 本仓库不做临床部署声明。

## 数据治理与伦理

本仓库应被视为一个包含脱敏处理材料与分析代码的**科研仓库**，而不是对“完整底层数据已无条件开放”的声明。现有内容并未建立完整源数据的无限制公开使用条款。

即使材料经过脱敏，复用任何医疗文本前，仍应由使用者自行审查本地机构、法律与伦理要求。当前文档只支持科研用途表述，不主张已获得临床批准、已接入临床流程，也不主张量表填写者身份已被独立验证。

## 引用

待形成可引用论文或正式记录后，再在此处补充引用信息。

## 许可证

本仓库包含 `LICENSE` 文件，当前许可证为 **Apache License 2.0**。
