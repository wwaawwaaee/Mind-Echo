# 门诊多模态情绪风险专利创新升级方案与RC-SEA验证计划

## TL;DR
> **Summary**: 将第三版专利的创新中心从“LIWC/TF-IDF＋传统模型＋固定窗口”迁移为“角色条件序贯证据累积＋稳定性/不确定性联合判定＋预警/继续采集/暂缓输出”的闭环自适应停止机制，并以现有数据完成文本型原型验证，最终交付创新方案包和中文SVG技术方案图。
> **Deliverables**:
> - `D:\曦源\曦源临时\专利创新升级方案\主诉期闭环自适应停止_创新升级方案.md`
> - `D:\曦源\曦源临时\专利创新升级方案\主诉期闭环自适应停止_技术方案图.svg`
> - `D:\曦源\曦源临时\专利创新升级方案\现有技术差异矩阵.csv`
> - `D:\曦源\曦源临时\专利创新升级方案\权利要求追溯矩阵.csv`
> - `D:\曦源\曦源临时\专利创新升级方案\RC-SEA实验验证报告.md`
> - additive prototype under `analysis/emotion_model/patent_rcsea/` and aggregate results under `analysis/emotion_model/experiments/patent_rcsea_oof/`
> **Effort**: Large
> **Parallel**: YES - 3 implementation waves plus final review
> **Critical Path**: Task 1 → Task 3 → Task 5 → Task 6 → Task 7 → Task 8 → Task 9/10/11 → Task 12

## Context
### Original Request
审阅第三版专利，解决LIWC、TF-IDF及“传统模型”导致的创造性不足问题，提出改进方向与配套技术方案，并参照 `D:\Obsidian Vault\research_flowchart 1.svg` 生成SVG方案设计图。

### Interview Summary
- 允许加入尚未实现但将在提交前补实验验证的新机制。
- 本轮交付范围为“创新升级方案包＋SVG”，不生成完整第四版专利全文。
- 独立权利要求核心选择“闭环自适应停止”，而非文本型保守方案或单独的缺失模态融合。
- 默认使用中文专利表达；新文件写入第三版同级的 `专利创新升级方案` 目录，第三版DOCX不得覆盖。
- 音频、LoRA、完整多模态、在线部署均按“拟研发/可选从属实施例”处理，除非新增证据完成验证。

### Metis Review (gaps addressed)
- 强制建立证据/R&D状态矩阵，防止把拟研发内容写成已实现。
- 强制建立权利要求—说明书—公式—附图—证据追溯矩阵。
- 强制建立现有技术差异矩阵，回答“为何不是动态窗口＋不确定性＋多模态的简单拼接”。
- 独立权利要求必须包含具体技术链和判定规则，禁止只写功能性结果。
- 固定窗口仅作基线，不能声称6–8轮最优。
- SVG必须区分已实现、拟研发和可选模态，并通过XML及文本规则自动验证。

## Work Objectives
### Core Objective
形成一套可供专利代理人继续撰写的创新升级方案，并以可复现实验验证文本型RC-SEA核心机制的可行性；独立权利要求保护闭环控制机制，而不是任何单一特征、分类器、Transformer、GNN、注意力或校准算法。

### Deliverables
- 创新性诊断、现有技术差异、核心方案、公式参数、权利要求骨架、证据矩阵、实验结论和边界说明组成的Markdown方案包。
- 可解析、纯矢量、中文、1600×1740三栏布局SVG。
- 文本型RC-SEA原型、测试、运行器和聚合实验结果。
- 查新差异矩阵与权利要求追溯矩阵。

### Definition of Done
- `python analysis/emotion_model/patent_rcsea/tests/test_rcsea.py` 全部通过。
- RC-SEA运行器在固定78 visits / 63 patients manifest上完成patient-grouped OOF回放并输出聚合结果。
- 固定窗口、全文、角色无关、无稳定性、无不确定性边界等基线/消融均有结果和patient-bootstrap 95% CI。
- 方案包的十个规定章节、证据状态矩阵、现有技术差异矩阵和追溯矩阵均完整。
- SVG可由XML解析，viewBox为 `0 0 1600 1740`，无嵌入位图，必需中文节点齐全。
- 第三版DOCX的执行前后SHA-256一致。

### Must Have
- 核心链：角色标注 → 角色条件编码 → 序贯证据累积 → 风险/稳定性/不确定性计算 → warn/continue/abstain → 风险区间与证据片段。
- 文本型实现采用现有可复现数据；音频分支以模态存在掩码和虚线可选模块表示。
- 明确模型：现有基线为 Logistic Regression、Linear SVM、Random Forest、Gradient Boosting、Ridge；原型默认使用 Logistic Regression/Bootstrap ensemble，不再写“传统模型”。
- 医疗表述限定为“非诊断性风险预警、辅助提示、供临床人员参考”。
- 每项拟研发机制均给出输入、状态、公式、阈值选择、边界行为和验证实验。

### Must NOT Have
- 不覆盖或修改第三版DOCX。
- 不生成完整第四版专利全文或声称已完成正式法律意见。
- 不把LIWC、TF-IDF、GNN、Transformer、注意力、动态窗口、Conformal Prediction或多模态融合单独写成创新中心。
- 不声称6–8轮最优、现有音频融合有效、LoRA已训练、系统已实时部署或具备临床诊断能力。
- 不写虚构实验数值，不把结果占位符表述为已完成结果。
- 不提交患者级预测CSV、原始对话文本、fold map或可识别医疗数据。

## Verification Strategy
> ZERO HUMAN INTERVENTION - all verification is agent-executed.
- Test decision: tests-after；复用现有 `unittest` 风格，新增RC-SEA单元/集成测试。
- QA policy: 每项任务必须有可执行的正常路径与失败/边界路径。
- Evidence: `.sisyphus/evidence/task-{N}-{slug}.{ext}`。
- Figure QA: XML解析、viewBox/文本/样式/无位图检查，并以浏览器或SVG渲染器生成截图证据。
- Patent QA: 章节搜索、禁用词搜索、矩阵空单元检查、公式符号表一致性检查、第三版文件哈希对比。

## Execution Strategy
### Parallel Execution Waves
Wave 1: Tasks 1–4 — 基线审阅、正式查新、核心机制规格、证据/追溯矩阵。

Wave 2: Tasks 5–8 — RC-SEA数据构造、停止机制、OOF运行器、基线/消融实验。

Wave 3: Tasks 9–12 — 创新方案文档、权利要求骨架、SVG、最终包验证。

### Dependency Matrix
| Task | Depends On | Blocks |
|---|---|---|
| 1 | — | 3, 4, 9, 10, 12 |
| 2 | — | 3, 4, 9, 10 |
| 3 | 1, 2 | 5, 6, 9, 10, 11 |
| 4 | 1, 2, 3 | 9, 10, 12 |
| 5 | 3 | 6, 7 |
| 6 | 3, 5 | 7, 8 |
| 7 | 5, 6 | 8 |
| 8 | 7 | 9, 10, 11, 12 |
| 9 | 1, 2, 3, 4, 8 | 12 |
| 10 | 1, 2, 3, 4, 8 | 12 |
| 11 | 3, 8 | 12 |
| 12 | 9, 10, 11 | Final Verification |

### Agent Dispatch Summary
| Wave | Tasks | Categories |
|---|---:|---|
| 1 | 4 | deep, writing |
| 2 | 4 | deep, unspecified-high |
| 3 | 4 | writing, visual-engineering, unspecified-high |
| Final | 4 | oracle, unspecified-high, deep |

## TODOs
> Implementation + Test = ONE task. Every task includes agent-executed QA.

- [ ] 1. 固化第三版基线、提取正文并形成创新性问题清单

  **What to do**:
  - 在任何处理前计算第三版DOCX的SHA-256，并记录到 `.sisyphus/evidence/task-1-v3-hash-before.txt`。
  - 使用 `patent-disclosure-skill/tools/docx_to_md.py` 将第三版转换到 `D:\曦源\曦源临时\专利创新升级方案\working\第三版提取.md`；媒体文件仅存入同级working目录，不覆盖原DOCX。
  - 形成 `working\第三版创新性审阅.md`，逐项记录：截断悖论、LIWC/TF-IDF成熟、传统模型歧义、late concatenation、Qwen/LoRA无实现、语音模型未定义、音频无队列实验、熔断机制无参数、医学措辞边界。
  - 对第三版四个保护点分别标记 `保留并重构 / 降级为从属 / 移入可选实施例 / 删除`；固定结论：PP1重构为闭环自适应停止，PP2降级为可选编码/模态实施例，PP3并入角色条件累积，PP4重构为warn/continue/abstain状态机。
  - 完成后再次计算第三版DOCX SHA-256，必须与执行前一致。

  **Must NOT do**:
  - 不在第三版DOCX内写入、接受修订、更新域或另存覆盖。
  - 不把第三版中的实验数值自动当作可信新证据；必须与仓库摘要交叉核对。

  **Recommended Agent Profile**:
  - Category: `writing` — 原因：需要精读中文专利并进行结构化审阅。
  - Skills: [`patent-disclosure-skill`] — 需要Office转换与专利迭代审阅规范。
  - Omitted: [`figure-designer`] — 本任务不绘图。

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: [3, 4, 9, 10, 12] | Blocked By: []

  **References**:
  - Source: `D:\曦源\曦源临时\一种基于主诉期截断预测的门诊多模态情绪风险预警系统及方法 张哲毓-第三版.docx` — 唯一基准稿，禁止覆盖。
  - Pattern: `C:\Users\Administrator\.agents\skills\patent-disclosure-skill\prompts\iteration_context.md` — 非破坏迭代和版本规则。
  - Pattern: `C:\Users\Administrator\.agents\skills\patent-disclosure-skill\prompts\merger.md` — 增量方案合并边界；本轮仅借用审阅规则，不产出第四版。
  - Evidence: `analysis/emotion_model/experiments/mind_echo_revision_minimal/main_v2_oof/summary.md` — 当前主模型真实结果。
  - Evidence: `analysis/emotion_model/experiments/mind_echo_revision_minimal/window_oof/summary.md` — 固定窗口无明确最优的证据。

  **Acceptance Criteria**:
  - [ ] 原DOCX执行前后SHA-256完全一致。
  - [ ] `第三版创新性审阅.md`包含九类问题和四个保护点处置结论。
  - [ ] 审阅中不存在“6–8轮最优”“已完成LoRA”“已验证音频融合”等表述。

  **QA Scenarios**:
  ```
  Scenario: 正常提取并完成基线审阅
    Tool: Bash (PowerShell)
    Steps: Test-Path原DOCX；Get-FileHash保存before；执行docx_to_md；检查输出MD包含“模型构建”“关键点和发明人欲保护点”；Get-FileHash保存after并比较。
    Expected: 转换MD与审阅MD存在且非空；before=after；原DOCX修改时间不变。
    Evidence: .sisyphus/evidence/task-1-v3-baseline.txt

  Scenario: DOCX转换失败或正文缺节
    Tool: Bash (PowerShell)
    Steps: 捕获转换退出码；检查关键标题数量；缺任一标题时终止后续专利写作任务。
    Expected: 返回明确失败并保留原DOCX；不得以代理摘要替代缺失正文继续执行。
    Evidence: .sisyphus/evidence/task-1-v3-baseline-error.txt
  ```

  **Commit**: NO | Files: 外部working审阅文件，不进入仓库

- [ ] 2. 完成正式查新并建立现有技术差异矩阵

  **What to do**:
  - 按 `prior_art_search.md` 先调用国知局检索，每次仅使用一个语义块，至少执行六轮：`主诉期 截断预测`、`多角色 对话 情绪预警`、`序贯证据 自适应停止`、`多模态 情绪风险`、`不确定性 风险预警`、`缺失模态 融合`。
  - 国知局不可用或命中不足时，使用Google Patents/公开论文页补充并逐条打开核验；不得仅凭搜索摘要写区别。
  - `现有技术差异矩阵.csv`固定列：`identifier,title,publication_date,date_status,url,application,role_conditioning,sequential_accumulation,adaptive_stopping,modality_mask,uncertainty_or_interval,abstention,evidence_span,overlap,difference,claim_implication`。
  - 至少纳入8条高相关文献，其中必须覆盖：音频文本对话预测、角色依赖上下文、图/超图多模态ERC、早期加权/自适应窗口、 uncertainty-aware ERC、校准/Conformal Prediction。
  - 每条都写“重叠点、RC-SEA本质区别、权利要求影响”，并在方案中形成一句核心区别：创新在门诊主诉阶段的角色证据—模态可用性—稳定性—不确定性共同驱动的状态转换闭环，而非任一单模块。

  **Must NOT do**:
  - 不编造国知局公开号、摘要或URL；无摘要须明确标注并用可核验详情页补足。
  - 不把2026年晚于当前申请优先权日的材料直接当成可适用现有技术；须单列“风险参考/非确定现有技术”，由代理人判断日期效力。

  **Recommended Agent Profile**:
  - Category: `deep` — 原因：涉及专利语义查新、日期和组合区别判断。
  - Skills: [`patent-disclosure-skill`] — 需要国知局优先检索和摘要使用规则。
  - Omitted: [`figure-designer`] — 查新不绘图。

  **Parallelization**: Can Parallel: YES | Wave 1 | Blocks: [3, 4, 9, 10] | Blocked By: []

  **References**:
  - Search procedure: `C:\Users\Administrator\.agents\skills\patent-disclosure-skill\prompts\prior_art_search.md`。
  - External: `https://patents.google.com/patent/US20240177729A1/en` — 多说话人音频/文本情绪预测与后续轮次 forecasting。
  - External: `https://patentsgazette.uspto.gov/week01/OG/html/1542-1/US12518776-20260106.html` — 多模态ERC GNN/超图。
  - External: `https://patents.google.com/patent/US9842106B2/en` — 角色依赖上下文语言理解。
  - External: `https://www.cambridge.org/core/journals/natural-language-processing/article/reliable-uncertainty-estimation-in-emotion-recognition-in-conversation-using-conformal-prediction-framework/CE882D4B782256860B81AD2834A27477` — ERC可靠性与Conformal Prediction。
  - External: `https://www.isca-archive.org/interspeech_2024/zhao24g_interspeech.pdf` — 自适应窗口多模态情绪识别。
  - External: `https://ojs.aaai.org/index.php/AAAI/article/view/29876` — 自适应多模态对话图学习。

  **Acceptance Criteria**:
  - [ ] 差异矩阵至少8行，每行identifier/title/url/overlap/difference/claim_implication非空。
  - [ ] 每个URL已打开验证且与标题/标识一致。
  - [ ] 至少一行明确指出每个高饱和方向为何不能单独作为创新核心。
  - [ ] 公开日期晚于潜在优先权日的项目具有显式日期风险标记。

  **QA Scenarios**:
  ```
  Scenario: 形成可追溯现有技术矩阵
    Tool: Bash + browser/web fetch
    Steps: 解析CSV；逐行HTTP访问URL；检查16列完整；按identifier去重；统计高相关行数。
    Expected: >=8条唯一高相关项目，URL可访问，重叠/区别/权利要求影响均非空。
    Evidence: .sisyphus/evidence/task-2-prior-art-matrix.json

  Scenario: 国知局无命中或摘要缺失
    Tool: Bash + browser/web fetch
    Steps: 记录空结果/错误；转Google Patents稳定页；核对标题和公开文本；标记来源降级及摘要缺失。
    Expected: 不生成虚构条目；矩阵仍达到最低覆盖或明确阻塞并列出缺口。
    Evidence: .sisyphus/evidence/task-2-prior-art-error.md
  ```

  **Commit**: NO | Files: `D:\曦源\曦源临时\专利创新升级方案\现有技术差异矩阵.csv`

- [ ] 3. 冻结RC-SEA技术规格、公式和状态机

  **What to do**:
  - 将专利通用机制与当前文本型验证实施例分开定义：通用机制处理每个角色话轮；验证实施例在第4/6/8/10/12/15个照护者话轮检查点使用截至该点的全部医生/患者/照护者上下文。
  - 固定符号：`x_t`话轮内容、`g_t∈{doctor,patient,caregiver}`角色、`m_t`模态存在掩码、`z_t=E_{g_t,m_t}(x_≤t)`角色条件证据、`h_t=ρh_{t-1}+W_{g_t}z_t`累积状态、`r_t=σ(w^T h_t+b)`风险、`[L_t,U_t]`bootstrap ensemble分位区间、`u_t=U_t-L_t`不确定性宽度、`s_t=max_{j=t-h+1..t}|r_j-r_{j-1}|`稳定性。
  - 固定状态规则：当 `r_t≥θ_r ∧ u_t≤θ_u(m_t) ∧ s_t≤θ_s ∧ t≥t_min` 时WARN；未满足且 `t<T_max` 时CONTINUE；达到 `T_max`仍不满足或模态质量不足时ABSTAIN。
  - 验证实施例固定：`h=2`个相邻检查点；`t_min∈{4,6}`、`T_max=15`；`θ_r∈{0.50,0.60,0.70}`、`θ_u∈{0.15,0.20,0.25}`、`θ_s∈{0.03,0.05,0.10}`、`ρ∈{0.5,0.7,0.9}`，只在训练折内部患者分组验证集选择。
  - 配置选择采用固定字典序：先筛选decision coverage≥0.70且Recall不低于同折full-text基线Recall减0.05的配置；再最大化BACC；再最小化平均消耗对话比例；再最小化abstention rate；仍并列时选择较高 `θ_r`、较低 `T_max`。
  - 证据片段固定输出风险增量绝对值最大的前3个话轮，带角色、顺序号和脱敏文本摘要；方案包中仅给算法，不暴露真实患者文本。
  - 输出 `working\RC-SEA技术规格.md`，含符号表、状态转移表、参数表和9个边界案例。

  **Must NOT do**:
  - 不将Conformal Prediction、Transformer/GNN或特定分类器写入独立权利要求必要要素。
  - 不把风险区间写成医学置信诊断区间；它只是模型不确定性表示。

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — 原因：需要封闭公式、状态机、参数选择和专利可实施性。
  - Skills: [`patent-disclosure-skill`] — 公式符号与权利要求可实施性需专利规范。
  - Omitted: [`figure-designer`] — 图形在Task 11处理。

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: [4, 5, 6, 9, 10, 11] | Blocked By: [1, 2]

  **References**:
  - Pattern: `analysis/emotion_model/revision/windows.py:reconstruct_window_rows` — 固定窗口从真实话轮边界重建。
  - Pattern: `analysis/emotion_model/revision/experiment.py:run_oof_experiment` — OOF预测和折内训练模式。
  - Pattern: `analysis/emotion_model/revision/metrics.py:bootstrap_patient_ci` — 当前患者级bootstrap，仅用于聚合CI；新原型需另建预测级bootstrap ensemble。
  - Contract: `analysis/emotion_model/experiments/mind_echo_revision_minimal/manifest/manifest_metadata.json` — 78 visits、63 patients、5 folds。
  - Patent baseline: `working\第三版创新性审阅.md` — 第三版保护点和必须修复的矛盾。

  **Acceptance Criteria**:
  - [ ] 符号表覆盖公式中100%的符号且无一符多义。
  - [ ] WARN/CONTINUE/ABSTAIN对所有输入状态互斥且穷尽。
  - [ ] 参数网格、选择顺序、训练/验证/测试边界明确，无测试集调参路径。
  - [ ] 至少覆盖：缺音频、无caregiver、少于4轮、早高后低、早低后高、高风险高不确定、角色冲突、Tmax仍不稳定、全模态正常九类边界。

  **QA Scenarios**:
  ```
  Scenario: 状态规则正常闭环
    Tool: Python specification checker
    Steps: 枚举风险高低、不确定性高低、稳定/不稳定、t<Tmax/t=Tmax组合，执行规则真值表。
    Expected: 每种组合仅返回WARN、CONTINUE或ABSTAIN之一；高风险+低不确定+稳定返回WARN。
    Evidence: .sisyphus/evidence/task-3-state-table.json

  Scenario: 高风险但音频缺失且区间过宽
    Tool: Python specification checker
    Steps: m=[text=1,audio=0]，r>=θr，u>θu(text-only)，分别测试t<Tmax和t=Tmax。
    Expected: t<Tmax为CONTINUE；t=Tmax为ABSTAIN；不得直接WARN。
    Evidence: .sisyphus/evidence/task-3-state-table-error.json
  ```

  **Commit**: NO | Files: `D:\曦源\曦源临时\专利创新升级方案\working\RC-SEA技术规格.md`

- [ ] 4. 建立证据状态矩阵与权利要求追溯矩阵

  **What to do**:
  - 为所有模块赋唯一状态：`已实现`、`第三版文本支持但未实验验证`、`拟研发`、`建议删除或降级为从属`。
  - 矩阵至少覆盖：固定窗口、patient-grouped OOF、TF-IDF+SVD、LIWC、domain rules、LR/SVM/RF/GB/Ridge、角色标注、RC-SEA、稳定性规则、预测级区间、abstention、模态mask、音频融合、LoRA/Qwen、证据片段、在线部署。
  - `权利要求追溯矩阵.csv`固定列：`claim_element,claim_level,description_section,formula_or_rule,svg_node,evidence_status,repository_reference,experiment_reference,wording_guardrail`。
  - 独立权利要求行固定为：角色序列、角色条件编码、模态mask、序贯累积、风险/区间/稳定性、三态停止、非诊断风险区间、触发证据片段。
  - 若某独立要素缺说明书段落、公式/规则或SVG节点，标为阻塞，不允许Task 10完成。

  **Must NOT do**:
  - 不以“第三版已描述”替代实施支持；文本描述和实验支持必须分列。
  - 不允许独立权利要求要素只有功能效果而没有输入/运算/输出链。

  **Recommended Agent Profile**:
  - Category: `writing` — 原因：核心是证据与权利要求可追溯性。
  - Skills: [`patent-disclosure-skill`] — 需要专利层级和支持性判断。
  - Omitted: [`figure-designer`] — 仅引用SVG节点ID，不绘制。

  **Parallelization**: Can Parallel: NO | Wave 1 | Blocks: [9, 10, 12] | Blocked By: [1, 2, 3]

  **References**:
  - Implementation: `analysis/emotion_model/revision/features.py` — 现有TF-IDF+SVD/LIWC/domain/hybrid实现。
  - Implementation: `analysis/emotion_model/revision/manifest.py:build_patient_fold_map` — patient-grouped GroupKFold。
  - Implementation: `analysis/emotion_model/core/audio_features.py:extract_audio_features` — 仅轻量WAV特征，不代表队列融合。
  - Gap: `analysis/emotion_model/inputs/audio/README_audio_import.md` — 音频输入仍为占位。
  - Gap: `README.md` — 明确无验证过的队列级音频/多模态结果。
  - Specification: `working\RC-SEA技术规格.md`。

  **Acceptance Criteria**:
  - [ ] 每个独立权利要求要素均有非空description_section、formula_or_rule、svg_node和evidence_status。
  - [ ] 所有仓库引用路径存在；不存在的拟研发文件不得伪装成现有证据。
  - [ ] 音频融合、LoRA、calibration、abstention和在线停止均正确标为拟研发或从属。

  **QA Scenarios**:
  ```
  Scenario: 追溯矩阵完整
    Tool: Python CSV validator
    Steps: 解析CSV；验证必需列；筛选claim_level=independent；检查空单元和引用路径。
    Expected: 独立要素8类全部存在且关键列无空值；现有证据路径可读取。
    Evidence: .sisyphus/evidence/task-4-traceability.json

  Scenario: 拟研发要素被误标为已实现
    Tool: Python CSV validator
    Steps: 对RC-SEA、prediction interval、abstention、modality mask、cohort audio、LoRA设置禁止状态规则。
    Expected: 任一误标为“已实现”即失败，并输出具体行号。
    Evidence: .sisyphus/evidence/task-4-traceability-error.json
  ```

  **Commit**: NO | Files: `D:\曦源\曦源临时\专利创新升级方案\权利要求追溯矩阵.csv`, `working\证据与研发状态矩阵.csv`

- [ ] 5. 构建角色条件前缀数据与折内编码器

  **What to do**:
  - 新建 `analysis/emotion_model/patent_rcsea/`，包含 `__init__.py`、`config.py`、`prefixes.py`、`features.py` 和 `tests/test_rcsea.py`。
  - 复用固定78-visit manifest和patient fold map；不得重排或重新随机划分外层fold。
  - `prefixes.py`从原始turn boundaries构造检查点：第4/6/8/10/12/15个目标角色话轮；存在caregiver时目标角色为caregiver，否则为patient。每个检查点包含此前全部doctor/patient/caregiver话轮，并分别输出三个脱敏内存文本字段；少于4个目标话轮的visit标记`insufficient_prefix=True`。
  - 每个prefix保留 `patient_id,visit_id,checkpoint,target_role,role_turn_counts,modality_mask,label`；禁止将真实文本写入实验聚合输出。
  - `features.py`实现 `RoleLexSVDEncoder`：每个角色单独使用fold-local TF-IDF+SVD；caregiver/patient增加现有LIWC和domain-rule特征；doctor仅作上下文编码。三角色块、role-presence flags和modality mask按固定顺序拼接。
  - 每个角色TF-IDF参数固定为 `max_features=1000,ngram_range=(1,2),min_df=2,max_df=0.95,sublinear_tf=True`；SVD维度为 `min(80,max(2,n_features-1))`，随机种子42；若某角色训练语料不足以满足min_df或SVD维度，回退为不做SVD的稀疏TF-IDF块并在config中记录，不得丢弃该角色。
  - 训练prefix使用 `sample_weight=1/n_prefixes_for_visit`，避免长对话因前缀数量多而被重复加权。
  - 配置固定seed=2026；TF-IDF/SVD/scaler均只能在当前训练折prefix上拟合。

  **Must NOT do**:
  - 不把同一visit的多个prefix跨fold拆分；外层分组永远是patient_id。
  - 不在Git跟踪输出中写patient_id/visit_id对应的预测、原始文本或证据片段。

  **Recommended Agent Profile**:
  - Category: `deep` — 原因：需处理多层级前缀、角色编码和泄漏控制。
  - Skills: [] — 现有仓库模式足够，不需额外技能。
  - Omitted: [`patent-disclosure-skill`] — 本任务是实验实现。

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: [6, 7] | Blocked By: [3]

  **References**:
  - Pattern: `analysis/emotion_model/revision/windows.py:reconstruct_window_rows` — 从原始话轮重建窗口并验证标签。
  - Pattern: `analysis/emotion_model/revision/manifest.py:validate_no_group_leakage` — patient级泄漏断言。
  - Pattern: `analysis/emotion_model/revision/features.py:build_fold_features` — fold-local TF-IDF/SVD/scaler模式。
  - API: `analysis/emotion_model/core/domain_rule_features.py` — 领域规则特征。
  - API: `analysis/liwc_analyzer.py` — 中文LIWC特征；外部词典路径由CLI传入。
  - Contract: `analysis/emotion_model/experiments/mind_echo_revision_minimal/manifest/manifest_metadata.json` — 固定样本和fold计数。

  **Acceptance Criteria**:
  - [ ] 78个visit均只映射到一个外层patient fold；63名患者无跨折。
  - [ ] 每个prefix三个角色文本仅含检查点之前的turn，不含未来信息。
  - [ ] encoder在测试prefix上只调用transform，不调用fit/fit_transform。
  - [ ] 同一visit所有训练prefix的sample weights之和为1±1e-9。

  **QA Scenarios**:
  ```
  Scenario: 正常三角色对话前缀构建
    Tool: Bash
    Steps: 用合成doctor-caregiver-patient序列构造第4和第6 caregiver检查点；运行单元测试并检查每角色累计文本和计数。
    Expected: 第4检查点不含第5/6及其后话轮；角色文本分离；同一visit前缀保持同fold。
    Evidence: .sisyphus/evidence/task-5-prefixes.txt

  Scenario: 无caregiver或少于4个目标话轮
    Tool: Bash
    Steps: 分别构造仅doctor+patient和仅3个目标话轮的样本。
    Expected: 前者自动以patient为目标角色；后者标记insufficient并在决策层走ABSTAIN，不抛出未处理异常。
    Evidence: .sisyphus/evidence/task-5-prefixes-error.txt
  ```

  **Commit**: YES | Message: `Add role-conditioned prefix encoding` | Files: `analysis/emotion_model/patent_rcsea/{__init__,config,prefixes,features}.py`, `analysis/emotion_model/patent_rcsea/tests/test_rcsea.py`

- [ ] 6. 实现bootstrap风险区间、稳定性和三态停止控制器

  **What to do**:
  - 新增 `uncertainty.py` 和 `stopping.py`；实现Task 3冻结的公式和状态机。
  - 每个外层训练fold最终模型按patient_id进行B=200次bootstrap ensemble；内层阈值/ρ选择固定使用B=50以控制计算量。每次重采样患者并携带其全部visit/prefix，训练 `LogisticRegression(max_iter=2000,class_weight='balanced',random_state=seed+b)`。
  - 若某次bootstrap只有单类，最多重采样20次；每个fold至少获得180个有效模型，否则该fold停止并报告失败，不静默缩小ensemble。
  - 对每个prefix输出ensemble概率中位数 `r_t` 及2.5/97.5分位 `[L_t,U_t]`；`u_t=U_t-L_t`。
  - 根据相邻检查点计算 `s_t`，执行WARN/CONTINUE/ABSTAIN；模态mask选择阈值表，当前真实实验只估计text-only阈值，其它mask必须标为“未验证/拟研发”。
  - 证据片段只在内存QA中产生；通过leave-one-turn-block-out概率差计算贡献，返回绝对增量最大的3个turn索引和角色。聚合结果仅保留角色计数/贡献分布，不保留文本。

  **Must NOT do**:
  - 不使用外层测试fold选择任何阈值或参数。
  - 不将aggregate patient-bootstrap CI误用为单样本风险区间；预测级区间必须来自训练fold内ensemble。

  **Recommended Agent Profile**:
  - Category: `ultrabrain` — 原因：嵌套bootstrap、区间和状态机正确性要求高。
  - Skills: [] — 采用numpy/sklearn现有依赖。
  - Omitted: [`patent-disclosure-skill`] — 本任务是算法实现。

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: [7, 8] | Blocked By: [3, 5]

  **References**:
  - Pattern: `analysis/emotion_model/revision/metrics.py:bootstrap_patient_ci` — 患者重采样实现约束。
  - Pattern: `analysis/emotion_model/revision/metrics.py:compute_binary_metrics` — 指标边界和单类处理。
  - Pattern: `analysis/emotion_model/revision/experiment.py` — LogisticRegression配置和OOF row模式。
  - Specification: `D:\曦源\曦源临时\专利创新升级方案\working\RC-SEA技术规格.md` — 唯一公式与状态规则来源。

  **Acceptance Criteria**:
  - [ ] 相同seed、相同fold和输入产生完全相同ensemble分位区间和状态序列。
  - [ ] bootstrap采样单位是patient；同一患者所有prefix同时入样或不入样。
  - [ ] WARN/CONTINUE/ABSTAIN在所有测试组合上互斥且穷尽。
  - [ ] 高风险但高不确定性不得WARN；Tmax时必须ABSTAIN。

  **QA Scenarios**:
  ```
  Scenario: 稳定高风险触发预警
    Tool: Bash
    Steps: 输入连续检查点r=[0.72,0.74,0.75]、u=[0.12,0.10,0.09]、对应阈值θr=0.7,θu=0.15,θs=0.05。
    Expected: 最早满足t_min且稳定的检查点返回WARN；返回3个证据索引且不含原文落盘。
    Evidence: .sisyphus/evidence/task-6-stopping.txt

  Scenario: 单类bootstrap和高不确定性
    Tool: Bash
    Steps: 构造单类重采样及u>θu；检查重试计数和Tmax行为。
    Expected: 单类样本被重试；有效模型<180时明确失败；若模型有效但u高，Tmax返回ABSTAIN。
    Evidence: .sisyphus/evidence/task-6-stopping-error.txt
  ```

  **Commit**: YES | Message: `Add adaptive stopping uncertainty controller` | Files: `analysis/emotion_model/patent_rcsea/{uncertainty,stopping}.py`, `analysis/emotion_model/patent_rcsea/tests/test_rcsea.py`

- [ ] 7. 实现嵌套patient-grouped OOF运行器与隐私安全输出

  **What to do**:
  - 新增 `experiment.py`、`reporting.py` 和 `analysis/emotion_model/runners/run_patent_rcsea_oof.py`。
  - 外层fold严格复用固定5-fold patient map；每个外层训练集内部使用4-fold GroupKFold选择 `ρ,θ_r,θ_u,θ_s,t_min`。
  - 参数选择使用Task 3字典序；若无配置满足coverage/recall约束，选择Recall最高、再BACC最高、再消费比例最低的配置，并在summary中写 `constraint_fallback=true`。
  - 对外层测试visit按检查点回放，只在首次满足条件时停止；少于4个目标话轮或到15轮仍不满足时ABSTAIN。
  - 运行器CLI固定包含 `--liwc-dict`、`--output-root`、`--inner-bootstrap-models`（默认50）、`--bootstrap-models`（默认200）、`--seed`（默认2026）、`--smoke`。
  - 输出 `summary.json`、`summary.md`、`config.json`；患者级decision trace仅写到已被 `.gitignore` 排除的 `decision_traces.csv`，正式聚合报告不得包含patient_id、visit_id或原文。
  - 为运行器增加smoke test：inner B=10、outer B=20、小合成数据/首个外层fold，验证全链路而不冒充正式结果。

  **Must NOT do**:
  - 不改变既有revision实验、manifest或旧输出。
  - 不在summary中报告仅对非弃权样本计算的性能而不同时报告coverage和abstention。

  **Recommended Agent Profile**:
  - Category: `deep` — 原因：嵌套分组验证和隐私安全输出复杂。
  - Skills: [] — 复用项目实验框架。
  - Omitted: [`figure-designer`] — 无图形任务。

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: [8] | Blocked By: [5, 6]

  **References**:
  - Pattern: `analysis/emotion_model/runners/run_mind_echo_revision_main_v2_oof.py` — additive runner CLI和summary布局。
  - Pattern: `analysis/emotion_model/revision/manifest.py:ensure_manifest_files` — 固定fold载入。
  - Pattern: `analysis/emotion_model/revision/reporting.py` — Markdown/JSON写入风格。
  - Ignore policy: `.gitignore` — OOF/patient-level预测必须保持忽略。

  **Acceptance Criteria**:
  - [ ] smoke模式全链路退出码0；正式模式覆盖5个外层fold、78 visits、63 patients。
  - [ ] 每个测试patient只出现于一个外层fold；内层调参不访问外层测试label。
  - [ ] summary包含每折参数、覆盖率、弃权率、停止轮次分布和constraint_fallback标志。
  - [ ] tracked aggregate文件不含patient_id、visit_id或连续中文原始对话。

  **QA Scenarios**:
  ```
  Scenario: 嵌套OOF smoke运行
    Tool: Bash
    Steps: python analysis/emotion_model/runners/run_patent_rcsea_oof.py --smoke --inner-bootstrap-models 10 --bootstrap-models 20 --output-root <temp>；解析summary。
    Expected: 输出结构完整；fold/threshold/coverage字段齐全；无患者级标识。
    Evidence: .sisyphus/evidence/task-7-runner-smoke.txt

  Scenario: 外层测试泄漏或隐私字段出现
    Tool: Bash
    Steps: 执行泄漏断言；递归搜索tracked summary中的patient_id、visit_id和长中文文本字段。
    Expected: 检测到泄漏/隐私字段时退出非0并拒绝生成正式报告。
    Evidence: .sisyphus/evidence/task-7-runner-error.txt
  ```

  **Commit**: YES | Message: `Add patient-grouped RC-SEA experiment runner` | Files: `analysis/emotion_model/patent_rcsea/{experiment,reporting}.py`, `analysis/emotion_model/runners/run_patent_rcsea_oof.py`, tests, `.gitignore` if needed

- [ ] 8. 运行正式基线、消融和RC-SEA实验并生成证据报告

  **What to do**:
  - 运行正式5-fold nested patient-grouped实验，固定seed=2026、B=200；保存aggregate结果到 `analysis/emotion_model/experiments/patent_rcsea_oof/`。
  - 必跑对照：full text、固定4/6/8/10/12/15、validation-selected best fixed window、risk-threshold-only、role-agnostic accumulator、RC-SEA without stability、RC-SEA without uncertainty、完整文本型RC-SEA。
  - 模态mask/audio ablation在无队列音频时固定写 `not_evaluable_without_cohort_audio`，不得生成模拟性能数值；方案中保留未来实验协议。
  - 报告指标：BACC、F1、Recall、Specificity、ROC-AUC、PR-AUC、warning coverage、abstention rate、平均/中位停止检查点、平均对话消费比例、warning latency、false-early-warning rate；全部以patient-bootstrap 95% CI报告。
  - 指标口径固定：全78 visits中发生WARN视为positive prediction，未WARN（含Tmax ABSTAIN）视为negative prediction，计算主BACC/F1/Recall/Specificity；ROC-AUC/PR-AUC使用每个visit停止或Tmax时的最终 `r_t`；另行报告仅低不确定性样本上的selective metrics并始终并列coverage；`false-early-warning rate=FP/真实阴性visit数`；对话消费比例为停止检查点除以 `min(15,该visit可用目标角色话轮数)`，少于4轮且ABSTAIN记为1.0。
  - 比较采用配对patient bootstrap；若CI重叠或差异不显著，明确写“未证明优于”，转而报告earliness-risk tradeoff。
  - 生成 `RC-SEA实验验证报告.md`，包含结果表、消融表、Pareto表、证据支持结论、尚未验证项；不写患者级明细。

  **Must NOT do**:
  - 不选择最好看的seed、fold或阈值；不在正式结果后改参数网格。
  - 不把弃权样本删除后只报高分；coverage和abstention必须与选择性性能并列。

  **Recommended Agent Profile**:
  - Category: `unspecified-high` — 原因：长时实验执行、结果一致性和统计核查。
  - Skills: [] — 运行现有Python实验。
  - Omitted: [`patent-disclosure-skill`] — 专利解释在后续文档任务完成。

  **Parallelization**: Can Parallel: NO | Wave 2 | Blocks: [9, 10, 11, 12] | Blocked By: [7]

  **References**:
  - Baseline data: `analysis/emotion_model/experiments/mind_echo_revision_minimal/main_v2_oof/summary.json`。
  - Window baseline: `analysis/emotion_model/experiments/mind_echo_revision_minimal/window_oof/summary.json`。
  - Reporting pattern: `analysis/emotion_model/experiments/mind_echo_revision_minimal/README.md`。
  - Specification: `working\RC-SEA技术规格.md`。

  **Acceptance Criteria**:
  - [ ] 正式summary验证78 visits、63 patients、5 outer folds、200 bootstrap models配置。
  - [ ] 所有必跑基线和消融均有结果或明确不可评估原因；audio不得有伪结果。
  - [ ] 每项选择性性能旁有coverage/abstention；每项核心指标有95% CI。
  - [ ] 报告结论与CI一致，不使用“显著”“最优”除非统计证据满足预定义规则。

  **QA Scenarios**:
  ```
  Scenario: 正式实验与报告一致
    Tool: Bash
    Steps: 运行正式runner；独立解析summary重算样本数、混淆矩阵、coverage、停止点分布；比对Markdown表。
    Expected: JSON和Markdown逐项一致；所有visit被预测或弃权且总数为78。
    Evidence: .sisyphus/evidence/task-8-results-validation.json

  Scenario: RC-SEA未优于固定窗口
    Tool: Bash
    Steps: 构造/识别CI重叠或BACC较低情形；检查报告措辞。
    Expected: 报告写“未证明性能优于”，仅陈述实际earliness/coverage权衡；不自动改阈值追分。
    Evidence: .sisyphus/evidence/task-8-results-edge.md
  ```

  **Commit**: YES | Message: `Add RC-SEA validation results` | Files: aggregate `summary.json`, `summary.md`, `config.json`, repo experiment README; patient traces excluded

- [ ] 9. 撰写创新升级方案正文与实验解释

  **What to do**:
  - 生成 `working\创新升级方案正文.md`，固定包含以下一级章节：
    1. `一、现有第三版方案的创新性诊断`
    2. `二、现有技术与审查风险对比`
    3. `三、推荐升级方向：角色条件序贯证据累积与闭环自适应停止`
    4. `四、核心技术方案`
    5. `五、公式、参数与判定规则`
    6. `六、权利要求骨架`（保留整合标记，由Task 10提供内容）
    7. `七、实施支撑与拟研发内容矩阵`
    8. `八、实验验证计划与结果`
    9. `九、附图设计说明`
    10. `十、风险与边界说明`
  - 第二章必须引用差异矩阵中的已核验项目，并解释为何角色LSTM、GNN/超图、音频文本forecasting、adaptive window、uncertainty ERC、Conformal calibration均不能单独构成本案创新。
  - 第三至五章按Task 3规格完整描述输入、角色状态、模态mask、累积、区间、稳定性、三态输出、证据片段和计算受限实现；给出符号表及公式。
  - 第七章嵌入证据状态矩阵，严格区分已实现、第三版支持未验证、拟研发、删除/从属。
  - 第八章只使用Task 8真实输出；若RC-SEA无显著性能优势，围绕earliness、coverage、abstention和错误预警控制解释，不虚构“准确率显著提升”。
  - 统一替换“传统模型”为具体名称；Qwen/LoRA仅在可选从属实施例段落出现，音频分支明确为可选/待队列验证。

  **Must NOT do**:
  - 不写成完整第四版说明书，不复制第三版全文。
  - 不使用“诊断抑郁症、确定患病、替代医生、治疗决策”等表述。

  **Recommended Agent Profile**:
  - Category: `writing` — 原因：需要专利代理人可用的中文技术方案表达。
  - Skills: [`patent-disclosure-skill`] — 需要专利术语、公式和现有技术区别规范。
  - Omitted: [`figure-designer`] — 图示在Task 11。

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: [12] | Blocked By: [1, 2, 3, 4, 8]

  **References**:
  - Baseline audit: `working\第三版创新性审阅.md`。
  - Specification: `working\RC-SEA技术规格.md`。
  - Prior art: `D:\曦源\曦源临时\专利创新升级方案\现有技术差异矩阵.csv`。
  - Evidence matrix: `working\证据与研发状态矩阵.csv`。
  - Results: `D:\曦源\曦源临时\专利创新升级方案\RC-SEA实验验证报告.md`。
  - Current evidence: `analysis/emotion_model/experiments/patent_rcsea_oof/summary.json`。

  **Acceptance Criteria**:
  - [ ] 十个规定一级章节全部存在且顺序正确。
  - [ ] 公式中每个符号均出现在符号表，且与实验配置同名同义。
  - [ ] 所有结果数值可追溯到Task 8 JSON；不存在未标注的假设数值。
  - [ ] 禁用医疗措辞、固定窗口最优、已验证LoRA/音频融合等搜索结果为0。

  **QA Scenarios**:
  ```
  Scenario: 正文结构与证据一致
    Tool: Python Markdown validator
    Steps: 解析一级标题顺序；提取数字和模型名；与summary及矩阵交叉核对；检查符号表。
    Expected: 十章完整；真实数值100%可追溯；独立核心不依赖LIWC/TF-IDF/LoRA。
    Evidence: .sisyphus/evidence/task-9-proposal-validation.json

  Scenario: 出现过度主张或禁用医疗措辞
    Tool: Grep
    Steps: 搜索“诊断为|替代医生|固定窗口最优|LoRA已完成|音频融合显著提升|临床有效”。
    Expected: 结果为0；若引用第三版原问题，必须位于明确的“问题/不得主张”上下文并被校验器允许。
    Evidence: .sisyphus/evidence/task-9-proposal-error.txt
  ```

  **Commit**: NO | Files: `D:\曦源\曦源临时\专利创新升级方案\working\创新升级方案正文.md`

- [ ] 10. 编制方法/系统/存储介质权利要求骨架并完成追溯

  **What to do**:
  - 输出 `working\权利要求骨架.md`，包含：1项方法独立权利要求、8–10项方法从属要点、1项系统独立权利要求、1项存储介质/程序产品要点。
  - 方法独立项必须按技术链书写：获取并角色标注主诉期话轮 → 基于角色和模态mask编码 → 更新序贯证据状态 → 计算风险/区间/稳定性 → 执行WARN/CONTINUE/ABSTAIN → 输出非诊断风险区间和触发证据片段。
  - 系统独立项逐一镜像为：输入/角色标注模块、角色条件编码模块、模态状态模块、序贯累积模块、可靠性评估模块、闭环停止控制模块、辅助输出模块。
  - 从属要点固定覆盖：TF-IDF+SVD/LIWC/domain实现、具体LR/SVM/RF/GB/Ridge基线、可选声学特征、预测级bootstrap ensemble或其它校准实现、稳定性窗口、阈值选择、证据片段、计算受限降级、患者级训练验证、可选LLM编码器。
  - 更新 `权利要求追溯矩阵.csv`，保证每个独立项要素链接到正文章节、公式、SVG node id和证据状态。
  - 在骨架开头写明“供代理人进一步法务定稿，不构成最终权利要求书”。

  **Must NOT do**:
  - 不在独立项限定Qwen、LoRA、LIWC、TF-IDF、Conformal、GNN或Transformer。
  - 不把已知算法名称本身描述成创造性效果。

  **Recommended Agent Profile**:
  - Category: `writing` — 原因：权利要求层级和技术要素映射要求高。
  - Skills: [`patent-disclosure-skill`] — 需要专利权利要求表达与支持性检查。
  - Omitted: [`figure-designer`] — 只消费预定义node id。

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: [12] | Blocked By: [1, 2, 3, 4, 8]

  **References**:
  - Specification: `working\RC-SEA技术规格.md`。
  - Traceability: `D:\曦源\曦源临时\专利创新升级方案\权利要求追溯矩阵.csv`。
  - Third-version issues: `working\第三版创新性审阅.md`。
  - Evidence: `working\证据与研发状态矩阵.csv`。

  **Acceptance Criteria**:
  - [ ] 方法独立项包含规定6段技术链且每段有输入/处理/输出。
  - [ ] 系统模块与方法步骤一一对应，无孤立模块。
  - [ ] 每个独立要素在追溯矩阵有非空章节、公式/规则、SVG节点、证据状态。
  - [ ] 独立项不包含被禁止作为核心的具体成熟算法名。

  **QA Scenarios**:
  ```
  Scenario: 权利要求骨架完整追溯
    Tool: Python Markdown/CSV validator
    Steps: 提取独立项要素；比对系统模块和追溯矩阵；检查必需技术动作。
    Expected: 方法/系统镜像完整；8类独立要素100%可追溯。
    Evidence: .sisyphus/evidence/task-10-claims-validation.json

  Scenario: 成熟算法误入独立项
    Tool: Grep
    Steps: 仅在独立项范围搜索“LIWC|TF-IDF|Qwen|LoRA|Conformal|GNN|Transformer”。
    Expected: 搜索结果为0；这些词仅允许在从属要点/实施例出现。
    Evidence: .sisyphus/evidence/task-10-claims-error.txt
  ```

  **Commit**: NO | Files: `working\权利要求骨架.md`, 更新后的 `权利要求追溯矩阵.csv`

- [ ] 11. 按参考风格绘制中文纯矢量技术方案SVG

  **What to do**:
  - 新建 `主诉期闭环自适应停止_技术方案图.svg`，根节点固定 `width="1600" height="1740" viewBox="0 0 1600 1740"`，纯白背景。
  - 严格复用参考图视觉语言：顶部标题/来源；黑色三列表头；左列220px纵向阶段带；中列640px主流程；右列365px注释；蓝色数据/编码阶段 `#3d78a8/#78add0/#d9edf9`；绿色控制/输出阶段 `#5c9a69/#79b987/#dff1df`；黑色箭头和描边。
  - 左侧阶段固定为：`门诊主诉输入`、`角色证据编码`、`序贯累积控制`、`辅助输出`。
  - 中央节点及id固定为：
    - `node-role-stream` 三方话轮流；
    - `node-role-segmentation` 角色标注与检查点；
    - `node-role-encoding` 角色条件文本编码；
    - `node-optional-audio` 可选声学编码（虚线）；
    - `node-modality-mask` 模态存在掩码；
    - `node-rcsea` RC-SEA序贯证据累积（粗描边核心）；
    - `node-reliability` 风险区间/不确定性/稳定性；
    - `decision-stop` 联合判定；
    - `output-warn`、`output-continue`、`output-abstain` 三态；
    - `output-clinician` 非诊断风险区间与触发证据片段。
  - 右侧注释固定为：`创新不在LIWC/TF-IDF本身`、`缺失语音通过模态掩码处理`、`患者级验证避免泄漏`、`输出辅助提示而非诊断`。
  - 增加图例：实线=现有/文本型验证；粗实线=拟保护RC-SEA核心；虚线=待验证可选音频/多模态。拟研发节点不得伪装成已实现。
  - 底注固定：“本图描述门诊主诉阶段的非诊断性情绪风险沟通辅助流程，不代表心理诊断、治疗建议或临床确诊结论。”

  **Must NOT do**:
  - 不使用嵌入PNG/JPG、3D效果、阴影或颜色作为唯一编码。
  - 不把Qwen/LoRA、固定6–8轮、GNN/Transformer放在核心流程。

  **Recommended Agent Profile**:
  - Category: `visual-engineering` — 原因：需要高质量SVG布局、样式和可读性。
  - Skills: [`figure-designer`] — 需要方案总览图设计和质量门禁。
  - Omitted: [`patent-disclosure-skill`] — 技术内容已由规格和追溯矩阵冻结。

  **Parallelization**: Can Parallel: YES | Wave 3 | Blocks: [12] | Blocked By: [3, 8]

  **References**:
  - Visual pattern: `D:\Obsidian Vault\research_flowchart 1.svg` — 精确画布、CSS、颜色、三栏、箭头和字号参考。
  - Content spec: `working\RC-SEA技术规格.md`。
  - Traceability: `D:\曦源\曦源临时\专利创新升级方案\权利要求追溯矩阵.csv` — SVG node id契约。
  - Figure standard: `C:\Users\Administrator\.claude\skills\figure-designer\references\design-rules.md`。

  **Acceptance Criteria**:
  - [ ] SVG XML可解析，viewBox精确，且不存在`<image>`元素或base64栅格。
  - [ ] 所有固定node id和中文标签存在且唯一。
  - [ ] 实线/粗实线/虚线图例完整；可选音频为虚线。
  - [ ] 渲染后无文字越界、遮挡、箭头穿过文本或小于18px正文。

  **QA Scenarios**:
  ```
  Scenario: 正常渲染与结构检查
    Tool: Python XML parser + Playwright/browser SVG render
    Steps: 解析根属性和node ids；确认无image；浏览器打开SVG并截取1600x1740截图；检查文本框边界和箭头。
    Expected: XML有效、节点齐全、截图无裁切/重叠、文字清晰。
    Evidence: .sisyphus/evidence/task-11-svg-render.png

  Scenario: 缺音频与文本型降级路径
    Tool: SVG structural checker
    Steps: 从node-optional-audio追踪虚线到node-modality-mask；从mask追踪到RC-SEA；检查不存在强制音频依赖。
    Expected: 即使不走音频分支，文本编码仍可到达三态输出；音频明确标注待验证。
    Evidence: .sisyphus/evidence/task-11-svg-edge.json
  ```

  **Commit**: NO | Files: `D:\曦源\曦源临时\专利创新升级方案\主诉期闭环自适应停止_技术方案图.svg`

- [ ] 12. 组装最终方案包并执行非覆盖、术语、证据和图文一致性验证

  **What to do**:
  - 将Task 9正文和Task 10权利要求骨架整合为最终 `主诉期闭环自适应停止_创新升级方案.md`；删除整合标记，保持十章结构。
  - 将实验报告、查新矩阵、追溯矩阵、SVG及最终方案放在 `D:\曦源\曦源临时\专利创新升级方案\`；working资料保留在子目录，不混入最终文件清单。
  - 全包术语统一：`角色条件序贯证据累积器（RC-SEA）`、`闭环自适应停止`、`模态存在掩码`、`风险区间`、`稳定性度量`、`预警/继续采集/暂缓输出`。
  - 执行四向一致性：方案公式 ↔ 实验配置 ↔ 权利要求追溯矩阵 ↔ SVG node ids。
  - 执行第三版DOCX最终SHA-256比对；原文件hash、大小和修改时间均不得变化。
  - 输出 `working\交付验证报告.json`，记录所有文件hash、章节、禁用词、矩阵完整度、SVG检查、实验摘要引用和原DOCX非覆盖结果。

  **Must NOT do**:
  - 不将working转换文件误当最终专利稿，不生成或命名“第四版.docx”。
  - 不因SVG或方案验证失败而覆盖第三版或删除旧文件。

  **Recommended Agent Profile**:
  - Category: `unspecified-high` — 原因：跨文档、实验、SVG和原文件完整性核验。
  - Skills: [`patent-disclosure-skill`, `figure-designer`] — 需要专利一致性和图形质量联合检查。
  - Omitted: [] — 两项技能均直接相关。

  **Parallelization**: Can Parallel: NO | Wave 3 | Blocks: [Final Verification] | Blocked By: [9, 10, 11]

  **References**:
  - Final text source: `working\创新升级方案正文.md`。
  - Claims source: `working\权利要求骨架.md`。
  - Experiment evidence: `RC-SEA实验验证报告.md`, `analysis/emotion_model/experiments/patent_rcsea_oof/summary.json`。
  - Figure: `主诉期闭环自适应停止_技术方案图.svg`。
  - Original integrity target: third-version DOCX path from Task 1。

  **Acceptance Criteria**:
  - [ ] 五个最终交付文件存在且非空；最终方案十章完整，无整合标记。
  - [ ] 原DOCX SHA-256、大小、修改时间与Task 1 before记录一致。
  - [ ] 追溯矩阵独立项行无空值；所有svg_node在SVG中存在。
  - [ ] 方案数值与aggregate summary一致；音频/LoRA/online等未验证项均明确标记。
  - [ ] SVG XML/视觉检查通过；禁用医疗和过度主张词检查通过。

  **QA Scenarios**:
  ```
  Scenario: 完整方案包交付
    Tool: PowerShell + Python validators + Playwright/browser
    Steps: 枚举最终文件；校验hash；解析Markdown/CSV/JSON/SVG；渲染SVG；比对术语、公式、节点和实验数值。
    Expected: 所有自动检查PASS；交付验证报告中failures=[]；原DOCX完整未变。
    Evidence: .sisyphus/evidence/task-12-package-validation.json

  Scenario: 任一文件不一致或原DOCX变化
    Tool: PowerShell + Python validators
    Steps: 模拟缺失SVG节点/空矩阵字段/禁用词/原DOCX hash不符之一。
    Expected: 验证器非0退出，报告具体文件与字段；禁止进入Final Verification或宣称完成。
    Evidence: .sisyphus/evidence/task-12-package-error.json
  ```

  **Commit**: NO | Files: `D:\曦源\曦源临时\专利创新升级方案\*`

## Final Verification Wave (MANDATORY — after ALL implementation tasks)
> 4 review agents run in PARALLEL. ALL must APPROVE. Present consolidated results to user and get explicit "okay" before completing.
> **Do NOT auto-proceed after verification. Wait for user's explicit approval before marking work complete.**
> **Never mark F1-F4 as checked before getting user's okay.** Rejection or user feedback -> fix -> re-run -> present again -> wait for okay.
- [ ] F1. Plan Compliance Audit — oracle
- [ ] F2. Code Quality Review — unspecified-high
- [ ] F3. Real Manual QA — unspecified-high (+ browser/SVG renderer)
- [ ] F4. Scope Fidelity Check — deep

## Commit Strategy
- 仅仓库内新增RC-SEA代码、测试、运行器和聚合结果进入Git；患者级预测和manifest明细继续忽略。
- 建议提交顺序：数据/状态模型 → 停止与不确定性 → OOF运行器与测试 → 聚合实验结果。
- `D:\曦源\曦源临时\专利创新升级方案\` 下的专利方案与SVG不属于仓库提交，保持独立交付。
- 不执行push或创建PR，除非用户另行明确要求。

## Success Criteria
- 创造性论述可明确回答：创新不在特征或分类器，而在多角色证据、模态可用性、稳定性和不确定性共同控制采集/预警状态转换。
- 每个独立权利要求要素都有说明书段落、公式/规则、SVG节点和证据状态。
- 实验不再比较“哪一个固定窗口一定最好”，而是比较自适应停止在性能、时效和弃权覆盖之间的权衡。
- 音频缺失、无caregiver、极短对话、角色冲突、高风险高不确定性、到达最大轮次仍不稳定等边界均有定义。
- 方案、矩阵、实验报告和SVG之间术语/符号/状态一致，且无诊断性或未经验证的效果表述。
