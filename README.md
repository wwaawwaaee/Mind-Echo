# Mind-Echo
[简体中文](README_CN.md) | [English](README.md)

## Research-only status and scope warning

> **Research use only. Not for clinical use, diagnosis, triage, or deployment.**
> Mind-Echo is a **single-center, exploratory, proof-of-concept** repository built from **text-based outpatient dialogue records**. The current repository does **not** establish a clinically validated product, real-time deployed system, external validation result, or validated cohort-level audio or multimodal model.

## Project overview

Mind-Echo studies whether structured text features from outpatient doctor-patient-caregiver dialogues are associated with questionnaire-based anxiety and depression risk labels in a limited retrospective dataset. The repository contains data-processing scripts, descriptive statistics, LIWC-style text analysis, baseline and revision machine-learning experiments, and manuscript-oriented vector figure generation.

The most defensible framing for the current repository is a **single-center exploratory proof-of-concept study on text-based outpatient dialogues**. It should be read as a methods and reproducibility repository, not as evidence of clinical readiness.

## Verified data snapshot

Verified from `basic_status_summary/patient_basic_stats_summary.json` and the additive revision experiment metadata.

| Item | Verified count | Unit | Notes |
| --- | ---: | --- | --- |
| Dialogue source files | 84 | files | One source file per patient record in the current processed snapshot. |
| Patient records | 84 | records | Do not treat these as unique caregivers. |
| Visit segments | 131 | visit segments | Visit-level dialogue segments across the 84 patient records. |
| Paired GAD-7/PHQ-9 scale records | 109 | paired scale records | These are paired scale entries, not independent caregiver identities. |
| Revision ML cohort | 78 | visit records | Used by the additive revision OOF experiments. |
| Revision ML cohort | 63 | patients | Grouped by `patient_id` only. |
| Total dialogue turns | 5,531 | turns | Useful descriptively, but turns are **not** independent samples. |
| Role turn distribution | doctor 2,824, caregiver 1,889, patient 818 | turns | Derived from processed dialogue roles. |

## Repository contents and features

- `processed_dataset/`: scripts for dialogue building, speaker labeling, anonymization, and caregiver-based splitting.
- `basic_status_summary/`: patient-level counts, dialogue-turn summaries, and descriptive markdown/plots.
- `analysis/`: LIWC-style linguistic analysis, correlations, summary reports, and figure generation.
- `analysis/emotion_model/`: baseline experiment code, additive revision experiments, and runner scripts.
- `analysis/paper_figures/english_vector_pipeline/`: manuscript-oriented vector figure pipeline and additive revision figure exports.
- `raw_data/`, `patient's_ocr/`, `ocr_results/`: local raw or intermediate workspaces. Sensitive source material and ignored intermediate outputs are not part of the reproducible Git-tracked release.

## Project structure

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

## Installation and setup

From the repository root:

```bash
pip install -r requirements.txt
```

The root requirements cover the shared data-processing, statistical, plotting, tokenization, and machine-learning dependencies used by the documented workflows. The external LIWC dictionary described below is still required for LIWC-based experiments.

### External LIWC dictionary requirement

The additive revision runners default to an **external** LIWC dictionary path:

`D:\vscode-project\git-project\Auto_CLIWC\Auto_CLIWC\datasets\sc_liwc.dic`

That path is defined in `analysis/emotion_model/revision/paths.py` and is **not bundled in this repository**. If your local setup differs, pass your own dictionary path through the runner CLI or update local configuration accordingly.

## Reproduction commands

Run from the repository root `D:\vscode-project\git-project\Mind-Echo` unless noted otherwise.

### Revision utility tests

```bash
python analysis/emotion_model/revision/tests/test_revision_utils.py
```

### Additive revision OOF experiments

```bash
python analysis/emotion_model/runners/run_mind_echo_revision_main_v2_oof.py
python analysis/emotion_model/runners/run_mind_echo_revision_window_oof.py
```

### Vector paper figures

```bash
python analysis/paper_figures/english_vector_pipeline/generate_academic_figures.py --figure all
python analysis/paper_figures/english_vector_pipeline/build_revision_figures.py
```

Equivalent command from inside `analysis/paper_figures/english_vector_pipeline/`:

```bash
python build_revision_figures.py
```

## Revision experiment methodology

The additive revision pipeline under `analysis/emotion_model/experiments/mind_echo_revision_minimal/` is the cleanest reproducible ML protocol currently documented in this repository.

- Cohort: **78 visit records from 63 patients**.
- Grouping: **five-fold `GroupKFold` by `patient_id`**.
- Leakage control: cross-validation is **patient-grouped only**.
- Fold-local preprocessing: TF-IDF, SVD, and scaling are fit on each training fold only.
- Aggregation: results are reported from **pooled out-of-fold predictions**.
- Baselines: a fold-wise **majority baseline** and a **seeded prevalence-random baseline**.
- Metrics: **BACC, F1, recall, specificity, ROC-AUC, and PR-AUC**.
- Uncertainty: **2,000 patient-bootstrap draws** for percentile **95% confidence intervals**.
- Window study: windows `4`, `6`, `8`, `10`, `12`, `15`, and `full_text` are evaluated with a fixed feature mode for comparison.

## Key outputs and where they live

### Descriptive and analysis outputs

- `basic_status_summary/patient_basic_stats_summary.json`: most reliable root-level count snapshot.
- `analysis/results/summary_latest.json`: source summary reused by the English vector figure pipeline.

### Additive revision experiment outputs

- `analysis/emotion_model/experiments/mind_echo_revision_minimal/manifest/`: manifest and patient-fold maps.
- `analysis/emotion_model/experiments/mind_echo_revision_minimal/main_v2_oof/summary.json`: main revision OOF results.
- `analysis/emotion_model/experiments/mind_echo_revision_minimal/window_oof/summary.json`: context-window OOF results.

### Figure outputs

- `analysis/paper_figures/english_vector_pipeline/outputs/`: general academic figure exports.
- `analysis/paper_figures/english_vector_pipeline/outputs_revision_guide/`: additive revision-guide figure outputs.

For the additive revision figure guide:

- **Figure 1 revision is text/table only**, stored as `figure1_1.md` and `figure1_2.csv`.
- **Figure 3 outputs** are numbered `figure3_1`, `figure3_2`, and `figure3_3`.
- **Figure 4 outputs** are numbered `figure4_1`, `figure4_2`, and `figure4_3`.
- **SVG is the canonical tracked vector format**. PNG and PDF previews can be regenerated locally when needed.

## Current evidence and conservative interpretation

Current repository evidence supports a limited claim: text-derived features from outpatient dialogue transcripts can be organized into exploratory statistical and machine-learning analyses on this single-center dataset, and patient-grouped revision experiments can be reproduced with explicit uncertainty estimates.

It does **not** support stronger claims about clinical effectiveness, deployment readiness, causal effects of clinician speech, generalizable prevalence, multimodal superiority, or real-time psychiatric monitoring. Likewise, the current window analyses do **not** prove that **6 to 8 turns** are an optimal context size.

## Limitations

- Single center.
- Small dataset.
- No external validation cohort.
- Text-based analysis only for the validated cohort-level revision pipeline.
- No validated cohort-level audio or multimodal results in the current repository evidence.
- Caregiver presence is **role-derived** from dialogue turns.
- There is **no structured `caregiver_id` or `family_id`** field.
- Cross-validation claims are **patient-grouped only**.
- Scale respondent identity cannot be independently verified from the current processed records.
- The 5,531 dialogue turns should not be interpreted as 5,531 independent samples.
- Scale threshold positivity should not be interpreted as a clinical diagnosis or a population prevalence estimate.
- No clinical deployment claim is made.

## Data governance and ethics

This repository should be treated as a **research artifact containing de-identified processed materials and analysis code**, not as a statement that all underlying data are openly reusable without restriction. The repository does not establish unrestricted public data release terms for the full source dataset.

Users should review local institutional, legal, and ethical requirements before reusing any medical-text material, even in de-identified form. The current documentation supports only a research-use framing and does not claim clinical approval, clinical workflow integration, or independently verified respondent identity for the scale records.

## Citation

Citation information will be added here when a citable manuscript or record is available.

## License

This repository includes a `LICENSE` file with the **Apache License 2.0**.
