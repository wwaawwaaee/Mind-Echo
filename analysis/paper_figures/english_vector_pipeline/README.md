# Mind-Echo English Vector Academic Figure Pipeline

This directory contains a new, isolated figure-generation pipeline for English academic vector figures. It does **not** modify or delete any legacy PNGs or scripts under `analysis/results`, `basic_status_summary`, or `analysis/emotion_model`.

## Files

- `figure_data.py` loads primary JSON sources, recomputes cohort counts, clinical thresholds, Pearson correlations, the operational absolutist-expression feature, and strict experiment-summary datasets for Figures 3-5.
- `plot_style.py` defines vector-friendly Matplotlib settings: `svg.fonttype='none'`, `pdf.fonttype=42`, `ps.fonttype=42`, English fonts, and shared export helpers.
- `generate_academic_figures.py` is the CLI entry point.
- `outputs/` stores generated SVG/PDF/PNG previews and validation JSON files.

## Primary inputs

The pipeline uses these files as source data:

1. `processed_dataset/output/anonymized_dataset_with_caregiver.json`
2. `processed_dataset/output/anonymized_dataset_without_caregiver.json`
3. `basic_status_summary/patient_basic_stats_summary.json`
4. `analysis/results/summary_latest.json`
5. `analysis/emotion_model/core/domain_rule_features.py`
6. `analysis/emotion_model/experiments/with_caregiver_feature_v2_cv/summary.json`
7. `analysis/emotion_model/experiments/window_comparison/window_comparison_summary.json`
8. `analysis/emotion_model/experiments/direct_llm_zero_shot_testset/summary.json`

## Count reconciliation

The generation script includes internal assertions for the required counts:

- Role turns are recomputed from both anonymized datasets and must total **5,531** (`doctor=2,824`, `caregiver=1,889`, `patient=818`).
- Valid pediatric ages are integer ages from 0 to 17 across the primary anonymized datasets and must total **21** (`0-6`, `7-12`, `13-17`).
- GAD-7/PHQ-9 records are paired **scale records**, not caregiver-only patients, and must total **109**.

Figure 2 uses patient-level with-caregiver raw arrays from `summary_latest.json` for LIWC correlations. `summary_latest.json` does not contain an absolutist-expression raw feature, so `absolute_density` is operationalized from the existing `domain_rule_features.py` absolutist patterns over with-caregiver caregiver-role dialogue text and recorded in `figure2_validation.json`.

Child-compliance words are not used because no documented child-compliance extractor or lexicon exists in the current analysis pipeline. The third PHQ-9 scatter feature is selected from the strongest available Pearson p-value among the remaining documented LIWC features `posemo` and `tentat`, after reserving `sad` and `certain` as required panels.

Figures 3-5 are generated from existing experiment summaries only:

- Figure 3 uses grouped bars for V2 with-caregiver feature modes and targets, showing F1 and balanced accuracy with source cross-validation standard deviations as error bars.
- Figure 4 uses the combined available-window series from `window_comparison_summary.json`. The file contains `acc` and `f1`, not `balanced_acc`, so ACC is labeled as ACC and is not renamed to balanced accuracy. The current summary does not contain separate anxiety/depression window classification trajectories.
- Figure 5 compares Best ML against direct zero-shot LLM. Best ML anxiety is `hybrid_v2`; Best ML depression is `liwc_only`; direct LLM values come from the 12-sample direct LLM test-set summary.
- Reports explicitly record unsupported requirements: no documented child-compliance extractor/lexicon, no separate Anxiety/Depression window trajectories, no window `balanced_acc`, and no cohort-level audio-data or multimodal CV results in the current sources.

## Commands

Run from this directory to generate the combined figures and all split-panel outputs:

```powershell
python generate_academic_figures.py --figure all
```

Generate one figure family only. Each command keeps the combined figure and also writes that figure family's split panels:

```powershell
python generate_academic_figures.py --figure 1
python generate_academic_figures.py --figure 2
python generate_academic_figures.py --figure 3
python generate_academic_figures.py --figure 4
python generate_academic_figures.py --figure 5
```

Optional custom output directory:

```powershell
python generate_academic_figures.py --figure all --output-dir outputs
```

## Expected outputs

`outputs/` should contain:

- `figure1.svg`
- `figure1.pdf`
- `figure1.png`
- `figure1_validation.json`
- `figure1a_role_turn_distribution.svg`
- `figure1a_role_turn_distribution.pdf`
- `figure1a_role_turn_distribution.png`
- `figure1b_age_distribution.svg`
- `figure1b_age_distribution.pdf`
- `figure1b_age_distribution.png`
- `figure1c_severity_distribution.svg`
- `figure1c_severity_distribution.pdf`
- `figure1c_severity_distribution.png`
- `figure1d_gad_phq_correlation.svg`
- `figure1d_gad_phq_correlation.pdf`
- `figure1d_gad_phq_correlation.png`
- `figure1e_clinical_risk.svg`
- `figure1e_clinical_risk.pdf`
- `figure1e_clinical_risk.png`
- `figure2.svg`
- `figure2.pdf`
- `figure2.png`
- `figure2_validation.json`
- `figure2a_liwc_heatmap.svg`
- `figure2a_liwc_heatmap.pdf`
- `figure2a_liwc_heatmap.png`
- `figure2b_gad7_anxiety_features.svg`
- `figure2b_gad7_anxiety_features.pdf`
- `figure2b_gad7_anxiety_features.png`
- `figure2c_phq9_depression_features.svg`
- `figure2c_phq9_depression_features.pdf`
- `figure2c_phq9_depression_features.png`
- `figure3_feature_set_f1_bacc.svg`
- `figure3_feature_set_f1_bacc.pdf`
- `figure3_feature_set_f1_bacc.png`
- `figure4_available_window_curve_combined.svg`
- `figure4_available_window_curve_combined.pdf`
- `figure4_available_window_curve_combined.png`
- `figure5_best_ml_vs_zero_shot.svg`
- `figure5_best_ml_vs_zero_shot.pdf`
- `figure5_best_ml_vs_zero_shot.png`
- `figures2_answer_report.md`
- `figures2_answer_report.csv`
- `section_3_2_correlations.csv`
- `table1_feature_set_performance.csv`
- `table1_feature_set_performance.md`
- `figure4_window_data_availability.md`
- `figures2_validation.json`

The PNG files are 300-dpi previews. The SVG/PDF files are intended for manuscript editing and preserve vector text where supported by Matplotlib. `figure1_validation.json` and `figure2_validation.json` include `split_panel_files`; `figure2_validation.json` also records that the Figure 2a heatmap uses LIWC features on the y-axis and GAD-7/PHQ-9 clinical scales on the x-axis. `figures2_validation.json` records the Figure 3-5 sources, generated report paths, unsupported requirements, and the rule that Figure 4 ACC is not balanced accuracy.
