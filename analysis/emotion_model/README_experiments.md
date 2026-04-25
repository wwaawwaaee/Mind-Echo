# emotion_model Experiment Layout

## Structure

```text
emotion_model/
├─ core/          # shared pipeline code
├─ runners/       # experiment entry scripts
├─ outputs/       # default pipeline outputs
├─ experiments/   # named experiment results
└─ temp/          # temporary / disposable artifacts
```

## Core
- `core/01_prepare_data.py`: build visit-level datasets
- `core/02_train_model.py`: train/evaluate baseline models
- `core/03_evaluate.py`: default result visualization
- `core/domain_rule_features.py`: handcrafted rule features

## Runners
- `runners/run_continuous_window_comparison.py`
- `runners/run_with_caregiver_textfield_comparison.py`
- `runners/run_with_caregiver_label_comparison.py`
- `runners/run_with_caregiver_feature_mode_comparison.py`
- `runners/run_with_caregiver_feature_mode_insample.py`
- `runners/run_with_caregiver_feature_v2_cv.py`

## Default outputs
- `outputs/default/data/`
- `outputs/default/models/`
- `outputs/default/results/`

## Experiment outputs
- `experiments/window_comparison/`
- `experiments/with_caregiver_label_comparison/`
- `experiments/with_caregiver_textfield_comparison/`
- `experiments/with_caregiver_feature_mode_comparison/`
- `experiments/with_caregiver_feature_mode_insample/`
- `experiments/with_caregiver_feature_v2_cv/`

## Temp
- `temp/` stores smoke-test or disposable intermediate artifacts.
