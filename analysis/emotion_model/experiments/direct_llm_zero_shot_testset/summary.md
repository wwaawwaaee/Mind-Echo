# Direct current-model zero-shot on with_caregiver test set

This is a single-pass direct LLM judgment on the held-out test set, not a trained classifier.

## Anxiety

| ACC | F1 | Precision | Recall | Specificity | Balanced ACC | Confusion Matrix |
|---:|---:|---:|---:|---:|---:|---|
| 0.833 | 0.833 | 0.833 | 0.833 | 0.833 | 0.833 | [[5, 1], [1, 5]] |

## Depression

| ACC | F1 | Precision | Recall | Specificity | Balanced ACC | Confusion Matrix |
|---:|---:|---:|---:|---:|---:|---|
| 0.667 | 0.750 | 1.000 | 0.600 | 1.000 | 0.800 | [[2, 0], [4, 6]] |
