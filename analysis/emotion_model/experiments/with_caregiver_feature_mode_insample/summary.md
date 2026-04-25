# With-caregiver Feature Mode Comparison (In-sample)

> Warning: train/validation/test are identical in this exploratory run. Metrics are optimistic and do not estimate generalization.

| Feature Mode | ACC | F1 | Precision | Recall | Specificity | Balanced ACC | GAD r | PHQ r | Confusion Matrix |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| text_only | 0.987 | 0.990 | 0.980 | 1.000 | 0.964 | 0.982 | 0.896 | 0.938 | [[27, 1], [0, 50]] |
| liwc_only | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 0.749 | 0.650 | [[28, 0], [0, 50]] |
| hybrid | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 1.000 | 0.962 | 0.953 | [[28, 0], [0, 50]] |
