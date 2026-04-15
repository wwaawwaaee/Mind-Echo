# With-caregiver Feature Mode Comparison

| Feature Mode | ACC | F1 | Precision | Recall | Specificity | Balanced ACC | GAD r | PHQ r | Confusion Matrix |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| text_only | 0.500 | 0.571 | 0.500 | 0.667 | 0.333 | 0.500 | 0.301 | 0.361 | [[2, 4], [2, 4]] |
| liwc_only | 0.333 | 0.429 | 0.375 | 0.500 | 0.167 | 0.333 | -0.264 | -0.167 | [[1, 5], [3, 3]] |
| hybrid | 0.333 | 0.429 | 0.375 | 0.500 | 0.167 | 0.333 | -0.220 | -0.123 | [[1, 5], [3, 3]] |
