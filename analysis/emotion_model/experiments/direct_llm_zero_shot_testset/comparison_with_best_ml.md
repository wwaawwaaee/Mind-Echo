# Direct LLM Zero-shot vs Best Traditional Baseline

> Note: the direct LLM zero-shot result below is a **single held-out test-set judgment**, whereas the best traditional baseline is reported from **patient-level 5-fold cross-validation**. They are informative for practical comparison, but not strictly the same evaluation protocol.

## Anxiety

| Method | ACC | F1 | Precision | Recall | Specificity | Balanced ACC |
|---|---:|---:|---:|---:|---:|---:|
| Direct LLM zero-shot | 0.833 | 0.833 | 0.833 | 0.833 | 0.833 | 0.833 |

## Depression

| Method | ACC | F1 | Precision | Recall | Specificity | Balanced ACC |
|---|---:|---:|---:|---:|---:|---:|
| Direct LLM zero-shot | 0.667 | 0.750 | 1.000 | 0.600 | 1.000 | 0.800 |
| Best traditional baseline (LIWC-only, 5-fold CV) | 0.667 ± 0.118 | 0.761 ± 0.114 | 0.785 ± 0.130 | 0.753 ± 0.138 | 0.470 ± 0.302 | 0.612 ± 0.152 |
