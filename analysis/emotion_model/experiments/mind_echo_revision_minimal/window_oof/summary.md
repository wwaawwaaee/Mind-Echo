# Window OOF revision results

Each target is evaluated separately. Every window uses the fixed 78-visit manifest and the same patient fold map. The feature mode is fixed before window inspection and recorded in metadata.

## anxiety

| Window | Feature mode | BACC | F1 | Recall | Specificity | ROC-AUC | PR-AUC | TN/FP/FN/TP |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 4 | hybrid_v2 | 0.482 | 0.556 | 0.500 | 0.464 | 0.503 | 0.644 | 13/15/25/25 |
| 6 | hybrid_v2 | 0.421 | 0.547 | 0.520 | 0.321 | 0.399 | 0.582 | 9/19/24/26 |
| 8 | hybrid_v2 | 0.496 | 0.619 | 0.600 | 0.393 | 0.530 | 0.667 | 11/17/20/30 |
| 10 | hybrid_v2 | 0.511 | 0.673 | 0.700 | 0.321 | 0.493 | 0.634 | 9/19/15/35 |
| 12 | hybrid_v2 | 0.498 | 0.535 | 0.460 | 0.536 | 0.501 | 0.645 | 15/13/27/23 |
| 15 | hybrid_v2 | 0.498 | 0.535 | 0.460 | 0.536 | 0.509 | 0.654 | 15/13/27/23 |
| full_text | hybrid_v2 | 0.548 | 0.615 | 0.560 | 0.536 | 0.609 | 0.716 | 15/13/22/28 |

## depression

| Window | Feature mode | BACC | F1 | Recall | Specificity | ROC-AUC | PR-AUC | TN/FP/FN/TP |
|---|---|---:|---:|---:|---:|---:|---:|---|
| 4 | hybrid_v2 | 0.471 | 0.679 | 0.627 | 0.316 | 0.536 | 0.764 | 6/13/22/37 |
| 6 | hybrid_v2 | 0.497 | 0.714 | 0.678 | 0.316 | 0.446 | 0.716 | 6/13/19/40 |
| 8 | hybrid_v2 | 0.567 | 0.722 | 0.661 | 0.474 | 0.570 | 0.797 | 9/10/20/39 |
| 10 | hybrid_v2 | 0.523 | 0.721 | 0.678 | 0.368 | 0.488 | 0.758 | 7/12/19/40 |
| 12 | hybrid_v2 | 0.569 | 0.660 | 0.559 | 0.579 | 0.562 | 0.790 | 11/8/26/33 |
| 15 | hybrid_v2 | 0.438 | 0.594 | 0.508 | 0.368 | 0.444 | 0.742 | 7/12/29/30 |
| full_text | hybrid_v2 | 0.544 | 0.619 | 0.508 | 0.579 | 0.581 | 0.815 | 11/8/29/30 |
