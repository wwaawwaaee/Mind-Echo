# Main V2 OOF revision results

Patient-grouped 5-fold OOF predictions with fold-local V2 preprocessing and fixed LogisticRegression. Metrics are pooled OOF values; CIs are patient-level bootstrap percentile intervals.

## anxiety

| Feature mode | BACC | F1 | Recall | Specificity | ROC-AUC | PR-AUC | TN/FP/FN/TP |
|---|---:|---:|---:|---:|---:|---:|---|
| tfidf_svd | 0.541 | 0.537 | 0.440 | 0.643 | 0.580 | 0.700 | 18/10/28/22 |
| liwc_only | 0.493 | 0.667 | 0.700 | 0.286 | 0.416 | 0.577 | 8/20/15/35 |
| domain_only | 0.452 | 0.506 | 0.440 | 0.464 | 0.454 | 0.651 | 13/15/28/22 |
| hybrid_v2 | 0.548 | 0.615 | 0.560 | 0.536 | 0.609 | 0.716 | 15/13/22/28 |

## depression

| Feature mode | BACC | F1 | Recall | Specificity | ROC-AUC | PR-AUC | TN/FP/FN/TP |
|---|---:|---:|---:|---:|---:|---:|---|
| tfidf_svd | 0.474 | 0.577 | 0.475 | 0.474 | 0.562 | 0.808 | 9/10/31/28 |
| liwc_only | 0.566 | 0.776 | 0.763 | 0.368 | 0.590 | 0.810 | 7/12/14/45 |
| domain_only | 0.567 | 0.722 | 0.661 | 0.474 | 0.581 | 0.810 | 9/10/20/39 |
| hybrid_v2 | 0.544 | 0.619 | 0.508 | 0.579 | 0.581 | 0.815 | 11/8/29/30 |
