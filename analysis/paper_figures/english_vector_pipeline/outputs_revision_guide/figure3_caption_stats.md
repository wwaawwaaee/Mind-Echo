# Figure 3 caption statistics

Figure 3 reports patient-grouped five-fold pooled OOF performance for 78 visits from 63 patients. Error bars are patient-bootstrap 95% confidence intervals from 2,000 draws with seed 2026. Majority and seeded prevalence-random baselines are shown to contextualize both balanced accuracy and F1.

## Anxiety target

| Method | BACC (95% CI) | F1 (95% CI) | ROC-AUC (95% CI) | PR-AUC (95% CI) |
| --- | --- | --- | --- | --- |
| TF-IDF + SVD | 0.541 (0.425-0.658) | 0.537 (0.394-0.659) | 0.580 (0.463-0.695) | 0.700 (0.577-0.827) |
| LIWC only | 0.493 (0.394-0.596) | 0.667 (0.557-0.766) | 0.416 (0.274-0.567) | 0.577 (0.459-0.754) |
| Domain rules | 0.452 (0.337-0.560) | 0.506 (0.375-0.622) | 0.454 (0.321-0.587) | 0.651 (0.529-0.792) |
| Hybrid v2 | 0.548 (0.428-0.671) | 0.615 (0.488-0.731) | 0.609 (0.494-0.730) | 0.716 (0.587-0.838) |
| Majority baseline | 0.500 (0.500-0.500) | 0.781 (0.696-0.851) | 0.500 (0.500-0.500) | 0.641 (0.533-0.740) |
| Seeded prevalence random | 0.443 (0.332-0.553) | 0.600 (0.468-0.708) | 0.443 (0.332-0.553) | 0.616 (0.504-0.736) |

## Depression target

| Method | BACC (95% CI) | F1 (95% CI) | ROC-AUC (95% CI) | PR-AUC (95% CI) |
| --- | --- | --- | --- | --- |
| TF-IDF + SVD | 0.474 (0.341-0.618) | 0.577 (0.458-0.686) | 0.562 (0.419-0.704) | 0.808 (0.714-0.900) |
| LIWC only | 0.566 (0.455-0.671) | 0.776 (0.691-0.848) | 0.590 (0.442-0.737) | 0.810 (0.712-0.914) |
| Domain rules | 0.567 (0.423-0.707) | 0.722 (0.614-0.815) | 0.581 (0.396-0.757) | 0.810 (0.704-0.914) |
| Hybrid v2 | 0.544 (0.405-0.679) | 0.619 (0.500-0.723) | 0.581 (0.440-0.718) | 0.815 (0.724-0.906) |
| Majority baseline | 0.500 (0.500-0.500) | 0.861 (0.797-0.916) | 0.500 (0.500-0.500) | 0.756 (0.662-0.845) |
| Seeded prevalence random | 0.531 (0.416-0.647) | 0.759 (0.667-0.836) | 0.531 (0.416-0.647) | 0.768 (0.665-0.862) |

## Selected confusion-matrix models

| Target | Selected model | TN | FP | FN | TP | BACC | F1 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Anxiety | Hybrid v2 | 15 | 13 | 22 | 28 | 0.548 | 0.615 |
| Depression | LIWC only | 7 | 12 | 14 | 45 | 0.566 | 0.776 |
