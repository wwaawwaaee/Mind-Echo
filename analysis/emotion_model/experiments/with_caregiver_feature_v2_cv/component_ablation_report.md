# Component Ablation Report

## Scope

This repository currently supports **textual / linguistic components only** for the with_caregiver subgroup. There is no implemented audio/acoustic feature branch in the codebase, so the present ablation is a **component ablation over available text-side modalities/features**, not a full audio-text multimodal ablation.

## Evaluated components

- `tfidf_svd`: lexical text component after TF-IDF and SVD reduction
- `liwc_only`: LIWC-style psycholinguistic component
- `domain_only`: handcrafted domain-rule component
- `hybrid_v2`: fused text + LIWC + domain-rule component

## Main artifacts

- `summary.json`
- `summary.md`
- `v2_classification_results.png`

## Practical reading

- For **anxiety**, `hybrid_v2` achieved the best balanced accuracy among available components.
- For **depression**, `liwc_only` was the strongest traditional component baseline.
- Because no audio branch exists in the repository, these results should be described as **component ablation of available text-side features**.
