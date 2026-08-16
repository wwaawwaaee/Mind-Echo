"""Fold-local V2 feature construction reused additively for revision runners."""

from __future__ import annotations

import sys
from importlib import import_module
from pathlib import Path
from typing import Protocol

import numpy as np
from scipy.sparse import csr_matrix, hstack
from sklearn.decomposition import TruncatedSVD
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.preprocessing import StandardScaler

CORE_DIR = Path(__file__).resolve().parents[1] / "core"
ANALYSIS_DIR = Path(__file__).resolve().parents[2]
if str(CORE_DIR) not in sys.path:
    sys.path.append(str(CORE_DIR))
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_DIR))

from emotion_model.core.domain_rule_features import DOMAIN_FEATURE_KEYS, extract_domain_rule_features  # noqa: E402

LIWCAnalyzer = import_module("liwc_analyzer").LIWCAnalyzer


class LiwcAnalyzerLike(Protocol):
    def extract_features(self, text: str) -> dict[str, float]: ...

    def extract_category_ratios(self, features: dict[str, float]) -> dict[str, float]: ...

    def extract_pronoun_density(self, text: str) -> float: ...

    def get_key_indicators(self, features: dict[str, float]) -> dict[str, float]: ...


LIWC_FEATURE_KEYS = [
    "negemo", "posemo", "anx", "sad", "anger", "health", "humans", "insight", "cause", "body",
    "family", "funct", "negate", "quant", "number", "PastM", "PresentM", "FutureM", "certain",
    "tentat", "discrep", "inhib", "friend", "see", "hear", "feel", "motion", "space", "time",
    "affect_ratio", "cogmech_ratio", "bio_ratio", "social_ratio", "anxiety_index", "depression_index",
    "positive_affect", "negative_affect", "pronoun_density", "cognitive_complexity", "health_focus",
    "social_reference", "_match_ratio",
]


def build_liwc_matrix(texts: list[str], analyzer: LiwcAnalyzerLike) -> np.ndarray:
    rows = []
    for text in texts:
        features = analyzer.extract_features(text)
        features.update(analyzer.extract_category_ratios(features))
        features["pronoun_density"] = analyzer.extract_pronoun_density(text)
        features.update(analyzer.get_key_indicators(features))
        rows.append([features.get(key, 0.0) for key in LIWC_FEATURE_KEYS])
    return np.array(rows, dtype=float)


def build_domain_matrix(texts: list[str]) -> np.ndarray:
    rows = []
    for text in texts:
        features = extract_domain_rule_features(text)
        rows.append([features.get(key, 0.0) for key in DOMAIN_FEATURE_KEYS])
    return np.array(rows, dtype=float)


def create_liwc_analyzer(liwc_dict: str) -> LiwcAnalyzerLike:
    return LIWCAnalyzer(liwc_dict)


def build_fold_features(
    train_texts: list[str],
    test_texts: list[str],
    feature_mode: str,
    liwc_analyzer: LiwcAnalyzerLike,
    svd_seed: int = 42,
):
    """Build V2 features with TF-IDF, SVD, and scalers fit on the training fold only."""
    x_parts_train = []
    x_parts_test = []

    if feature_mode in {"tfidf_svd", "hybrid_v2"}:
        vectorizer = TfidfVectorizer(max_features=1000, ngram_range=(1, 2), min_df=2, max_df=0.95, sublinear_tf=True)
        x_train_tfidf = vectorizer.fit_transform(train_texts)
        x_test_tfidf = vectorizer.transform(test_texts)
        tfidf_feature_count = len(vectorizer.get_feature_names_out())
        svd_components = min(80, max(2, tfidf_feature_count - 1))
        svd = TruncatedSVD(n_components=svd_components, random_state=svd_seed)
        x_train_svd = svd.fit_transform(x_train_tfidf)
        x_test_svd = svd.transform(x_test_tfidf)
        scaler_text = StandardScaler()
        x_parts_train.append(csr_matrix(scaler_text.fit_transform(x_train_svd)))
        x_parts_test.append(csr_matrix(scaler_text.transform(x_test_svd)))

    if feature_mode in {"liwc_only", "hybrid_v2"}:
        x_train_liwc = build_liwc_matrix(train_texts, liwc_analyzer)
        x_test_liwc = build_liwc_matrix(test_texts, liwc_analyzer)
        scaler_liwc = StandardScaler()
        x_parts_train.append(csr_matrix(scaler_liwc.fit_transform(x_train_liwc)))
        x_parts_test.append(csr_matrix(scaler_liwc.transform(x_test_liwc)))

    if feature_mode in {"domain_only", "hybrid_v2"}:
        x_train_domain = build_domain_matrix(train_texts)
        x_test_domain = build_domain_matrix(test_texts)
        scaler_domain = StandardScaler()
        x_parts_train.append(csr_matrix(scaler_domain.fit_transform(x_train_domain)))
        x_parts_test.append(csr_matrix(scaler_domain.transform(x_test_domain)))

    if not x_parts_train:
        raise ValueError(f"Unsupported feature_mode: {feature_mode}")

    x_train = hstack(x_parts_train).tocsr() if len(x_parts_train) > 1 else x_parts_train[0]
    x_test = hstack(x_parts_test).tocsr() if len(x_parts_test) > 1 else x_parts_test[0]
    return x_train, x_test
