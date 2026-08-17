"""Shared paths for the minimal Mind-Echo revision experiments."""

from __future__ import annotations

from pathlib import Path


REVISION_DIR = Path(__file__).resolve().parent
EMOTION_MODEL_DIR = REVISION_DIR.parent
ANALYSIS_DIR = EMOTION_MODEL_DIR.parent
PROJECT_ROOT = ANALYSIS_DIR.parent

SOURCE_DATA_PATH = EMOTION_MODEL_DIR / "experiments" / "with_caregiver_label_comparison" / "data" / "all_data.json"
RAW_WITH_CAREGIVER_PATH = PROJECT_ROOT / "processed_dataset" / "output" / "anonymized_dataset_with_caregiver.json"
EXPERIMENT_ROOT = EMOTION_MODEL_DIR / "experiments" / "mind_echo_revision_minimal"
MANIFEST_DIR = EXPERIMENT_ROOT / "manifest"
MAIN_RESULTS_DIR = EXPERIMENT_ROOT / "main_v2_oof"
WINDOW_RESULTS_DIR = EXPERIMENT_ROOT / "window_oof"

DEFAULT_LIWC_DICT = r"D:\vscode-project\git-project\Auto_CLIWC\Auto_CLIWC\datasets\sc_liwc.dic"
SEED = 2026
N_SPLITS = 5
EXPECTED_VISITS = 78
EXPECTED_PATIENTS = 63
TARGETS = ("anxiety", "depression")
FEATURE_MODES = ("tfidf_svd", "liwc_only", "domain_only", "hybrid_v2")
WINDOWS = ("4", "6", "8", "10", "12", "15", "full_text")
