"""Data loading and recomputation for English Mind-Echo academic figures."""

from __future__ import annotations

import importlib.util
import json
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, cast

import numpy as np
from scipy import stats


PROJECT_ROOT = Path(__file__).resolve().parents[3]
WITH_CAREGIVER_PATH = PROJECT_ROOT / "processed_dataset" / "output" / "anonymized_dataset_with_caregiver.json"
WITHOUT_CAREGIVER_PATH = PROJECT_ROOT / "processed_dataset" / "output" / "anonymized_dataset_without_caregiver.json"
BASIC_STATS_PATH = PROJECT_ROOT / "basic_status_summary" / "patient_basic_stats_summary.json"
SUMMARY_LATEST_PATH = PROJECT_ROOT / "analysis" / "results" / "summary_latest.json"
DOMAIN_RULE_FEATURES_PATH = PROJECT_ROOT / "analysis" / "emotion_model" / "core" / "domain_rule_features.py"
V2_CV_SUMMARY_PATH = PROJECT_ROOT / "analysis" / "emotion_model" / "experiments" / "with_caregiver_feature_v2_cv" / "summary.json"
WINDOW_COMPARISON_SUMMARY_PATH = PROJECT_ROOT / "analysis" / "emotion_model" / "experiments" / "window_comparison" / "window_comparison_summary.json"
DIRECT_LLM_SUMMARY_PATH = PROJECT_ROOT / "analysis" / "emotion_model" / "experiments" / "direct_llm_zero_shot_testset" / "summary.json"


INPUT_PATHS = {
    "with_caregiver_dataset": str(WITH_CAREGIVER_PATH),
    "without_caregiver_dataset": str(WITHOUT_CAREGIVER_PATH),
    "patient_basic_stats_summary": str(BASIC_STATS_PATH),
    "summary_latest": str(SUMMARY_LATEST_PATH),
    "domain_rule_features": str(DOMAIN_RULE_FEATURES_PATH),
    "with_caregiver_feature_v2_cv_summary": str(V2_CV_SUMMARY_PATH),
    "window_comparison_summary": str(WINDOW_COMPARISON_SUMMARY_PATH),
    "direct_llm_zero_shot_testset_summary": str(DIRECT_LLM_SUMMARY_PATH),
}


FEATURE_MODE_LABELS = {
    "tfidf_svd": "TF-IDF + SVD",
    "liwc_only": "LIWC only",
    "domain_only": "Domain rules",
    "hybrid_v2": "Hybrid v2",
}

TARGET_LABELS = {"anxiety": "Anxiety", "depression": "Depression"}


LIWC_HEATMAP_CANDIDATES = [
    "negemo",
    "posemo",
    "anx",
    "sad",
    "anger",
    "health",
    "humans",
    "insight",
    "cause",
    "body",
    "family",
    "funct",
    "negate",
    "quant",
    "number",
    "PastM",
    "PresentM",
    "FutureM",
    "certain",
    "tentat",
    "discrep",
    "inhib",
    "friend",
    "see",
    "hear",
    "feel",
    "motion",
    "space",
    "time",
]


@dataclass(frozen=True)
class CorrelationResult:
    n: int
    pearson_r: float
    pearson_p: float
    slope: Optional[float] = None
    intercept: Optional[float] = None

    def as_dict(self) -> Dict[str, Optional[float]]:
        return {
            "n": self.n,
            "pearson_r": round_float(self.pearson_r),
            "pearson_p": round_float(self.pearson_p),
            "slope": round_float(self.slope),
            "intercept": round_float(self.intercept),
        }


def round_float(value: Optional[float], digits: int = 6) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    return round(float(value), digits)


def load_json(path: Path) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def require_mapping(value: Any, context: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{context} must be an object")
    return value


def require_list(value: Any, context: str) -> List[Any]:
    if not isinstance(value, list):
        raise ValueError(f"{context} must be a list")
    return value


def require_keys(mapping: Dict[str, Any], keys: Sequence[str], context: str) -> None:
    missing = [key for key in keys if key not in mapping]
    if missing:
        raise ValueError(f"{context} missing required keys: {missing}")


def require_number(value: Any, context: str) -> float:
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise ValueError(f"{context} must be numeric")
    return float(value)


def metric_summary(classification: Dict[str, Any], metric: str, context: str) -> Dict[str, float]:
    raw_metric = require_mapping(classification.get(metric), f"{context}.{metric}")
    require_keys(raw_metric, ["mean", "std"], f"{context}.{metric}")
    return {
        "mean": require_number(raw_metric["mean"], f"{context}.{metric}.mean"),
        "std": require_number(raw_metric["std"], f"{context}.{metric}.std"),
    }


def load_v2_cv_summary() -> Dict[str, Any]:
    summary = load_json(V2_CV_SUMMARY_PATH)
    require_keys(summary, ["group", "evaluation", "targets"], "V2 CV summary")
    require_equal("V2 CV group", summary["group"], "with_caregiver")
    targets = require_mapping(summary["targets"], "V2 CV targets")
    for target in ["anxiety", "depression"]:
        rows = require_list(targets.get(target), f"V2 CV targets.{target}")
        modes = []
        for idx, row_any in enumerate(rows):
            row = require_mapping(row_any, f"V2 CV targets.{target}[{idx}]")
            require_keys(row, ["feature_mode", "classification"], f"V2 CV targets.{target}[{idx}]")
            mode = row["feature_mode"]
            if mode not in FEATURE_MODE_LABELS:
                raise ValueError(f"Unsupported V2 CV feature mode for {target}: {mode}")
            modes.append(mode)
            classification = require_mapping(row["classification"], f"V2 CV targets.{target}.{mode}.classification")
            for metric in ["acc", "f1", "precision", "recall", "specificity", "balanced_acc"]:
                metric_summary(classification, metric, f"V2 CV targets.{target}.{mode}.classification")
        require_equal(f"V2 CV feature modes for {target}", modes, list(FEATURE_MODE_LABELS.keys()))
    return summary


def load_window_comparison_summary() -> Dict[str, Any]:
    summary = load_json(WINDOW_COMPARISON_SUMMARY_PATH)
    require_keys(summary, ["datasets", "windows", "results"], "window comparison summary")
    require_equal("window comparison datasets", summary["datasets"], ["with_caregiver"])
    results = require_list(summary["results"], "window comparison results")
    if not results:
        raise ValueError("window comparison results must not be empty")
    for idx, row_any in enumerate(results):
        row = require_mapping(row_any, f"window comparison results[{idx}]")
        require_keys(row, ["window", "samples", "patients", "acc", "f1", "gad_corr", "phq_corr"], f"window comparison results[{idx}]")
        if "balanced_acc" in row:
            raise ValueError("window comparison summary unexpectedly contains balanced_acc; Figure 4 must label acc separately")
        window = row["window"]
        if not isinstance(window, (int, str)) or isinstance(window, bool):
            raise ValueError(f"window comparison results[{idx}].window must be an integer or 'full_text'")
        for key in ["samples", "patients", "acc", "f1", "gad_corr", "phq_corr"]:
            require_number(row[key], f"window comparison results[{idx}].{key}")
    return summary


def load_direct_llm_summary() -> Dict[str, Any]:
    summary = load_json(DIRECT_LLM_SUMMARY_PATH)
    require_keys(summary, ["method", "dataset", "n_samples", "anxiety", "depression"], "direct LLM summary")
    require_equal("direct LLM n_samples", summary["n_samples"], 12)
    for target in ["anxiety", "depression"]:
        target_metrics = require_mapping(summary[target], f"direct LLM {target}")
        require_keys(target_metrics, ["acc", "f1", "precision", "recall", "specificity", "balanced_acc"], f"direct LLM {target}")
        for metric in ["acc", "f1", "precision", "recall", "specificity", "balanced_acc"]:
            require_number(target_metrics[metric], f"direct LLM {target}.{metric}")
    return summary


def v2_cv_rows(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    targets = require_mapping(summary["targets"], "V2 CV targets")
    for target in ["anxiety", "depression"]:
        for row_any in require_list(targets[target], f"V2 CV targets.{target}"):
            row = require_mapping(row_any, f"V2 CV targets.{target} row")
            mode = str(row["feature_mode"])
            classification = require_mapping(row["classification"], f"V2 CV {target}.{mode}.classification")
            rows.append(
                {
                    "target": target,
                    "target_label": TARGET_LABELS[target],
                    "feature_mode": mode,
                    "feature_mode_label": FEATURE_MODE_LABELS[mode],
                    "acc_mean": metric_summary(classification, "acc", f"V2 CV {target}.{mode}.classification")["mean"],
                    "acc_std": metric_summary(classification, "acc", f"V2 CV {target}.{mode}.classification")["std"],
                    "f1_mean": metric_summary(classification, "f1", f"V2 CV {target}.{mode}.classification")["mean"],
                    "f1_std": metric_summary(classification, "f1", f"V2 CV {target}.{mode}.classification")["std"],
                    "balanced_acc_mean": metric_summary(classification, "balanced_acc", f"V2 CV {target}.{mode}.classification")["mean"],
                    "balanced_acc_std": metric_summary(classification, "balanced_acc", f"V2 CV {target}.{mode}.classification")["std"],
                    "precision_mean": metric_summary(classification, "precision", f"V2 CV {target}.{mode}.classification")["mean"],
                    "precision_std": metric_summary(classification, "precision", f"V2 CV {target}.{mode}.classification")["std"],
                    "recall_mean": metric_summary(classification, "recall", f"V2 CV {target}.{mode}.classification")["mean"],
                    "recall_std": metric_summary(classification, "recall", f"V2 CV {target}.{mode}.classification")["std"],
                    "specificity_mean": metric_summary(classification, "specificity", f"V2 CV {target}.{mode}.classification")["mean"],
                    "specificity_std": metric_summary(classification, "specificity", f"V2 CV {target}.{mode}.classification")["std"],
                }
            )
    return rows


def figure3_dataset() -> Dict[str, Any]:
    summary = load_v2_cv_summary()
    return {
        "input_paths": INPUT_PATHS,
        "source": str(V2_CV_SUMMARY_PATH),
        "feature_modes": list(FEATURE_MODE_LABELS.keys()),
        "feature_mode_labels": FEATURE_MODE_LABELS,
        "targets": ["anxiety", "depression"],
        "target_labels": TARGET_LABELS,
        "rows": v2_cv_rows(summary),
        "error_bar_note": "Error bars use the source cross-validation standard deviations from with_caregiver_feature_v2_cv/summary.json.",
    }


def figure4_dataset() -> Dict[str, Any]:
    summary = load_window_comparison_summary()
    rows = []
    for row_any in require_list(summary["results"], "window comparison results"):
        row = require_mapping(row_any, "window comparison row")
        rows.append(
            {
                "window": row["window"],
                "window_label": "Full text" if row["window"] == "full_text" else str(row["window"]),
                "samples": int(require_number(row["samples"], "window samples")),
                "patients": int(require_number(row["patients"], "window patients")),
                "acc": require_number(row["acc"], "window acc"),
                "f1": require_number(row["f1"], "window f1"),
                "gad_corr": require_number(row["gad_corr"], "window gad_corr"),
                "phq_corr": require_number(row["phq_corr"], "window phq_corr"),
            }
        )
    return {
        "input_paths": INPUT_PATHS,
        "source": str(WINDOW_COMPARISON_SUMMARY_PATH),
        "rows": rows,
        "availability_note": "The current window summary provides one combined available-window classification series with ACC and F1 plus GAD-7/PHQ-9 correlations. It does not provide separate anxiety/depression classification trajectories or window balanced_acc.",
    }


def find_v2_row(rows: Sequence[Dict[str, Any]], target: str, feature_mode: str) -> Dict[str, Any]:
    for row in rows:
        if row["target"] == target and row["feature_mode"] == feature_mode:
            return row
    raise ValueError(f"Missing V2 CV row for {target}/{feature_mode}")


def figure5_dataset() -> Dict[str, Any]:
    v2_summary = load_v2_cv_summary()
    direct_summary = load_direct_llm_summary()
    v2_rows = v2_cv_rows(v2_summary)
    best_ml_specs = {"anxiety": "hybrid_v2", "depression": "liwc_only"}
    rows = []
    for target in ["anxiety", "depression"]:
        best_mode = best_ml_specs[target]
        best_row = find_v2_row(v2_rows, target, best_mode)
        rows.append(
            {
                "target": target,
                "target_label": TARGET_LABELS[target],
                "method": "Best ML",
                "method_detail": FEATURE_MODE_LABELS[best_mode],
                "n": None,
                "f1": best_row["f1_mean"],
                "f1_std": best_row["f1_std"],
                "balanced_acc": best_row["balanced_acc_mean"],
                "balanced_acc_std": best_row["balanced_acc_std"],
            }
        )
        direct_metrics = require_mapping(direct_summary[target], f"direct LLM {target}")
        rows.append(
            {
                "target": target,
                "target_label": TARGET_LABELS[target],
                "method": "Direct zero-shot LLM",
                "method_detail": "Direct manual judgment prompt",
                "n": int(direct_summary["n_samples"]),
                "f1": require_number(direct_metrics["f1"], f"direct LLM {target}.f1"),
                "f1_std": 0.0,
                "balanced_acc": require_number(direct_metrics["balanced_acc"], f"direct LLM {target}.balanced_acc"),
                "balanced_acc_std": 0.0,
            }
        )
    return {
        "input_paths": INPUT_PATHS,
        "sources": {"best_ml": str(V2_CV_SUMMARY_PATH), "direct_llm": str(DIRECT_LLM_SUMMARY_PATH)},
        "best_ml_specs": best_ml_specs,
        "direct_llm_n": int(direct_summary["n_samples"]),
        "rows": rows,
    }


def table1_feature_set_performance_rows() -> List[Dict[str, Any]]:
    return v2_cv_rows(load_v2_cv_summary())


def section_3_2_correlation_rows() -> List[Dict[str, Any]]:
    summary = load_json(SUMMARY_LATEST_PATH)
    correlations_root = require_mapping(summary.get("correlations"), "summary_latest correlations")
    rows: List[Dict[str, Any]] = []
    for dataset in ["with_caregiver", "without_caregiver"]:
        dataset_block = require_mapping(correlations_root.get(dataset), f"summary_latest correlations.{dataset}")
        require_keys(dataset_block, ["respondent", "target_role", "sample_size", "correlations"], f"summary_latest correlations.{dataset}")
        scale_blocks = require_mapping(dataset_block["correlations"], f"summary_latest correlations.{dataset}.correlations")
        for scale in ["GAD-7_mean", "PHQ-9_mean"]:
            feature_blocks = require_mapping(scale_blocks.get(scale), f"summary_latest correlations.{dataset}.{scale}")
            for feature, values_any in feature_blocks.items():
                values = require_mapping(values_any, f"summary_latest correlations.{dataset}.{scale}.{feature}")
                require_keys(values, ["pearson_r", "pearson_p", "spearman_r", "spearman_p", "significant"], f"summary_latest correlations.{dataset}.{scale}.{feature}")
                rows.append(
                    {
                        "dataset": dataset,
                        "respondent": dataset_block["respondent"],
                        "target_role": dataset_block["target_role"],
                        "sample_size": int(require_number(dataset_block["sample_size"], f"summary_latest correlations.{dataset}.sample_size")),
                        "scale": scale,
                        "feature": str(feature),
                        "pearson_r": require_number(values["pearson_r"], f"summary_latest correlations.{dataset}.{scale}.{feature}.pearson_r"),
                        "pearson_p": require_number(values["pearson_p"], f"summary_latest correlations.{dataset}.{scale}.{feature}.pearson_p"),
                        "spearman_r": require_number(values["spearman_r"], f"summary_latest correlations.{dataset}.{scale}.{feature}.spearman_r"),
                        "spearman_p": require_number(values["spearman_p"], f"summary_latest correlations.{dataset}.{scale}.{feature}.spearman_p"),
                        "significant": bool(values["significant"]),
                    }
                )
    return rows


def load_primary_inputs() -> Dict[str, Dict[str, Any]]:
    return {
        "with_caregiver": load_json(WITH_CAREGIVER_PATH),
        "without_caregiver": load_json(WITHOUT_CAREGIVER_PATH),
        "basic_stats": load_json(BASIC_STATS_PATH),
        "summary_latest": load_json(SUMMARY_LATEST_PATH),
    }


def patients_from_inputs(inputs: Dict[str, Dict[str, Any]]) -> List[Tuple[str, Dict[str, Any]]]:
    rows: List[Tuple[str, Dict[str, Any]]] = []
    for dataset_name in ["with_caregiver", "without_caregiver"]:
        for patient in inputs[dataset_name].get("patients", []):
            rows.append((dataset_name, patient))
    return rows


def numeric_total(scale: Dict[str, Any], key: str) -> Optional[float]:
    value = scale.get(key, {}).get("total")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def collect_scale_records(inputs: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    records: List[Dict[str, Any]] = []
    for dataset_name, patient in patients_from_inputs(inputs):
        for scale_index, scale in enumerate(patient.get("scales", [])):
            gad = numeric_total(scale, "GAD-7")
            phq = numeric_total(scale, "PHQ-9")
            if gad is not None and phq is not None:
                records.append(
                    {
                        "dataset": dataset_name,
                        "patient_id": patient.get("patient_id"),
                        "scale_index": scale_index,
                        "gad7": gad,
                        "phq9": phq,
                        "respondent_role": scale.get("respondent_role", "unknown"),
                    }
                )
    return records


def gad7_severity(total: float) -> str:
    if total <= 4:
        return "minimal"
    if total <= 9:
        return "mild"
    if total <= 14:
        return "moderate"
    return "severe"


def phq9_severity(total: float) -> str:
    if total <= 4:
        return "minimal"
    if total <= 9:
        return "mild"
    if total <= 14:
        return "moderate"
    if total <= 19:
        return "moderately severe"
    return "severe"


def pearson_with_regression(x: Sequence[float], y: Sequence[float]) -> CorrelationResult:
    x_arr = np.asarray(x, dtype=float)
    y_arr = np.asarray(y, dtype=float)
    mask = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_arr = x_arr[mask]
    y_arr = y_arr[mask]
    if len(x_arr) < 3 or np.std(x_arr) == 0 or np.std(y_arr) == 0:
        return CorrelationResult(n=int(len(x_arr)), pearson_r=np.nan, pearson_p=np.nan)
    pearson = cast(Any, stats.pearsonr(x_arr, y_arr))
    regression = cast(Any, stats.linregress(x_arr, y_arr))
    return CorrelationResult(
        n=int(len(x_arr)),
        pearson_r=float(pearson[0]),
        pearson_p=float(pearson[1]),
        slope=float(regression.slope),
        intercept=float(regression.intercept),
    )


def role_turn_distribution(inputs: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    counter: Counter[str] = Counter()
    for _, patient in patients_from_inputs(inputs):
        for visit in patient.get("visits", []):
            for turn in visit.get("dialogue", {}).get("turns", []):
                counter[turn.get("role", "other")] += 1
    return {role: int(counter.get(role, 0)) for role in ["doctor", "caregiver", "patient"]}


def pediatric_age_bins(inputs: Dict[str, Dict[str, Any]]) -> Dict[str, int]:
    bins = {"0-6": 0, "7-12": 0, "13-17": 0}
    for _, patient in patients_from_inputs(inputs):
        age = patient.get("age")
        if isinstance(age, int) and 0 <= age <= 17:
            if age <= 6:
                bins["0-6"] += 1
            elif age <= 12:
                bins["7-12"] += 1
            else:
                bins["13-17"] += 1
    return bins


def count_percent(count: int, total: int) -> Dict[str, float]:
    return {"count": int(count), "percentage": round(100.0 * count / total, 2) if total else 0.0}


def require_equal(name: str, actual: Any, expected: Any) -> None:
    if actual != expected:
        raise ValueError(f"{name} mismatch: expected {expected}, found {actual}")


def figure1_dataset() -> Dict[str, Any]:
    inputs = load_primary_inputs()
    basic_stats = inputs["basic_stats"]
    scale_records = collect_scale_records(inputs)
    role_counts = role_turn_distribution(inputs)
    age_bins = pediatric_age_bins(inputs)
    gad_values = [row["gad7"] for row in scale_records]
    phq_values = [row["phq9"] for row in scale_records]

    gad_severity = Counter(gad7_severity(value) for value in gad_values)
    phq_severity = Counter(phq9_severity(value) for value in phq_values)
    gad_risk_count = sum(1 for value in gad_values if value >= 10)
    phq_risk_count = sum(1 for value in phq_values if value >= 10)

    total_turns = sum(role_counts.values())
    valid_pediatric_age_n = sum(age_bins.values())
    scale_record_n = len(scale_records)

    require_equal("total role turns", total_turns, 5531)
    require_equal("valid pediatric ages", valid_pediatric_age_n, 21)
    require_equal("paired scale records", scale_record_n, 109)
    require_equal("patient total", len(patients_from_inputs(inputs)), basic_stats["base"]["patient_count"])
    require_equal("role turn distribution", role_counts, basic_stats["visits_and_dialogue"]["turn_role_distribution"])
    require_equal("GAD-7 scale count", scale_record_n, basic_stats["scales"]["gad7_total"]["count"])
    require_equal("PHQ-9 scale count", scale_record_n, basic_stats["scales"]["phq9_total"]["count"])

    return {
        "input_paths": INPUT_PATHS,
        "sample_counts": {
            "total_turns": total_turns,
            "valid_pediatric_age_n": valid_pediatric_age_n,
            "scale_record_n": scale_record_n,
            "patients_total": len(patients_from_inputs(inputs)),
        },
        "role_turn_distribution": role_counts,
        "role_turn_percentages": {role: round(100.0 * count / total_turns, 2) for role, count in role_counts.items()},
        "pediatric_age_bins": age_bins,
        "severity_distribution": {
            "GAD-7": {label: int(gad_severity.get(label, 0)) for label in ["minimal", "mild", "moderate", "severe"]},
            "PHQ-9": {label: int(phq_severity.get(label, 0)) for label in ["minimal", "mild", "moderate", "moderately severe", "severe"]},
        },
        "scale_pairs": {"gad7": gad_values, "phq9": phq_values},
        "gad7_phq9_correlation": pearson_with_regression(gad_values, phq_values).as_dict(),
        "clinical_risk_thresholds": {
            "threshold": ">=10",
            "GAD-7": count_percent(gad_risk_count, scale_record_n),
            "PHQ-9": count_percent(phq_risk_count, scale_record_n),
        },
        "source_reconciliation": {
            "turns_source": "Recomputed from both anonymized datasets and mechanically checked against patient_basic_stats_summary.json.",
            "age_source": "Valid pediatric ages are integer ages from 0 through 17 across both anonymized datasets.",
            "scale_record_source": "Paired GAD-7/PHQ-9 scale records are mechanically checked against patient_basic_stats_summary.json counts; they are not caregiver-only patients.",
        },
    }


def summary_raw(summary: Dict[str, Any], scale_key: str) -> Dict[str, Any]:
    return summary["correlations"]["with_caregiver"]["raw_data"][scale_key]


def raw_feature_series(summary: Dict[str, Any], scale_key: str, feature: str) -> Tuple[List[float], List[float]]:
    raw = summary_raw(summary, scale_key)
    scale_values = [float(v) for v in raw.get("scale_values", [])]
    feature_values = [float(v) for v in raw.get("feature_data", {}).get(feature, [])]
    if len(scale_values) != len(feature_values):
        raise ValueError(f"Length mismatch for {scale_key}/{feature}: {len(scale_values)} vs {len(feature_values)}")
    return scale_values, feature_values


def select_heatmap_features(summary: Dict[str, Any], top_n: int = 10) -> Dict[str, Any]:
    selected_rows = []
    for feature in LIWC_HEATMAP_CANDIDATES:
        rows = {}
        p_values = []
        for scale_key in ["GAD-7_mean", "PHQ-9_mean"]:
            try:
                x, y = raw_feature_series(summary, scale_key, feature)
            except ValueError:
                continue
            corr = pearson_with_regression(x, y)
            rows[scale_key] = corr
            if np.isfinite(corr.pearson_p):
                p_values.append(corr.pearson_p)
        if set(rows.keys()) == {"GAD-7_mean", "PHQ-9_mean"} and p_values:
            selected_rows.append((min(p_values), feature, rows))

    selected_rows.sort(key=lambda item: (item[0], item[1]))
    selected_rows = selected_rows[:top_n]
    features = [feature for _, feature, _ in selected_rows]
    correlations = {
        feature: {scale: corr.as_dict() for scale, corr in rows.items()} for _, feature, rows in selected_rows
    }
    return {"features": features, "correlations": correlations}


def aggregate_scale_means(scales: List[Dict[str, Any]]) -> Dict[str, float]:
    gad = [numeric_total(scale, "GAD-7") for scale in scales]
    phq = [numeric_total(scale, "PHQ-9") for scale in scales]
    gad = [value for value in gad if value is not None]
    phq = [value for value in phq if value is not None]
    result: Dict[str, float] = {}
    if gad:
        result["GAD-7_mean"] = round(float(np.mean(gad)), 2)
    if phq:
        result["PHQ-9_mean"] = round(float(np.mean(phq)), 2)
    return result


def collect_role_text(patient: Dict[str, Any], role: str) -> str:
    texts: List[str] = []
    for visit in patient.get("visits", []):
        for turn in visit.get("dialogue", {}).get("turns", []):
            if turn.get("role") == role and turn.get("text"):
                texts.append(str(turn["text"]))
    return "\n".join(texts)


def load_domain_extractor() -> Callable[[str], Dict[str, float]]:
    spec = importlib.util.spec_from_file_location("mind_echo_domain_rule_features", DOMAIN_RULE_FEATURES_PATH)
    if spec is None or spec.loader is None:
        raise ImportError(f"Unable to import {DOMAIN_RULE_FEATURES_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.extract_domain_rule_features


def operational_absolute_density(role: str = "caregiver") -> Dict[str, Any]:
    dataset = load_json(WITH_CAREGIVER_PATH)
    extractor = load_domain_extractor()
    rows: List[Dict[str, Any]] = []
    for patient in dataset.get("patients", []):
        scale_means = aggregate_scale_means(patient.get("scales", []))
        text = collect_role_text(patient, role)
        if scale_means and text:
            features = extractor(text)
            rows.append(
                {
                    "patient_id": patient.get("patient_id"),
                    "GAD-7_mean": scale_means.get("GAD-7_mean"),
                    "PHQ-9_mean": scale_means.get("PHQ-9_mean"),
                    "absolute_density": float(features.get("absolute_density", 0.0)),
                    "absolute_count": float(features.get("absolute_count", 0.0)),
                    "text_role": role,
                }
            )
    return {
        "scale_values": [row["GAD-7_mean"] for row in rows if row.get("GAD-7_mean") is not None],
        "feature_values": [row["absolute_density"] for row in rows if row.get("GAD-7_mean") is not None],
        "rows": rows,
        "source_note": (
            "absolute_density was operationalized with analysis/emotion_model/core/domain_rule_features.py "
            f"from with_caregiver {role}-role dialogue text because summary_latest raw_data does not contain absolutist expression values."
        ),
    }


def choose_phq_additional_feature(summary: Dict[str, Any]) -> Dict[str, Any]:
    candidates = ["posemo", "tentat"]
    ranked = []
    for feature in candidates:
        x, y = raw_feature_series(summary, "PHQ-9_mean", feature)
        corr = pearson_with_regression(x, y)
        ranked.append((corr.pearson_p if np.isfinite(corr.pearson_p) else 1.0, feature, corr))
    ranked.sort(key=lambda item: (item[0], item[1]))
    _, feature, corr = ranked[0]
    return {
        "feature": feature,
        "correlation": corr.as_dict(),
        "selection_rule": "Strongest available PHQ-9 Pearson p-value among posemo and tentat after reserving sad and certain as required panels.",
    }


def feature_scatter_payload(summary: Dict[str, Any], scale_key: str, feature: str) -> Dict[str, Any]:
    x, y = raw_feature_series(summary, scale_key, feature)
    return {"scale_values": x, "feature_values": y, "correlation": pearson_with_regression(x, y).as_dict()}


def figure2_dataset() -> Dict[str, Any]:
    inputs = load_primary_inputs()
    summary = inputs["summary_latest"]
    heatmap = select_heatmap_features(summary, top_n=10)
    absolute = operational_absolute_density(role="caregiver")
    absolute_corr = pearson_with_regression(absolute["scale_values"], absolute["feature_values"])
    additional = choose_phq_additional_feature(summary)

    gad_features = {
        "negemo": feature_scatter_payload(summary, "GAD-7_mean", "negemo"),
        "discrep": feature_scatter_payload(summary, "GAD-7_mean", "discrep"),
        "absolute_density": {
            "scale_values": absolute["scale_values"],
            "feature_values": absolute["feature_values"],
            "correlation": absolute_corr.as_dict(),
        },
    }
    phq_feature_names = ["sad", "certain", additional["feature"]]
    phq_features = {feature: feature_scatter_payload(summary, "PHQ-9_mean", feature) for feature in phq_feature_names}

    return {
        "input_paths": INPUT_PATHS,
        "sample_counts": {
            "with_caregiver_patient_level_correlation_n": int(summary["correlations"]["with_caregiver"].get("sample_size", 0)),
            "absolute_density_n": int(len(absolute["scale_values"])),
        },
        "heatmap": heatmap,
        "gad7_scatter_features": gad_features,
        "phq9_scatter_features": phq_features,
        "selected_phq9_additional_feature": additional,
        "operationalized_features": {
            "absolute_density": absolute["source_note"],
        },
        "skipped_features": {
            "child_compliance": "Not used: no documented child-compliance extractor or lexicon was found in the existing analysis pipeline; a strongest available PHQ-9 LIWC feature was used instead.",
        },
    }
