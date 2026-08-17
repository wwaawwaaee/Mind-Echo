from __future__ import annotations

import math
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[4]
ANALYSIS_DIR = ROOT / "analysis"
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_DIR))

from emotion_model.revision.manifest import build_patient_fold_map, ensure_manifest_files, load_patient_fold_map, validate_no_group_leakage  # noqa: E402
from emotion_model.revision.metrics import bootstrap_patient_ci, compute_binary_metrics, prevalence_random_scores  # noqa: E402
from emotion_model.revision.windows import reconstruct_window_rows  # noqa: E402

Row = dict[str, object]


def toy_data() -> list[Row]:
    return [
        {"patient_id": "p1", "visit_id": "v1", "anxiety_label": 0, "depression_label": 0},
        {"patient_id": "p1", "visit_id": "v2", "anxiety_label": 1, "depression_label": 0},
        {"patient_id": "p2", "visit_id": "v3", "anxiety_label": 0, "depression_label": 1},
        {"patient_id": "p3", "visit_id": "v4", "anxiety_label": 1, "depression_label": 1},
        {"patient_id": "p4", "visit_id": "v5", "anxiety_label": 0, "depression_label": 0},
        {"patient_id": "p5", "visit_id": "v6", "anxiety_label": 1, "depression_label": 1},
    ]


class RevisionUtilityTests(unittest.TestCase):
    def test_metric_one_class_auc_is_nan(self) -> None:
        metrics = compute_binary_metrics([0, 0, 0], [0.1, 0.2, 0.3])
        self.assertTrue(math.isnan(metrics["roc_auc"]))
        self.assertTrue(math.isnan(metrics["pr_auc"]))
        self.assertEqual(metrics["tn"], 3)

    def test_grouping_no_patient_leakage(self) -> None:
        data = toy_data()
        fold_map = build_patient_fold_map(data, n_splits=5)
        validate_no_group_leakage(data, fold_map)
        self.assertEqual(set(fold_map), {"p1", "p2", "p3", "p4", "p5"})

    def test_bootstrap_by_patient_records_valid_counts(self) -> None:
        rows = [
            {"patient_id": "p1", "y_true": 0, "y_score": 0.1},
            {"patient_id": "p1", "y_true": 1, "y_score": 0.8},
            {"patient_id": "p2", "y_true": 0, "y_score": 0.4},
            {"patient_id": "p3", "y_true": 1, "y_score": 0.9},
        ]
        ci = bootstrap_patient_ci(rows, seed=2026, n_bootstrap=25)
        self.assertGreaterEqual(set(ci), {"bacc", "roc_auc", "pr_auc"})
        self.assertEqual(ci["bacc"]["n_valid_bootstrap"], 25)
        self.assertGreaterEqual(ci["roc_auc"]["n_valid_bootstrap"], 0)
        self.assertLessEqual(ci["roc_auc"]["n_valid_bootstrap"], 25)

    def test_prevalence_random_reproducible(self) -> None:
        first, prevalence = prevalence_random_scores([0, 1, 1, 0, 1], n_test=10, seed=2026, fold=2, target="anxiety")
        second, second_prevalence = prevalence_random_scores([0, 1, 1, 0, 1], n_test=10, seed=2026, fold=2, target="anxiety")
        self.assertEqual(prevalence, 0.6)
        self.assertEqual(second_prevalence, 0.6)
        self.assertEqual(first.tolist(), second.tolist())

    def test_same_fold_map_can_be_used_across_windows(self) -> None:
        data = toy_data()
        fold_map = build_patient_fold_map(data, n_splits=5)
        window_a = [dict(row, revision_window_text="a text long enough for testing") for row in data]
        window_b = [dict(row, revision_window_text="different text long enough") for row in data]
        self.assertEqual(
            [fold_map[str(row["patient_id"])] for row in window_a],
            [fold_map[str(row["patient_id"])] for row in window_b],
        )

    def test_manifest_fold_map_round_trip(self) -> None:
        data = [
            {
                "patient_id": f"p{i:02d}",
                "visit_id": f"v{i:02d}",
                "dataset": "with_caregiver",
                "role": "caregiver",
                "gad_score": float(i % 12),
                "phq_score": float((i + 2) % 12),
                "anxiety_label": 1 if i % 3 == 0 else 0,
                "depression_label": 1 if i % 4 == 0 else 0,
                "target_role_turns": 5,
                "dialogue_turns": 10,
            }
            for i in range(63)
        ]
        data.extend(
            {
                "patient_id": f"p{i:02d}",
                "visit_id": f"v_extra_{i:02d}",
                "dataset": "with_caregiver",
                "role": "caregiver",
                "gad_score": float((i + 5) % 12),
                "phq_score": float((i + 7) % 12),
                "anxiety_label": 1 if (i + 1) % 3 == 0 else 0,
                "depression_label": 1 if (i + 2) % 4 == 0 else 0,
                "target_role_turns": 6,
                "dialogue_turns": 12,
            }
            for i in range(15)
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_paths = ensure_manifest_files(data, Path(tmpdir), seed=2026)
            loaded_fold_map = load_patient_fold_map(manifest_paths["patient_fold_map"])
        self.assertEqual(loaded_fold_map, build_patient_fold_map(data, n_splits=5))

    def test_window_reconstruction_fails_on_missing_manifest_visit(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = Path(tmpdir) / "raw.json"
            raw_path.write_text('{"patients": []}', encoding="utf-8")
            missing_manifest = [{"patient_id": "p_missing", "visit_id": "v_missing", "anxiety_label": 0, "depression_label": 0}]
            with self.assertRaisesRegex(ValueError, "could not be reconstructed"):
                reconstruct_window_rows(missing_manifest, "4", raw_path=raw_path)

    def test_window_reconstruction_fails_on_manifest_score_mismatch(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_path = Path(tmpdir) / "raw.json"
            raw_path.write_text(
                '{"patients": [{"patient_id": "p1", "visits": [{"visit_id": "v1", "dialogue": {"turns": [{"role": "caregiver", "text": "abcdef"}, {"role": "caregiver", "text": "ghijklmnopqrstuvwxyz"}]}}], "scales": [{"GAD-7": {"total": 5}, "PHQ-9": {"total": 6}}]}]}',
                encoding="utf-8",
            )
            bad_manifest = [{
                "patient_id": "p1",
                "visit_id": "v1",
                "full_text": "abcdef ghijklmnopqrstuvwxyz",
                "gad_score": 10.0,
                "phq_score": 6.0,
                "anxiety_label": 1,
                "depression_label": 0,
            }]
            with self.assertRaisesRegex(ValueError, "scores do not match manifest"):
                reconstruct_window_rows(bad_manifest, "full_text", raw_path=raw_path)


if __name__ == "__main__":
    unittest.main()
