#!/usr/bin/env python3
"""Run minimal additive window OOF revision experiments."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

CURRENT_DIR = Path(__file__).resolve().parent
EMOTION_MODEL_DIR = CURRENT_DIR.parent
ANALYSIS_DIR = EMOTION_MODEL_DIR.parent
if str(ANALYSIS_DIR) not in sys.path:
    sys.path.append(str(ANALYSIS_DIR))

from emotion_model.revision.experiment import run_oof_experiment  # noqa: E402
from emotion_model.revision.manifest import ensure_manifest_files, load_patient_fold_map, load_source_data  # noqa: E402
from emotion_model.revision.paths import DEFAULT_LIWC_DICT, EXPERIMENT_ROOT, SEED, TARGETS, WINDOW_RESULTS_DIR, WINDOWS  # noqa: E402
from emotion_model.revision.reporting import write_json, write_root_readme, write_window_markdown  # noqa: E402
from emotion_model.revision.windows import reconstruct_window_rows  # noqa: E402


FIXED_WINDOW_FEATURE_MODE = "hybrid_v2"
FIXED_WINDOW_FEATURE_MODE_RATIONALE = (
    "A single common feature mode is pre-specified for all windows and both targets to avoid target/window-specific "
    "selection after inspecting window results. hybrid_v2 is the broadest main-paper V2 comparison mode because it "
    "combines TF-IDF/SVD, LIWC, and domain-rule features."
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run additive Mind-Echo window OOF revision experiments")
    parser.add_argument("--output-root", type=Path, default=EXPERIMENT_ROOT, help="Revision experiment root")
    parser.add_argument("--liwc-dict", default=DEFAULT_LIWC_DICT, help="Path to LIWC dictionary")
    parser.add_argument("--bootstrap-draws", type=int, default=2000, help="Patient bootstrap draws")
    parser.add_argument("--seed", type=int, default=SEED, help="Deterministic random seed")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest_data = load_source_data()
    output_root = args.output_root
    window_dir = output_root / WINDOW_RESULTS_DIR.name
    write_root_readme(output_root)
    manifest_paths = ensure_manifest_files(manifest_data, output_root / "manifest", seed=args.seed)
    fold_by_patient = load_patient_fold_map(manifest_paths["patient_fold_map"])

    payload: dict[str, Any] = {
        "experiment": "mind_echo_revision_minimal_window_oof",
        "source_manifest_data": "analysis/emotion_model/experiments/with_caregiver_label_comparison/data/all_data.json",
        "raw_turn_source": "processed_dataset/output/anonymized_dataset_with_caregiver.json",
        "validated_counts": {"visits": len(manifest_data), "patients": len({row["patient_id"] for row in manifest_data})},
        "grouping_field": "patient_id",
        "grouping_limitation": "No structured caregiver_id or family_id exists in the source rows; no caregiver/family grouped CV is claimed.",
        "seed": args.seed,
        "windows": list(WINDOWS),
        "fixed_feature_mode": FIXED_WINDOW_FEATURE_MODE,
        "fixed_feature_mode_rationale": FIXED_WINDOW_FEATURE_MODE_RATIONALE,
        "targets": {},
    }

    reconstructed = {window: reconstruct_window_rows(manifest_data, window) for window in WINDOWS}
    for target in TARGETS:
        payload["targets"][target] = []
        for window in WINDOWS:
            result_dir = window_dir / target / f"window_{window}" / FIXED_WINDOW_FEATURE_MODE
            summary = run_oof_experiment(
                data=reconstructed[window],
                feature_mode=FIXED_WINDOW_FEATURE_MODE,
                target=target,
                output_dir=result_dir,
                fold_by_patient=fold_by_patient,
                text_field="revision_window_text",
                liwc_dict=args.liwc_dict,
                seed=args.seed,
                n_bootstrap=args.bootstrap_draws,
            )
            summary["window"] = window
            summary["fixed_feature_mode_rationale"] = FIXED_WINDOW_FEATURE_MODE_RATIONALE
            payload["targets"][target].append(summary)

    window_dir.mkdir(parents=True, exist_ok=True)
    write_json(window_dir / "summary.json", payload)
    write_window_markdown(window_dir / "summary.md", payload)
    print(f"Saved window OOF revision outputs to: {window_dir}")


if __name__ == "__main__":
    main()
