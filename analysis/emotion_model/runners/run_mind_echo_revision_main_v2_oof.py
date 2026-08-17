#!/usr/bin/env python3
"""Run minimal additive main V2 OOF revision experiments."""

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
from emotion_model.revision.paths import DEFAULT_LIWC_DICT, EXPERIMENT_ROOT, FEATURE_MODES, MAIN_RESULTS_DIR, SEED, TARGETS  # noqa: E402
from emotion_model.revision.reporting import write_json, write_main_markdown, write_root_readme  # noqa: E402


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run additive Mind-Echo main V2 OOF revision experiments")
    parser.add_argument("--output-root", type=Path, default=EXPERIMENT_ROOT, help="Revision experiment root")
    parser.add_argument("--liwc-dict", default=DEFAULT_LIWC_DICT, help="Path to LIWC dictionary")
    parser.add_argument("--bootstrap-draws", type=int, default=2000, help="Patient bootstrap draws")
    parser.add_argument("--seed", type=int, default=SEED, help="Deterministic random seed")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    data = load_source_data()
    output_root = args.output_root
    main_dir = output_root / MAIN_RESULTS_DIR.name
    write_root_readme(output_root)
    manifest_paths = ensure_manifest_files(data, output_root / "manifest", seed=args.seed)
    fold_by_patient = load_patient_fold_map(manifest_paths["patient_fold_map"])

    payload: dict[str, Any] = {
        "experiment": "mind_echo_revision_minimal_main_v2_oof",
        "source_data": "analysis/emotion_model/experiments/with_caregiver_label_comparison/data/all_data.json",
        "validated_counts": {"visits": len(data), "patients": len({row["patient_id"] for row in data})},
        "grouping_field": "patient_id",
        "grouping_limitation": "No structured caregiver_id or family_id exists in the source rows; no caregiver/family grouped CV is claimed.",
        "seed": args.seed,
        "targets": {},
    }

    for target in TARGETS:
        payload["targets"][target] = []
        for feature_mode in FEATURE_MODES:
            result_dir = main_dir / target / feature_mode
            summary = run_oof_experiment(
                data=data,
                feature_mode=feature_mode,
                target=target,
                output_dir=result_dir,
                fold_by_patient=fold_by_patient,
                text_field="full_text",
                liwc_dict=args.liwc_dict,
                seed=args.seed,
                n_bootstrap=args.bootstrap_draws,
            )
            payload["targets"][target].append(summary)

    main_dir.mkdir(parents=True, exist_ok=True)
    write_json(main_dir / "summary.json", payload)
    write_main_markdown(main_dir / "summary.md", payload)
    print(f"Saved main V2 OOF revision outputs to: {main_dir}")


if __name__ == "__main__":
    main()
