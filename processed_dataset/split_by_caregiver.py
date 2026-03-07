import json
from pathlib import Path


def patient_sort_key(patient: dict) -> int:
    patient_id = str(patient.get("patient_id", "P-0"))
    try:
        return int(patient_id.split("-")[-1])
    except ValueError:
        return 0


def has_caregiver_role(patient: dict) -> bool:
    visits = patient.get("visits", [])
    for visit in visits:
        dialogue = visit.get("dialogue", {})
        turns = dialogue.get("turns", [])
        for turn in turns:
            if turn.get("role") == "caregiver":
                return True
    return False


def build_output(base_meta: dict, base_stats: dict, patients: list) -> dict:
    stats = {
        "total_patients": len(patients),
        "total_visits": sum(len(p.get("visits", [])) for p in patients),
        "source_total_files": base_stats.get("total_files"),
        "source_converted_files": base_stats.get("converted_files"),
    }
    return {
        "dataset_meta": base_meta,
        "stats": stats,
        "patients": patients,
    }


def main():
    input_path = Path("processed_dataset/output/anonymized_dataset.json")
    out_with_path = Path("processed_dataset/output/anonymized_dataset_with_caregiver.json")
    out_without_path = Path("processed_dataset/output/anonymized_dataset_without_caregiver.json")

    raw = json.loads(input_path.read_text(encoding="utf-8"))
    patients = raw.get("patients", [])

    with_caregiver = []
    without_caregiver = []
    for patient in patients:
        if has_caregiver_role(patient):
            with_caregiver.append(patient)
        else:
            without_caregiver.append(patient)

    with_caregiver.sort(key=patient_sort_key)
    without_caregiver.sort(key=patient_sort_key)

    base_meta = dict(raw.get("dataset_meta", {}))
    base_stats = raw.get("stats", {})

    with_obj = build_output(base_meta, base_stats, with_caregiver)
    with_obj["dataset_meta"]["split"] = "with_caregiver"
    without_obj = build_output(base_meta, base_stats, without_caregiver)
    without_obj["dataset_meta"]["split"] = "without_caregiver"

    out_with_path.write_text(
        json.dumps(with_obj, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    out_without_path.write_text(
        json.dumps(without_obj, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    print(f"input patients: {len(patients)}")
    print(f"with caregiver: {len(with_caregiver)} -> {out_with_path}")
    print(f"without caregiver: {len(without_caregiver)} -> {out_without_path}")


if __name__ == "__main__":
    main()
