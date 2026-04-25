# Audio Import Convention

This folder is reserved for future patient-level audio analysis.

## Supported format

- Recommended: `.wav`
- Current framework uses `scipy.io.wavfile`, so WAV is the supported format for direct analysis.

## Recommended naming

Use one of the following paths:

1. `inputs/audio/<PATIENT_ID>/<VISIT_ID>.wav`
2. `inputs/audio/<PATIENT_ID>/<VISIT_ID>_caregiver.wav`
3. `inputs/audio/<PATIENT_ID>_<VISIT_ID>.wav`
4. `inputs/audio/<VISIT_ID>.wav`

Example:

```text
inputs/audio/P-000003/V-000003-1.wav
```

## How to run after importing audio

```bash
python runners/analyze_single_patient_multimodal.py --patient-id P-000003 --visit-id V-000003-1
```

## Output

The framework will write outputs to:

```text
experiments/single_patient_multimodal/<PATIENT_ID>/<VISIT_ID>/
```

including:
- `multimodal_ablation_report.json`
- `multimodal_ablation_summary.png`
- `README.md`
