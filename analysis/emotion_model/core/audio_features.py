#!/usr/bin/env python3
"""Lightweight audio feature extraction using scipy/numpy for multimodal analysis."""

from __future__ import annotations

from pathlib import Path
from typing import Dict

import numpy as np
from scipy.io import wavfile
from scipy.signal import stft


AUDIO_FEATURE_KEYS = [
    'sample_rate',
    'duration_sec',
    'rms_mean',
    'rms_std',
    'peak_amplitude',
    'zero_crossing_rate',
    'silence_ratio',
    'spectral_centroid_hz',
    'spectral_bandwidth_hz',
    'dominant_frequency_hz',
]


def _to_float_mono(audio: np.ndarray) -> np.ndarray:
    if audio.ndim > 1:
        audio = np.mean(audio, axis=1)

    if np.issubdtype(audio.dtype, np.integer):
        max_val = max(abs(np.iinfo(audio.dtype).min), np.iinfo(audio.dtype).max)
        audio = audio.astype(np.float32) / float(max_val)
    else:
        audio = audio.astype(np.float32)

    return np.clip(audio, -1.0, 1.0)


def extract_audio_features(audio_path: str | Path) -> Dict[str, float]:
    audio_path = Path(audio_path)
    sample_rate, audio = wavfile.read(audio_path)
    signal = _to_float_mono(audio)

    if signal.size == 0:
        return {key: 0.0 for key in AUDIO_FEATURE_KEYS}

    duration_sec = signal.size / float(sample_rate)
    frame_length = max(256, min(2048, signal.size))
    hop_length = max(128, frame_length // 4)

    frame_count = 1 + max(0, (signal.size - frame_length) // hop_length)
    rms_values = []
    zcr_values = []
    silence_flags = []
    silence_threshold = 0.01

    for i in range(frame_count):
        start = i * hop_length
        frame = signal[start:start + frame_length]
        if frame.size == 0:
            continue
        rms = float(np.sqrt(np.mean(frame ** 2)))
        rms_values.append(rms)
        zero_crossings = np.mean(np.abs(np.diff(np.signbit(frame))).astype(np.float32))
        zcr_values.append(float(zero_crossings))
        silence_flags.append(1.0 if rms < silence_threshold else 0.0)

    rms_mean = float(np.mean(rms_values)) if rms_values else 0.0
    rms_std = float(np.std(rms_values)) if rms_values else 0.0
    peak_amplitude = float(np.max(np.abs(signal))) if signal.size else 0.0
    zero_crossing_rate = float(np.mean(zcr_values)) if zcr_values else 0.0
    silence_ratio = float(np.mean(silence_flags)) if silence_flags else 0.0

    freqs, _, zxx = stft(signal, fs=sample_rate, nperseg=min(1024, signal.size), noverlap=min(512, max(0, signal.size - 1)))
    magnitude = np.abs(zxx)
    spectrum = np.mean(magnitude, axis=1) if magnitude.size else np.zeros(1)
    if np.sum(spectrum) > 0:
        spectral_centroid = float(np.sum(freqs * spectrum) / np.sum(spectrum))
        spectral_bandwidth = float(np.sqrt(np.sum(((freqs - spectral_centroid) ** 2) * spectrum) / np.sum(spectrum)))
        dominant_frequency = float(freqs[int(np.argmax(spectrum))])
    else:
        spectral_centroid = 0.0
        spectral_bandwidth = 0.0
        dominant_frequency = 0.0

    return {
        'sample_rate': float(sample_rate),
        'duration_sec': float(duration_sec),
        'rms_mean': rms_mean,
        'rms_std': rms_std,
        'peak_amplitude': peak_amplitude,
        'zero_crossing_rate': zero_crossing_rate,
        'silence_ratio': silence_ratio,
        'spectral_centroid_hz': spectral_centroid,
        'spectral_bandwidth_hz': spectral_bandwidth,
        'dominant_frequency_hz': dominant_frequency,
    }
