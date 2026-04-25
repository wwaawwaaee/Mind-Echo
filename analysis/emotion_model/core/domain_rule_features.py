#!/usr/bin/env python3
"""Domain-specific handcrafted features for caregiver language analysis."""

from __future__ import annotations

import re
from typing import Dict


ABSOLUTE_PATTERNS = [
    r'总是', r'一直', r'老是', r'永远', r'绝对', r'必须', r'肯定', r'一定',
    r'完全', r'根本', r'一点[也都]不', r'从来不', r'怎么都', r'绝不能',
]

SYMPTOM_WORDS = [
    '鼻塞', '流鼻血', '打呼噜', '张口呼吸', '鼻炎', '过敏', '咳嗽', '耳朵疼', '难受',
    '憋气', '缺氧', '疼', '发炎', '肿大', '睡不好', '鼻涕', '打喷嚏', '流鼻涕'
]

AMPLIFICATION_PATTERNS = [
    r'特别', r'非常', r'很严重', r'越来越', r'反复', r'经常', r'老是', r'总是',
    r'频繁', r'好多年', r'很久很久', r'一直不好', r'越来越严重', r'哗啦哗啦',
]

CATASTROPHIC_PATTERNS = [
    r'会不会.*(严重|不好|恶化|影响)', r'怕.*(影响|严重|不好|恶化)', r'越来越严重',
    r'影响.*(发育|面容|睡眠|听力|智力)', r'折磨', r'吓死', r'不行了',
]

REASSURANCE_PATTERNS = [
    r'要不要紧', r'严重吗', r'有没有事', r'正常吗', r'需不需要担心', r'没关系吧',
    r'对不对', r'是不是', r'可以吗', r'行不行',
]

RISK_CONFIRMATION_PATTERNS = [
    r'会不会', r'是不是会', r'有没有可能', r'会不会.*影响', r'会不会.*严重',
    r'会不会.*恶化', r'会不会.*越来越', r'是不是也有影响',
]

QUESTION_MARKERS = ['？', '?', '吗', '呢', '吧']


def _count_patterns(text: str, patterns: list[str]) -> int:
    return sum(len(re.findall(pattern, text)) for pattern in patterns)


def extract_domain_rule_features(text: str) -> Dict[str, float]:
    text = text or ''
    char_len = max(len(text), 1)
    sentence_like_count = max(sum(text.count(mark) for mark in ['。', '！', '？', '?']) or 1, 1)

    absolute_count = _count_patterns(text, ABSOLUTE_PATTERNS)
    symptom_count = sum(text.count(word) for word in SYMPTOM_WORDS)
    amplification_count = _count_patterns(text, AMPLIFICATION_PATTERNS)
    catastrophic_count = _count_patterns(text, CATASTROPHIC_PATTERNS)
    reassurance_count = _count_patterns(text, REASSURANCE_PATTERNS)
    risk_confirmation_count = _count_patterns(text, RISK_CONFIRMATION_PATTERNS)
    question_marker_count = sum(text.count(marker) for marker in QUESTION_MARKERS)

    symptom_amplification_score = symptom_count + amplification_count + catastrophic_count
    reassurance_score = reassurance_count + risk_confirmation_count

    return {
        'absolute_count': float(absolute_count),
        'absolute_density': absolute_count / char_len,
        'symptom_count': float(symptom_count),
        'symptom_density': symptom_count / char_len,
        'amplification_count': float(amplification_count),
        'catastrophic_count': float(catastrophic_count),
        'symptom_amplification_score': float(symptom_amplification_score),
        'symptom_amplification_density': symptom_amplification_score / char_len,
        'reassurance_count': float(reassurance_count),
        'risk_confirmation_count': float(risk_confirmation_count),
        'reassurance_score': float(reassurance_score),
        'reassurance_density': reassurance_score / char_len,
        'question_marker_count': float(question_marker_count),
        'question_density': question_marker_count / sentence_like_count,
    }


DOMAIN_FEATURE_KEYS = [
    'absolute_count', 'absolute_density',
    'symptom_count', 'symptom_density',
    'amplification_count', 'catastrophic_count',
    'symptom_amplification_score', 'symptom_amplification_density',
    'reassurance_count', 'risk_confirmation_count',
    'reassurance_score', 'reassurance_density',
    'question_marker_count', 'question_density',
]
