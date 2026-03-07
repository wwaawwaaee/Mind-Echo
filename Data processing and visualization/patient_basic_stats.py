"""病人基础统计分析脚本。

输入: anonymized_dataset.json（患者中心结构）
输出: 统计汇总 JSON + 控制台关键指标
"""

import argparse
import json
from collections import Counter
from pathlib import Path
from statistics import mean, median


def safe_stats(values):
    """对数值列表做基础统计；空列表返回 None 指标，避免报错。"""
    if not values:
        return {"count": 0, "mean": None, "median": None, "min": None, "max": None}
    return {
        "count": len(values),
        "mean": round(mean(values), 2),
        "median": round(median(values), 2),
        "min": min(values),
        "max": max(values),
    }


def bucket_age(age):
    """按项目需要将年龄映射到固定分段。"""
    if age is None:
        return "unknown"
    if age <= 12:
        return "0-12"
    if age <= 17:
        return "13-17"
    if age <= 44:
        return "18-44"
    if age <= 59:
        return "45-59"
    return "60+"


def gad7_severity(total):
    """GAD-7 严重程度分级。"""
    if total <= 4:
        return "minimal"
    if total <= 9:
        return "mild"
    if total <= 14:
        return "moderate"
    return "severe"


def phq9_severity(total):
    """PHQ-9 严重程度分级。"""
    if total <= 4:
        return "minimal"
    if total <= 9:
        return "mild"
    if total <= 14:
        return "moderate"
    if total <= 19:
        return "moderately_severe"
    return "severe"


def metric_definitions():
    """返回统计结果字段释义（用普通字段代替 JSON 注释）。"""
    return {
        "base.patient_count": "患者总人数（patients 数组长度）",
        "base.patients_with_keywords": "包含 keywords 字段且非空的患者数",
        "base.patients_with_scales": "包含 scales 字段且非空的患者数",
        "demographics.gender_distribution": "患者性别分布；缺失记为 unknown",
        "demographics.age_distribution.age_stats.count": "有有效年龄（int）的患者数",
        "demographics.age_distribution.age_stats.mean": "有效年龄均值",
        "demographics.age_distribution.age_stats.median": "有效年龄中位数",
        "demographics.age_distribution.age_stats.min": "有效年龄最小值",
        "demographics.age_distribution.age_stats.max": "有效年龄最大值",
        "demographics.age_distribution.age_buckets": "年龄分段计数（0-12, 13-17, 18-44, 45-59, 60+, unknown）",
        "visits_and_dialogue.visits_per_patient.count": "患者数（用于就诊次数统计）",
        "visits_and_dialogue.visits_per_patient.mean": "每位患者平均就诊次数",
        "visits_and_dialogue.visits_per_patient.median": "每位患者就诊次数中位数",
        "visits_and_dialogue.visits_per_patient.min": "单个患者最少就诊次数",
        "visits_and_dialogue.visits_per_patient.max": "单个患者最多就诊次数",
        "visits_and_dialogue.turns_per_visit.count": "visit 总数（用于轮次数统计）",
        "visits_and_dialogue.turns_per_visit.mean": "每次就诊平均轮次数",
        "visits_and_dialogue.turns_per_visit.median": "每次就诊轮次数中位数",
        "visits_and_dialogue.turns_per_visit.min": "单次就诊最少轮次数",
        "visits_and_dialogue.turns_per_visit.max": "单次就诊最多轮次数",
        "visits_and_dialogue.turn_role_distribution": "所有轮次中各角色占比计数（doctor/patient/caregiver/other）",
        "scales.gad7_total.count": "有有效 GAD-7 总分的量表条目数",
        "scales.gad7_total.mean": "GAD-7 总分均值",
        "scales.gad7_total.median": "GAD-7 总分中位数",
        "scales.gad7_total.min": "GAD-7 总分最小值",
        "scales.gad7_total.max": "GAD-7 总分最大值",
        "scales.gad7_severity_distribution": "GAD-7 严重程度分布（minimal/mild/moderate/severe）",
        "scales.phq9_total.count": "有有效 PHQ-9 总分的量表条目数",
        "scales.phq9_total.mean": "PHQ-9 总分均值",
        "scales.phq9_total.median": "PHQ-9 总分中位数",
        "scales.phq9_total.min": "PHQ-9 总分最小值",
        "scales.phq9_total.max": "PHQ-9 总分最大值",
        "scales.phq9_severity_distribution": "PHQ-9 严重程度分布（minimal/mild/moderate/moderately_severe/severe）",
    }


def analyze(dataset):
    """遍历 patients，汇总人口学、对话、量表三个维度的基础统计。"""
    patients = dataset.get("patients", [])

    # 分布类统计使用 Counter，最终会转成普通 dict 写入 JSON。
    gender_counter = Counter()
    age_bucket_counter = Counter()
    role_counter = Counter()
    gad_severity_counter = Counter()
    phq_severity_counter = Counter()

    # 数值类统计先累计原始值，再统一走 safe_stats。
    ages = []
    visit_counts = []
    turns_per_visit = []
    gad_totals = []
    phq_totals = []

    patients_with_keywords = 0
    patients_with_scales = 0

    for p in patients:
        gender = p.get("gender", "unknown")
        gender_counter[gender] += 1

        age = p.get("age")
        if isinstance(age, int):
            ages.append(age)
        age_bucket_counter[bucket_age(age if isinstance(age, int) else None)] += 1

        if p.get("keywords"):
            patients_with_keywords += 1
        if p.get("scales"):
            patients_with_scales += 1

        visits = p.get("visits", [])
        visit_counts.append(len(visits))

        for visit in visits:
            turns = visit.get("dialogue", {}).get("turns", [])
            turns_per_visit.append(len(turns))
            for turn in turns:
                role_counter[turn.get("role", "other")] += 1

        for scale in p.get("scales", []):
            gad_total = scale.get("GAD-7", {}).get("total")
            phq_total = scale.get("PHQ-9", {}).get("total")
            if isinstance(gad_total, int):
                gad_totals.append(gad_total)
                gad_severity_counter[gad7_severity(gad_total)] += 1
            if isinstance(phq_total, int):
                phq_totals.append(phq_total)
                phq_severity_counter[phq9_severity(phq_total)] += 1

    summary = {
        # JSON 标准不支持注释；用该字段提供各统计项解释。
        "metric_definitions": metric_definitions(),
        "dataset_meta": dataset.get("dataset_meta", {}),
        "base": {
            "patient_count": len(patients),
            "patients_with_keywords": patients_with_keywords,
            "patients_with_scales": patients_with_scales,
        },
        "demographics": {
            "gender_distribution": dict(gender_counter),
            "age_distribution": {
                "age_stats": safe_stats(ages),
                "age_buckets": dict(age_bucket_counter),
            },
        },
        "visits_and_dialogue": {
            "visits_per_patient": safe_stats(visit_counts),
            "turns_per_visit": safe_stats(turns_per_visit),
            "turn_role_distribution": dict(role_counter),
        },
        "scales": {
            "gad7_total": safe_stats(gad_totals),
            "gad7_severity_distribution": dict(gad_severity_counter),
            "phq9_total": safe_stats(phq_totals),
            "phq9_severity_distribution": dict(phq_severity_counter),
        },
    }
    return summary


def main():
    parser = argparse.ArgumentParser(description="对 anonymized_dataset.json 做病人基础统计分析")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("processed_dataset/output/anonymized_dataset.json"),
        help="输入数据集 JSON 路径",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("Data processing and visualization/patient_basic_stats_summary.json"),
        help="输出统计汇总 JSON 路径",
    )
    args = parser.parse_args()

    dataset = json.loads(args.input.read_text(encoding="utf-8"))
    summary = analyze(dataset)

    # 统一写出 UTF-8 + 缩进，便于后续版本管理与人工检查。
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print("=== 病人基础统计分析 ===")
    print(f"输入文件: {args.input}")
    print(f"输出文件: {args.output}")
    print(f"患者总数: {summary['base']['patient_count']}")
    print(f"有关键词患者数: {summary['base']['patients_with_keywords']}")
    print(f"有量表患者数: {summary['base']['patients_with_scales']}")
    print(f"性别分布: {summary['demographics']['gender_distribution']}")
    print(f"年龄分段: {summary['demographics']['age_distribution']['age_buckets']}")
    print(f"每位患者就诊次数统计: {summary['visits_and_dialogue']['visits_per_patient']}")
    print(f"每次就诊轮次数统计: {summary['visits_and_dialogue']['turns_per_visit']}")
    print(f"轮次角色分布: {summary['visits_and_dialogue']['turn_role_distribution']}")
    print(f"GAD-7 总分统计: {summary['scales']['gad7_total']}")
    print(f"PHQ-9 总分统计: {summary['scales']['phq9_total']}")


if __name__ == "__main__":
    main()
