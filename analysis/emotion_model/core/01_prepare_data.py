#!/usr/bin/env python3
"""
数据准备脚本
从Mind-Echo数据集构建训练数据
"""

import csv
import argparse
import json
import os
from pathlib import Path
import numpy as np


CURRENT_DIR = Path(__file__).resolve().parent
EXPERIMENT_ROOT = CURRENT_DIR.parent
ANALYSIS_DIR = EXPERIMENT_ROOT.parent
PROJECT_ROOT = ANALYSIS_DIR.parent


def parse_args():
    parser = argparse.ArgumentParser(description='Prepare visit-level early-text datasets')
    parser.add_argument('--input-dir', default=str(PROJECT_ROOT / 'processed_dataset' / 'output'), help='Input dataset directory')
    parser.add_argument('--output-dir', default=str(EXPERIMENT_ROOT / 'outputs' / 'default' / 'data'), help='Output data directory')
    parser.add_argument('--datasets', nargs='+', choices=['with_caregiver', 'without_caregiver'], default=['with_caregiver', 'without_caregiver'], help='Datasets to include')
    parser.add_argument('--max-role-turns', type=int, default=10, help='Max target-role turns for early_text')
    parser.add_argument('--max-chars', type=int, default=1000, help='Max characters for early_text')
    parser.add_argument('--min-chars', type=int, default=30, help='Min characters required for early_text')
    return parser.parse_args()


def load_datasets(input_dir):
    """加载数据集"""
    base_path = input_dir

    datasets = {}
    with open(f'{base_path}/anonymized_dataset_with_caregiver.json', encoding='utf-8') as f:
        datasets['with_caregiver'] = json.load(f)

    with open(f'{base_path}/anonymized_dataset_without_caregiver.json', encoding='utf-8') as f:
        datasets['without_caregiver'] = json.load(f)

    return datasets


def extract_early_role_text(turns, target_role, max_role_turns=10, max_chars=1000):
    """提取同一次就诊中目标角色的早期文本。"""
    text_parts = []
    char_count = 0

    role_turn_count = 0
    for turn in turns:
        if turn.get('role') != target_role:
            continue

        text = turn.get('text', '').strip()
        if not text:
            continue
        if char_count + len(text) > max_chars:
            break

        text_parts.append(f"{target_role}: {text}")
        char_count += len(text)
        role_turn_count += 1

        if role_turn_count >= max_role_turns:
            break

    return '\n'.join(text_parts)


def extract_role_text(turns, target_role):
    """提取同一次就诊中目标角色的全部文本。"""
    texts = []
    for turn in turns:
        if turn.get('role') != target_role:
            continue
        text = turn.get('text', '').strip()
        if text:
            texts.append(text)
    return ' '.join(texts)


def extract_visit_data(dataset, dataset_name, min_chars=30, max_role_turns=10, max_chars=1000):
    """提取visit级别数据：同一次就诊文本与同一次量表严格对齐。"""
    results = []

    target_role = 'caregiver' if dataset_name == 'with_caregiver' else 'patient'

    for patient in dataset.get('patients', []):
        patient_id = patient.get('patient_id')
        visits = patient.get('visits', [])
        scales = patient.get('scales', [])

        paired_count = min(len(visits), len(scales))
        if paired_count == 0:
            continue

        for visit, scale in zip(visits[:paired_count], scales[:paired_count]):
            turns = visit.get('dialogue', {}).get('turns', [])
            gad = scale.get('GAD-7', {}).get('total')
            phq = scale.get('PHQ-9', {}).get('total')

            if not isinstance(gad, (int, float)) or not isinstance(phq, (int, float)):
                continue

            full_text = extract_role_text(turns, target_role)
            early_text = extract_early_role_text(
                turns,
                target_role=target_role,
                max_role_turns=max_role_turns,
                max_chars=max_chars,
            )

            if len(early_text) < min_chars:
                continue

            anxiety_label = 1 if gad >= 10 else 0
            depression_label = 1 if phq >= 10 else 0
            combined_label = 1 if (anxiety_label == 1 or depression_label == 1) else 0

            results.append({
                'patient_id': patient_id,
                'visit_id': visit.get('visit_id'),
                'dataset': dataset_name,
                'role': target_role,
                'full_text': full_text,
                'early_text': early_text,
                'gad_score': float(gad),
                'phq_score': float(phq),
                'anxiety_label': anxiety_label,
                'depression_label': depression_label,
                'combined_label': combined_label,
                'target_role_turns': sum(1 for turn in turns if turn.get('role') == target_role),
                'dialogue_turns': len(turns),
            })

    return results


def create_train_val_test_split(data, train_ratio=0.7, val_ratio=0.15):
    """按患者划分训练/验证/测试集，避免同一患者泄漏到多个split。"""
    np.random.seed(42)
    patient_ids = sorted({item['patient_id'] for item in data})
    shuffled_patient_ids = np.random.permutation(patient_ids)

    n_patients = len(shuffled_patient_ids)
    train_end = int(n_patients * train_ratio)
    val_end = int(n_patients * (train_ratio + val_ratio))

    train_patients = set(shuffled_patient_ids[:train_end])
    val_patients = set(shuffled_patient_ids[train_end:val_end])
    test_patients = set(shuffled_patient_ids[val_end:])

    return {
        'train': [item for item in data if item['patient_id'] in train_patients],
        'val': [item for item in data if item['patient_id'] in val_patients],
        'test': [item for item in data if item['patient_id'] in test_patients],
    }


def analyze_data_distribution(data):
    """分析数据分布"""
    print("\n=== 数据分布分析 ===")
    print(f"总样本数(visit): {len(data)}")
    print(f"患者数: {len({d['patient_id'] for d in data})}")
    
    anxiety_dist = [d['anxiety_label'] for d in data]
    depression_dist = [d['depression_label'] for d in data]
    combined_dist = [d['combined_label'] for d in data]
    
    print(f"\n焦虑分布 (GAD-7≥10):")
    print(f"  轻度/无 (0): {sum(1 for x in anxiety_dist if x==0)} ({sum(1 for x in anxiety_dist if x==0)/len(data)*100:.1f}%)")
    print(f"  中重度 (1): {sum(1 for x in anxiety_dist if x==1)} ({sum(1 for x in anxiety_dist if x==1)/len(data)*100:.1f}%)")
    
    print(f"\n抑郁分布 (PHQ-9≥10):")
    print(f"  轻度/无 (0): {sum(1 for x in depression_dist if x==0)} ({sum(1 for x in depression_dist if x==0)/len(data)*100:.1f}%)")
    print(f"  中重度 (1): {sum(1 for x in depression_dist if x==1)} ({sum(1 for x in depression_dist if x==1)/len(data)*100:.1f}%)")
    
    print(f"\n综合情绪障碍 (焦虑或抑郁≥10):")
    print(f"  正常 (0): {sum(1 for x in combined_dist if x==0)} ({sum(1 for x in combined_dist if x==0)/len(data)*100:.1f}%)")
    print(f"  中重度 (1): {sum(1 for x in combined_dist if x==1)} ({sum(1 for x in combined_dist if x==1)/len(data)*100:.1f}%)")
    
    print(f"\n量表分数统计:")
    gad_scores = [d['gad_score'] for d in data]
    phq_scores = [d['phq_score'] for d in data]
    print(f"  GAD-7: 均值={np.mean(gad_scores):.1f}, 标准差={np.std(gad_scores):.1f}")
    print(f"  PHQ-9: 均值={np.mean(phq_scores):.1f}, 标准差={np.std(phq_scores):.1f}")
    early_lengths = [len(d['early_text']) for d in data]
    print(f"  early_text长度: 均值={np.mean(early_lengths):.1f}, 中位数={np.median(early_lengths):.1f}")


def main():
    args = parse_args()
    print("="*60)
    print("Step 1: 数据准备")
    print("="*60)
    
    # 加载数据
    print("\n加载数据集...")
    datasets = load_datasets(args.input_dir)
    
    # 提取visit级别数据
    print("提取visit级别数据...")
    dataset_results = {}
    for dataset_name in args.datasets:
        dataset_results[dataset_name] = extract_visit_data(
            datasets[dataset_name],
            dataset_name,
            min_chars=args.min_chars,
            max_role_turns=args.max_role_turns,
            max_chars=args.max_chars,
        )

    # 合并数据
    all_data = []
    for dataset_name in args.datasets:
        all_data.extend(dataset_results[dataset_name])
    print(f"总样本数(visit): {len(all_data)}")
    for dataset_name in args.datasets:
        print(f"  - {dataset_name}: {len(dataset_results[dataset_name])}")
    
    # 分析分布
    analyze_data_distribution(all_data)
    
    # 创建训练/验证/测试集
    print("\n创建训练/验证/测试集...")
    splits = create_train_val_test_split(all_data)
    
    for split_name, split_data in splits.items():
        print(f"  {split_name}: {len(split_data)} 样本")
    
    # 保存数据
    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存完整数据
    with open(f'{output_dir}/all_data.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)

    metadata = {
        'analysis_unit': 'visit',
        'early_text_definition': {
            'target_role_only': True,
            'max_role_turns': args.max_role_turns,
            'max_chars': args.max_chars,
            'min_chars': args.min_chars,
        },
        'datasets': args.datasets,
        'split_strategy': 'patient_level',
        'sample_count': len(all_data),
        'patient_count': len({item['patient_id'] for item in all_data}),
    }
    with open(f'{output_dir}/metadata.json', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    
    # 保存分割数据
    for split_name, split_data in splits.items():
        # 保存为JSON (文本形式)
        with open(f'{output_dir}/{split_name}.json', 'w', encoding='utf-8') as f:
            json.dump(split_data, f, ensure_ascii=False, indent=2)
    
    # 保存CSV格式 (便于模型读取)
    for split_name, split_data in splits.items():
        with open(f'{output_dir}/{split_name}.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['patient_id', 'visit_id', 'text', 'gad_score', 'phq_score', 'anxiety_label', 'depression_label', 'combined_label', 'dataset'])
            for item in split_data:
                writer.writerow([
                    item['patient_id'],
                    item['visit_id'],
                    item['early_text'][:500],  # 限制文本长度
                    item['gad_score'],
                    item['phq_score'],
                    item['anxiety_label'],
                    item['depression_label'],
                    item['combined_label'],
                    item['dataset']
                ])
    
    print(f"\n数据已保存到: {output_dir}/")
    print("  - metadata.json")
    print("  - all_data.json")
    print("  - train.json / train.csv")
    print("  - val.json / val.csv")
    print("  - test.json / test.csv")
    
    return all_data, splits


if __name__ == '__main__':
    main()
