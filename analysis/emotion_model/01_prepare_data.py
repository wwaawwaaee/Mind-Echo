#!/usr/bin/env python3
"""
数据准备脚本
从Mind-Echo数据集构建训练数据
"""

import json
import os
import numpy as np
from pathlib import Path
import jieba


def load_datasets():
    """加载数据集"""
    base_path = '../../processed_dataset/output'
    
    with open(f'{base_path}/anonymized_dataset_with_caregiver.json', encoding='utf-8') as f:
        with_caregiver = json.load(f)
    
    with open(f'{base_path}/anonymized_dataset_without_caregiver.json', encoding='utf-8') as f:
        without_caregiver = json.load(f)
    
    return with_caregiver, without_caregiver


def extract_early_turns_text(turns, max_turns=15, max_chars=2000):
    """
    提取早期话轮文本
    - max_turns: 最多话轮数
    - max_chars: 最大字符数
    """
    text_parts = []
    char_count = 0
    
    for turn in turns[:max_turns]:
        text = turn.get('text', '').strip()
        if text and char_count + len(text) <= max_chars:
            text_parts.append(f"{turn.get('role', 'unknown')}: {text}")
            char_count += len(text)
    
    return '\n'.join(text_parts)


def extract_patient_data(dataset, dataset_name):
    """提取患者级别数据"""
    results = []
    
    target_role = 'caregiver' if dataset_name == 'with_caregiver' else 'patient'
    
    for patient in dataset.get('patients', []):
        patient_id = patient.get('patient_id')
        visits = patient.get('visits', [])
        scales = patient.get('scales', [])
        
        # 收集量表数据
        gad_scores = []
        phq_scores = []
        
        for scale in scales:
            gad = scale.get('GAD-7', {}).get('total')
            phq = scale.get('PHQ-9', {}).get('total')
            if isinstance(gad, (int, float)):
                gad_scores.append(gad)
            if isinstance(phq, (int, float)):
                phq_scores.append(phq)
        
        if not gad_scores or not phq_scores:
            continue
        
        gad_mean = np.mean(gad_scores)
        phq_mean = np.mean(phq_scores)
        
        # 提取对话文本
        all_text = []
        for visit in visits:
            turns = visit.get('dialogue', {}).get('turns', [])
            for turn in turns:
                if turn.get('role') == target_role:
                    text = turn.get('text', '').strip()
                    if text:
                        all_text.append(text)
        
        full_text = ' '.join(all_text)
        
        if len(full_text) < 50:  # 过滤太短的文本
            continue
        
        # 提取早期话轮 (前15个话轮，约1-2分钟)
        early_text = extract_early_turns_text(turns, max_turns=15)
        
        # 分类标签: 中重度焦虑/抑郁 (GAD-7/PHQ-9 >= 10)
        anxiety_label = 1 if gad_mean >= 10 else 0
        depression_label = 1 if phq_mean >= 10 else 0
        combined_label = 1 if (anxiety_label == 1 or depression_label == 1) else 0
        
        results.append({
            'patient_id': patient_id,
            'dataset': dataset_name,
            'role': target_role,
            'full_text': full_text,
            'early_text': early_text,
            'gad_score': gad_mean,
            'phq_score': phq_mean,
            'anxiety_label': anxiety_label,  # 0=轻度/无, 1=中重度
            'depression_label': depression_label,
            'combined_label': combined_label,  # 0=正常, 1=中重度焦虑或抑郁
            'n_visits': len(visits),
            'n_scales': len(gad_scores)
        })
    
    return results


def create_train_val_test_split(data, train_ratio=0.7, val_ratio=0.15):
    """创建训练/验证/测试集"""
    np.random.seed(42)
    n = len(data)
    indices = np.random.permutation(n)
    
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))
    
    train_indices = indices[:train_end]
    val_indices = indices[train_end:val_end]
    test_indices = indices[val_end:]
    
    return {
        'train': [data[i] for i in train_indices],
        'val': [data[i] for i in val_indices],
        'test': [data[i] for i in test_indices]
    }


def analyze_data_distribution(data):
    """分析数据分布"""
    print("\n=== 数据分布分析 ===")
    print(f"总样本数: {len(data)}")
    
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


def main():
    print("="*60)
    print("Step 1: 数据准备")
    print("="*60)
    
    # 加载数据
    print("\n加载数据集...")
    with_caregiver, without_caregiver = load_datasets()
    
    # 提取患者数据
    print("提取患者数据...")
    wc_data = extract_patient_data(with_caregiver, 'with_caregiver')
    woc_data = extract_patient_data(without_caregiver, 'without_caregiver')
    
    # 合并数据
    all_data = wc_data + woc_data
    print(f"总样本数: {len(all_data)}")
    print(f"  - with_caregiver: {len(wc_data)}")
    print(f"  - without_caregiver: {len(woc_data)}")
    
    # 分析分布
    analyze_data_distribution(all_data)
    
    # 创建训练/验证/测试集
    print("\n创建训练/验证/测试集...")
    splits = create_train_val_test_split(all_data)
    
    for split_name, split_data in splits.items():
        print(f"  {split_name}: {len(split_data)} 样本")
    
    # 保存数据
    output_dir = 'emotion_model/data'
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存完整数据
    with open(f'{output_dir}/all_data.json', 'w', encoding='utf-8') as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
    
    # 保存分割数据
    for split_name, split_data in splits.items():
        # 保存为JSON (文本形式)
        with open(f'{output_dir}/{split_name}.json', 'w', encoding='utf-8') as f:
            json.dump(split_data, f, ensure_ascii=False, indent=2)
    
    # 保存CSV格式 (便于模型读取)
    import csv
    for split_name, split_data in splits.items():
        with open(f'{output_dir}/{split_name}.csv', 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['text', 'gad_score', 'phq_score', 'anxiety_label', 'depression_label', 'combined_label', 'dataset'])
            for item in split_data:
                writer.writerow([
                    item['early_text'][:500],  # 限制文本长度
                    item['gad_score'],
                    item['phq_score'],
                    item['anxiety_label'],
                    item['depression_label'],
                    item['combined_label'],
                    item['dataset']
                ])
    
    print(f"\n数据已保存到: {output_dir}/")
    print("  - all_data.json")
    print("  - train.json / train.csv")
    print("  - val.json / val.csv")
    print("  - test.json / test.csv")
    
    return all_data, splits


if __name__ == '__main__':
    main()
