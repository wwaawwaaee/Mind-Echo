#!/usr/bin/env python3
"""
基础统计分析
1. 中重度焦虑/抑郁占比
2. GAD-7与PHQ-9相关性
"""

import json
import numpy as np
from scipy import stats
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False


# 焦虑/抑郁分级标准
GAD7_CUTOFFS = {
    'none': (0, 4),
    'mild': (5, 9),
    'moderate': (10, 14),
    'severe': (15, 21)
}

PHQ9_CUTOFFS = {
    'none': (0, 4),
    'mild': (5, 9),
    'moderate': (10, 14),
    'moderate_severe': (15, 19),
    'severe': (20, 27)
}


def load_datasets():
    base_path = '../processed_dataset/output'
    
    with open(f'{base_path}/anonymized_dataset_with_caregiver.json', encoding='utf-8') as f:
        with_caregiver = json.load(f)
    
    with open(f'{base_path}/anonymized_dataset_without_caregiver.json', encoding='utf-8') as f:
        without_caregiver = json.load(f)
    
    return with_caregiver, without_caregiver


def get_patient_scales(dataset):
    """提取患者级别的量表数据"""
    results = []
    for patient in dataset.get('patients', []):
        scales = patient.get('scales', [])
        
        gad_values = []
        phq_values = []
        
        for scale in scales:
            gad = scale.get('GAD-7', {}).get('total')
            phq = scale.get('PHQ-9', {}).get('total')
            if isinstance(gad, (int, float)):
                gad_values.append(gad)
            if isinstance(phq, (int, float)):
                phq_values.append(phq)
        
        if gad_values and phq_values:
            results.append({
                'patient_id': patient.get('patient_id'),
                'gad_mean': np.mean(gad_values),
                'phq_mean': np.mean(phq_values),
                'gad_latest': gad_values[-1],
                'phq_latest': phq_values[-1],
                'n_visits': len(gad_values)
            })
    
    return results


def calculate_severity_distribution(scales, cutoff_func, name):
    """计算严重程度分布"""
    levels = {'none': 0, 'mild': 0, 'moderate': 0, 'severe': 0, 'moderate_severe': 0}
    
    for patient in scales:
        score = patient.get(f'{name}_mean', 0)
        
        if 'none' in cutoff_func:
            none_min, none_max = cutoff_func['none']
            mild_min, mild_max = cutoff_func.get('mild', (5, 9))
            mod_min, mod_max = cutoff_func.get('moderate', (10, 14))
            sev_min, sev_max = cutoff_func.get('severe', (15, 21))
            
            if none_min <= score <= none_max:
                levels['none'] += 1
            elif mild_min <= score <= mild_max:
                levels['mild'] += 1
            elif mod_min <= score <= mod_max:
                levels['moderate'] += 1
            elif sev_min <= score <= sev_max:
                levels['severe'] += 1
        else:
            # For PHQ-9 which has 5 levels
            none_min, none_max = cutoff_func.get('none', (0, 4))
            mild_min, mild_max = cutoff_func.get('mild', (5, 9))
            mod_min, mod_max = cutoff_func.get('moderate', (10, 14))
            modsev_min, modsev_max = cutoff_func.get('moderate_severe', (15, 19))
            sev_min, sev_max = cutoff_func.get('severe', (20, 27))
            
            if none_min <= score <= none_max:
                levels['none'] += 1
            elif mild_min <= score <= mild_max:
                levels['mild'] += 1
            elif mod_min <= score <= mod_max:
                levels['moderate'] += 1
            elif modsev_min <= score <= modsev_max:
                levels['moderate_severe'] += 1
            elif sev_min <= score <= sev_max:
                levels['severe'] += 1
    
    total = len(scales)
    return {
        'levels': levels,
        'total': total,
        'percentages': {k: round(v/total*100, 1) if total > 0 else 0 for k, v in levels.items()}
    }


def calculate_correlation(gad_scores, phq_scores):
    """计算GAD-7与PHQ-9相关性"""
    if len(gad_scores) < 3:
        return None
    
    # Pearson
    pearson_r, pearson_p = stats.pearsonr(gad_scores, phq_scores)
    # Spearman
    spearman_r, spearman_p = stats.spearmanr(gad_scores, phq_scores)
    
    return {
        'pearson_r': round(pearson_r, 4),
        'pearson_p': round(pearson_p, 4),
        'spearman_r': round(spearman_r, 4),
        'spearman_p': round(spearman_p, 4),
        'n': len(gad_scores)
    }


def create_summary_table(with_caregiver_data, without_caregiver_data):
    """生成4个独立图表"""
    datasets = ['with_caregiver', 'without_caregiver']
    
    # 1. 焦虑严重程度分布
    fig1, ax1 = plt.subplots(figsize=(10, 6))
    levels = ['none', 'mild', 'moderate', 'severe']
    
    gad_dist = []
    for ds_name in datasets:
        ds = with_caregiver_data if ds_name == 'with_caregiver' else without_caregiver_data
        gad_dist.append([ds['gad_distribution']['percentages'].get(l, 0) for l in levels])
    
    x = np.arange(len(levels))
    width = 0.35
    ax1.bar(x - width/2, gad_dist[0], width, label='with_caregiver', color='#3498db')
    ax1.bar(x + width/2, gad_dist[1], width, label='without_caregiver', color='#e74c3c')
    ax1.set_ylabel('Percentage (%)', fontsize=12)
    ax1.set_title('1) GAD-7 焦虑严重程度分布', fontweight='bold', fontsize=14)
    ax1.set_xticks(x)
    ax1.set_xticklabels(['无(0-4)', '轻度(5-9)', '中度(10-14)', '重度(15-21)'])
    ax1.legend()
    ax1.grid(True, alpha=0.3, axis='y')
    
    for i in range(len(levels)):
        d1 = gad_dist[0][i]
        d2 = gad_dist[1][i]
        ax1.text(i - width/2, d1 + 1, f'{d1:.1f}%', ha='center', fontsize=9)
        ax1.text(i + width/2, d2 + 1, f'{d2:.1f}%', ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('1_gad_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'图表1已保存: 1_gad_distribution.png')
    
    # 2. 抑郁严重程度分布
    fig2, ax2 = plt.subplots(figsize=(10, 6))
    levels_p = ['none', 'mild', 'moderate', 'moderate_severe', 'severe']
    
    phq_dist = []
    for ds_name in datasets:
        ds = with_caregiver_data if ds_name == 'with_caregiver' else without_caregiver_data
        phq_dist.append([ds['phq_distribution']['percentages'].get(l, 0) for l in levels_p])
    
    x = np.arange(len(levels_p))
    width = 0.35
    ax2.bar(x - width/2, phq_dist[0], width, label='with_caregiver', color='#3498db')
    ax2.bar(x + width/2, phq_dist[1], width, label='without_caregiver', color='#e74c3c')
    ax2.set_ylabel('Percentage (%)', fontsize=12)
    ax2.set_title('2) PHQ-9 抑郁严重程度分布', fontweight='bold', fontsize=14)
    ax2.set_xticks(x)
    ax2.set_xticklabels(['无(0-4)', '轻度(5-9)', '中度(10-14)', '中重度(15-19)', '重度(20-27)'])
    ax2.legend()
    ax2.grid(True, alpha=0.3, axis='y')
    
    for i in range(len(levels_p)):
        d1 = phq_dist[0][i]
        d2 = phq_dist[1][i]
        ax2.text(i - width/2, d1 + 1, f'{d1:.1f}%', ha='center', fontsize=9)
        ax2.text(i + width/2, d2 + 1, f'{d2:.1f}%', ha='center', fontsize=9)
    
    plt.tight_layout()
    plt.savefig('2_phq_distribution.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'图表2已保存: 2_phq_distribution.png')
    
    # 3. GAD-7 vs PHQ-9 相关性散点图
    fig3, ax3 = plt.subplots(figsize=(10, 8))
    
    wc_gad = [p['gad_mean'] for p in with_caregiver_data['patients']]
    wc_phq = [p['phq_mean'] for p in with_caregiver_data['patients']]
    ax3.scatter(wc_gad, wc_phq, c='#3498db', alpha=0.6, s=60, label='with_caregiver')
    
    if len(wc_gad) >= 3:
        pr, pp = stats.pearsonr(wc_gad, wc_phq)
        slope, intercept, _, _, _ = stats.linregress(wc_gad, wc_phq)
        x_line = np.linspace(min(wc_gad), max(wc_gad), 100)
        y_line = slope * x_line + intercept
        ax3.plot(x_line, y_line, '--', color='#3498db', linewidth=2)
        ax3.annotate(f'with_caregiver\nr={pr:.3f}, p={pp:.4f}', xy=(0.05, 0.95), 
                    xycoords='axes fraction', fontsize=10, color='#3498db', fontweight='bold',
                    verticalalignment='top')
    
    woc_gad = [p['gad_mean'] for p in without_caregiver_data['patients']]
    woc_phq = [p['phq_mean'] for p in without_caregiver_data['patients']]
    ax3.scatter(woc_gad, woc_phq, c='#e74c3c', alpha=0.6, s=60, marker='s', label='without_caregiver')
    
    if len(woc_gad) >= 3:
        pr2, pp2 = stats.pearsonr(woc_gad, woc_phq)
        slope2, intercept2, _, _, _ = stats.linregress(woc_gad, woc_phq)
        x_line2 = np.linspace(min(woc_gad), max(woc_gad), 100)
        y_line2 = slope2 * x_line2 + intercept2
        ax3.plot(x_line2, y_line2, '--', color='#e74c3c', linewidth=2)
        ax3.annotate(f'without_caregiver\nr={pr2:.3f}, p={pp2:.4f}', xy=(0.05, 0.75), 
                    xycoords='axes fraction', fontsize=10, color='#e74c3c', fontweight='bold',
                    verticalalignment='top')
    
    ax3.set_xlabel('GAD-7 焦虑得分', fontsize=12)
    ax3.set_ylabel('PHQ-9 抑郁得分', fontsize=12)
    ax3.set_title('3) GAD-7 与 PHQ-9 相关性', fontweight='bold', fontsize=14)
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig('3_gad_phq_correlation.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'图表3已保存: 3_gad_phq_correlation.png')
    
    # 4. 数据汇总表
    fig4, ax4 = plt.subplots(figsize=(10, 8))
    ax4.axis('off')
    
    table_data = [
        ['指标', 'with_caregiver', 'without_caregiver'],
        ['样本数(患者)', str(with_caregiver_data['n']), str(without_caregiver_data['n'])],
        ['GAD-7 均值', f"{np.mean(wc_gad):.1f}", f"{np.mean(woc_gad):.1f}"],
        ['GAD-7 SD', f"{np.std(wc_gad):.1f}", f"{np.std(woc_gad):.1f}"],
        ['PHQ-9 均值', f"{np.mean(wc_phq):.1f}", f"{np.mean(woc_phq):.1f}"],
        ['PHQ-9 SD', f"{np.std(wc_phq):.1f}", f"{np.std(woc_phq):.1f}"],
        ['', '', ''],
        ['中重度焦虑占比', f"{with_caregiver_data['gad_moderate_severe_pct']:.1f}%", f"{without_caregiver_data['gad_moderate_severe_pct']:.1f}%"],
        ['中重度抑郁占比', f"{with_caregiver_data['phq_moderate_severe_pct']:.1f}%", f"{without_caregiver_data['phq_moderate_severe_pct']:.1f}%"],
        ['', '', ''],
        ['GAD-7 vs PHQ-9 Pearson r', f"{with_caregiver_data['gad_phq_corr']['pearson_r']:.3f}", f"{without_caregiver_data['gad_phq_corr']['pearson_r']:.3f}"],
        ['Pearson p', f"{with_caregiver_data['gad_phq_corr']['pearson_p']:.4f}", f"{without_caregiver_data['gad_phq_corr']['pearson_p']:.4f}"],
    ]
    
    table = ax4.table(cellText=table_data, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 2.0)
    
    for i in range(len(table_data[0])):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(color='white', fontweight='bold')
    
    ax4.set_title('4) 基础统计汇总', fontweight='bold', fontsize=14, pad=20)
    
    plt.tight_layout()
    plt.savefig('4_summary_table.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'图表4已保存: 4_summary_table.png')


def main():
    print("加载数据...")
    with_caregiver, without_caregiver = load_datasets()
    
    print("\n=== with_caregiver ===")
    wc_patients = get_patient_scales(with_caregiver)
    print(f"患者数: {len(wc_patients)}")
    
    # 严重程度分布
    wc_gad_dist = calculate_severity_distribution(wc_patients, GAD7_CUTOFFS, 'gad')
    wc_phq_dist = calculate_severity_distribution(wc_patients, PHQ9_CUTOFFS, 'phq')
    
    print(f"\nGAD-7 焦虑分布:")
    for level, pct in wc_gad_dist['percentages'].items():
        print(f"  {level}: {pct}%")
    
    print(f"\nPHQ-9 抑郁分布:")
    for level, pct in wc_phq_dist['percentages'].items():
        print(f"  {level}: {pct}%")
    
    # 中重度占比
    wc_gad_mod_sev = wc_gad_dist['percentages'].get('moderate', 0) + wc_gad_dist['percentages'].get('severe', 0)
    wc_phq_mod_sev = wc_phq_dist['percentages'].get('moderate', 0) + wc_phq_dist['percentages'].get('moderate_severe', 0) + wc_phq_dist['percentages'].get('severe', 0)
    print(f"\n中重度焦虑: {wc_gad_mod_sev}%")
    print(f"中重度抑郁: {wc_phq_mod_sev}%")
    
    # GAD-7 vs PHQ-9 相关性
    wc_gad_scores = [p['gad_mean'] for p in wc_patients]
    wc_phq_scores = [p['phq_mean'] for p in wc_patients]
    wc_corr = calculate_correlation(wc_gad_scores, wc_phq_scores)
    print(f"\nGAD-7 vs PHQ-9 相关性:")
    print(f"  Pearson r={wc_corr['pearson_r']}, p={wc_corr['pearson_p']}")
    print(f"  Spearman r={wc_corr['spearman_r']}, p={wc_corr['spearman_p']}")
    
    print("\n=== without_caregiver ===")
    woc_patients = get_patient_scales(without_caregiver)
    print(f"患者数: {len(woc_patients)}")
    
    woc_gad_dist = calculate_severity_distribution(woc_patients, GAD7_CUTOFFS, 'gad')
    woc_phq_dist = calculate_severity_distribution(woc_patients, PHQ9_CUTOFFS, 'phq')
    
    print(f"\nGAD-7 焦虑分布:")
    for level, pct in woc_gad_dist['percentages'].items():
        print(f"  {level}: {pct}%")
    
    print(f"\nPHQ-9 抑郁分布:")
    for level, pct in woc_phq_dist['percentages'].items():
        print(f"  {level}: {pct}%")
    
    woc_gad_mod_sev = woc_gad_dist['percentages'].get('moderate', 0) + woc_gad_dist['percentages'].get('severe', 0)
    woc_phq_mod_sev = woc_phq_dist['percentages'].get('moderate', 0) + woc_phq_dist['percentages'].get('moderate_severe', 0) + woc_phq_dist['percentages'].get('severe', 0)
    print(f"\n中重度焦虑: {woc_gad_mod_sev}%")
    print(f"中重度抑郁: {woc_phq_mod_sev}%")
    
    woc_gad_scores = [p['gad_mean'] for p in woc_patients]
    woc_phq_scores = [p['phq_mean'] for p in woc_patients]
    woc_corr = calculate_correlation(woc_gad_scores, woc_phq_scores)
    print(f"\nGAD-7 vs PHQ-9 相关性:")
    print(f"  Pearson r={woc_corr['pearson_r']}, p={woc_corr['pearson_p']}")
    print(f"  Spearman r={woc_corr['spearman_r']}, p={woc_corr['spearman_p']}")
    
    # 整理数据
    wc_data = {
        'patients': wc_patients,
        'n': len(wc_patients),
        'gad_distribution': wc_gad_dist,
        'phq_distribution': wc_phq_dist,
        'gad_moderate_severe_pct': wc_gad_mod_sev,
        'phq_moderate_severe_pct': wc_phq_mod_sev,
        'gad_phq_corr': wc_corr
    }
    
    woc_data = {
        'patients': woc_patients,
        'n': len(woc_patients),
        'gad_distribution': woc_gad_dist,
        'phq_distribution': woc_phq_dist,
        'gad_moderate_severe_pct': woc_gad_mod_sev,
        'phq_moderate_severe_pct': woc_phq_mod_sev,
        'gad_phq_corr': woc_corr
    }
    
    print("\n生成汇总图表...")
    create_summary_table(wc_data, woc_data)
    
    # 保存JSON
    output = {
        'with_caregiver': {
            'n_patients': len(wc_patients),
            'gad_distribution': wc_gad_dist,
            'phq_distribution': wc_phq_dist,
            'moderate_severe_anxiety_pct': wc_gad_mod_sev,
            'moderate_severe_depression_pct': wc_phq_mod_sev,
            'gad_phq_correlation': wc_corr
        },
        'without_caregiver': {
            'n_patients': len(woc_patients),
            'gad_distribution': woc_gad_dist,
            'phq_distribution': woc_phq_dist,
            'moderate_severe_anxiety_pct': woc_gad_mod_sev,
            'moderate_severe_depression_pct': woc_phq_mod_sev,
            'gad_phq_correlation': woc_corr
        }
    }
    
    with open('basic_statistics.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("JSON已保存: basic_statistics.json")
    
    print("\n完成!")


if __name__ == '__main__':
    main()
