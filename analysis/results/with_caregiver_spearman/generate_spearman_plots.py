#!/usr/bin/env python3
"""
生成 with_caregiver Spearman显著相关分析图
显著特征: number, negate, affect_ratio (Spearman p<0.05)
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False


def load_data():
    with open('../summary_latest.json', encoding='utf-8') as f:
        data = json.load(f)
    return data


def create_spearman_scatter(data, output_dir):
    """创建显著特征散点图"""
    sig_features = ['number', 'negate', 'affect_ratio']
    feature_labels = {
        'number': 'number (数量词)',
        'negate': 'negate (否定词)', 
        'affect_ratio': 'affect_ratio (情感比率)'
    }
    
    fig, axes = plt.subplots(1, 3, figsize=(16, 5))
    
    raw = data['correlations']['with_caregiver']['raw_data']
    gad_vals = raw.get('GAD-7_mean', {}).get('scale_values', [])
    
    for idx, feat in enumerate(sig_features):
        ax = axes[idx]
        
        feat_data = raw.get('GAD-7_mean', {}).get('feature_data', {}).get(feat, [])
        
        if len(gad_vals) >= 3 and len(feat_data) >= 3:
            ax.scatter(gad_vals, feat_data, c='#3498db', alpha=0.7, s=60, 
                      edgecolors='white', linewidth=0.5)
            
            # Pearson
            pr, pp = stats.pearsonr(gad_vals, feat_data)
            # Spearman  
            sr, sp = stats.spearmanr(gad_vals, feat_data)
            
            # Regression line
            slope, intercept, _, _, _ = stats.linregress(gad_vals, feat_data)
            x_line = np.linspace(min(gad_vals), max(gad_vals), 100)
            y_line = slope * x_line + intercept
            ax.plot(x_line, y_line, color='#3498db', linestyle='--', linewidth=2, alpha=0.8)
            
            sig_mark = '*' if sp < 0.05 else ('+' if sp < 0.1 else '')
            ax.annotate(f'Pearson r={pr:.3f}, p={pp:.3f}\nSpearman r={sr:.3f}, p={sp:.4f}{sig_mark}', 
                        xy=(0.05, 0.95), xycoords='axes fraction',
                        fontsize=10, fontweight='bold', verticalalignment='top',
                        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
        
        ax.set_xlabel('GAD-7 量表得分', fontsize=11)
        ax.set_ylabel(f'{feature_labels[feat]}', fontsize=11)
        ax.set_title(f'{feature_labels[feat]}', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)
    
    plt.suptitle('with_caregiver: Spearman显著相关特征 (GAD-7)\n(*=p<0.05)', fontsize=14, fontweight='bold')
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])
    plt.savefig(f'{output_dir}/spearman_scatter.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'散点图已保存: {output_dir}/spearman_scatter.png')


def create_pearson_vs_spearman(data, output_dir):
    """对比Pearson和Spearman相关性"""
    features = ['number', 'negate', 'affect_ratio', 'time', 'PastM', 'see']
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    raw = data['correlations']['with_caregiver']['raw_data']
    corr = data['correlations']['with_caregiver']['correlations']
    gad_vals = raw.get('GAD-7_mean', {}).get('scale_values', [])
    
    x = np.arange(len(features))
    width = 0.35
    
    pearson_rs = []
    spearman_rs = []
    pearson_ps = []
    spearman_ps = []
    
    for feat in features:
        feat_data = raw.get('GAD-7_mean', {}).get('feature_data', {}).get(feat, [])
        if len(gad_vals) >= 3 and len(feat_data) >= 3:
            pr, pp = stats.pearsonr(gad_vals, feat_data)
            sr, sp = stats.spearmanr(gad_vals, feat_data)
        else:
            pr, pp = 0, 1
            sr, sp = 0, 1
        pearson_rs.append(pr)
        spearman_rs.append(sr)
        pearson_ps.append(pp)
        spearman_ps.append(sp)
    
    bars1 = ax.bar(x - width/2, pearson_rs, width, label='Pearson r', color='#e74c3c', alpha=0.8)
    bars2 = ax.bar(x + width/2, spearman_rs, width, label='Spearman r', color='#3498db', alpha=0.8)
    
    # Add significance markers
    for i, (pp, sp) in enumerate(zip(pearson_ps, spearman_ps)):
        p_sig = '*' if pp < 0.05 else ('+' if pp < 0.1 else '')
        s_sig = '*' if sp < 0.05 else ('+' if sp < 0.1 else '')
        ax.annotate(f'{p_sig}', xy=(x[i] - width/2, pearson_rs[i]), 
                   ha='center', va='bottom', fontsize=10, color='#e74c3c')
        ax.annotate(f'{s_sig}', xy=(x[i] + width/2, spearman_rs[i]), 
                   ha='center', va='bottom', fontsize=10, color='#3498db', fontweight='bold')
    
    ax.set_ylabel('相关系数 r', fontsize=12)
    ax.set_title('with_caregiver: Pearson vs Spearman 相关性对比 (GAD-7)\n(*=p<0.05, +=p<0.1)', 
                fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(features, rotation=30, ha='right')
    ax.legend()
    ax.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/pearson_vs_spearman.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'对比图已保存: {output_dir}/pearson_vs_spearman.png')


def create_summary_table(data, output_dir):
    """创建显著性汇总表"""
    fig, ax = plt.subplots(figsize=(12, 4))
    ax.axis('off')
    
    features = ['number', 'negate', 'affect_ratio', 'time', 'PastM', 'see']
    feature_labels = {
        'number': 'number\n(数量词)',
        'negate': 'negate\n(否定词)', 
        'affect_ratio': 'affect_ratio\n(情感比率)',
        'time': 'time\n(时间词)',
        'PastM': 'PastM\n(过去时)',
        'see': 'see\n(视觉词)'
    }
    
    raw = data['correlations']['with_caregiver']['raw_data']
    gad_vals = raw.get('GAD-7_mean', {}).get('scale_values', [])
    
    table_data = [['特征', 'Pearson r', 'Pearson p', 'Spearman r', 'Spearman p', '显著性']]
    
    for feat in features:
        feat_data = raw.get('GAD-7_mean', {}).get('feature_data', {}).get(feat, [])
        if len(gad_vals) >= 3 and len(feat_data) >= 3:
            pr, pp = stats.pearsonr(gad_vals, feat_data)
            sr, sp = stats.spearmanr(gad_vals, feat_data)
        else:
            pr, pp, sr, sp = 0, 1, 0, 1
        
        sig = 'Spearman*' if sp < 0.05 else ('Pearson*' if pp < 0.05 else ('边缘显著+' if sp < 0.1 or pp < 0.1 else '不显著'))
        
        table_data.append([
            feature_labels[feat],
            f'{pr:.3f}',
            f'{pp:.4f}',
            f'{sr:.3f}',
            f'{sp:.4f}',
            sig
        ])
    
    table = ax.table(cellText=table_data, loc='center', cellLoc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 2.0)
    
    # Header styling
    for i in range(len(table_data[0])):
        table[(0, i)].set_facecolor('#4472C4')
        table[(0, i)].set_text_props(color='white', fontweight='bold')
    
    # Highlight significant rows
    for i in range(1, len(table_data)):
        if 'Spearman*' in table_data[i][5]:
            for j in range(len(table_data[0])):
                table[(i, j)].set_facecolor('#d4edda')
    
    plt.title('with_caregiver: GAD-7 相关性显著性汇总', fontsize=14, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(f'{output_dir}/significance_summary.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f'汇总表已保存: {output_dir}/significance_summary.png')


def main():
    output_dir = '.'
    
    print('加载数据...')
    data = load_data()
    
    print('生成Spearman散点图...')
    create_spearman_scatter(data, output_dir)
    
    print('生成Pearson vs Spearman对比图...')
    create_pearson_vs_spearman(data, output_dir)
    
    print('生成显著性汇总表...')
    create_summary_table(data, output_dir)
    
    print('\n所有图表生成完成!')


if __name__ == '__main__':
    main()
