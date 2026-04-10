#!/usr/bin/env python3
"""
Pearson与Spearman相关性对比柱状图
"""

import json
import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False


def load_data():
    with open('results/summary_latest.json', encoding='utf-8') as f:
        data = json.load(f)
    return data


def create_correlation_comparison(data):
    """创建Pearson与Spearman对比图"""
    
    fig, axes = plt.subplots(1, 2, figsize=(16, 8))
    
    for idx, ds_name in enumerate(['with_caregiver', 'without_caregiver']):
        ax = axes[idx]
        
        corr = data['correlations'][ds_name]['correlations']
        raw = data['correlations'][ds_name]['raw_data']
        
        # 重点特征列表
        features = ['negemo', 'posemo', 'anx', 'sad', 'discrep', 
                    'negate', 'tentat', 'certain', 'PastM', 'affect_ratio']
        
        gad_corr = corr.get('GAD-7_mean', {})
        
        pearson_rs = []
        spearman_rs = []
        pearson_ps = []
        spearman_ps = []
        feature_labels = []
        
        for feat in features:
            if feat in gad_corr:
                pr = gad_corr[feat].get('pearson_r', 0) or 0
                pp = gad_corr[feat].get('pearson_p', 1) or 1
                sr = gad_corr[feat].get('spearman_r')
                sp = gad_corr[feat].get('spearman_p')
                
                if sr is not None:
                    pearson_rs.append(pr)
                    spearman_rs.append(sr)
                    pearson_ps.append(pp)
                    spearman_ps.append(sp if sp is not None else 1)
                    feature_labels.append(feat)
        
        x = np.arange(len(feature_labels))
        width = 0.35
        
        bars1 = ax.bar(x - width/2, pearson_rs, width, label='Pearson r', color='#E74C3C', alpha=0.8)
        bars2 = ax.bar(x + width/2, spearman_rs, width, label='Spearman r', color='#3498DB', alpha=0.8)
        
        # 添加显著性标记
        for i, (pp, sp) in enumerate(zip(pearson_ps, spearman_ps)):
            p_sig = '*' if pp < 0.05 else ('+' if pp < 0.1 else '')
            s_sig = '*' if sp < 0.05 else ('+' if sp < 0.1 else '')
            
            # Pearson显著性
            if pearson_rs[i] >= 0:
                ax.annotate(p_sig, xy=(x[i] - width/2, pearson_rs[i] + 0.02), 
                           ha='center', fontsize=10, color='#E74C3C', fontweight='bold')
            else:
                ax.annotate(p_sig, xy=(x[i] - width/2, pearson_rs[i] - 0.05), 
                           ha='center', fontsize=10, color='#E74C3C', fontweight='bold')
            
            # Spearman显著性
            if spearman_rs[i] >= 0:
                ax.annotate(s_sig, xy=(x[i] + width/2, spearman_rs[i] + 0.02), 
                           ha='center', fontsize=10, color='#3498DB', fontweight='bold')
            else:
                ax.annotate(s_sig, xy=(x[i] + width/2, spearman_rs[i] - 0.05), 
                           ha='center', fontsize=10, color='#3498DB', fontweight='bold')
        
        ax.set_ylabel('Correlation Coefficient (r)', fontsize=12)
        ax.set_xlabel('LIWC Features', fontsize=12)
        ax.set_title(f'{ds_name}\nGAD-7 Pearson vs Spearman 相关性对比', fontweight='bold', fontsize=14)
        ax.set_xticks(x)
        ax.set_xticklabels(feature_labels, rotation=45, ha='right', fontsize=10)
        ax.legend(loc='upper right')
        ax.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
        ax.grid(True, alpha=0.3, axis='y')
        
        # 添加图例说明
        ax.text(0.02, 0.98, '* = p<0.05\n+ = p<0.1', transform=ax.transAxes, 
                fontsize=9, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
    
    plt.suptitle('Pearson与Spearman相关性方法对比\n(*=p<0.05, +=p<0.1)', 
                fontsize=16, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig('results/figures/pearson_spearman_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    print('Pearson vs Spearman对比图已保存: results/figures/pearson_spearman_comparison.png')


def main():
    print('加载数据...')
    data = load_data()
    
    print('生成对比图...')
    create_correlation_comparison(data)
    
    print('完成!')


if __name__ == '__main__':
    main()
