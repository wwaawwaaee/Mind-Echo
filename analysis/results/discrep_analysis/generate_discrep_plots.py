#!/usr/bin/env python3
"""
Generate discrep factor correlation analysis plots
"""

import json
import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['Arial', 'DejaVu Sans']
matplotlib.rcParams['axes.unicode_minus'] = False


def load_raw_data():
    with open('../summary_latest.json', encoding='utf-8') as f:
        data = json.load(f)
    return data


def create_discrep_scatter(data, output_dir):
    """Create discrep scatter plots"""
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    
    colors = {'with_caregiver': '#3498db', 'without_caregiver': '#e74c3c'}
    labels = {'with_caregiver': 'With Caregiver (Pediatric)', 'without_caregiver': 'Without Caregiver (Adult)'}
    
    for idx, ds_name in enumerate(['with_caregiver', 'without_caregiver']):
        ax = axes[idx]
        
        gad_raw = data['correlations'][ds_name]['raw_data'].get('GAD-7_mean', {})
        phq_raw = data['correlations'][ds_name]['raw_data'].get('PHQ-9_mean', {})
        
        gad_vals = gad_raw.get('scale_values', [])
        phq_vals = phq_raw.get('scale_values', [])
        discrep_gad = gad_raw.get('feature_data', {}).get('discrep', [])
        discrep_phq = phq_raw.get('feature_data', {}).get('discrep', [])
        
        color = colors[ds_name]
        
        if len(gad_vals) >= 3:
            ax.scatter(gad_vals, discrep_gad, c=color, alpha=0.7, s=60, 
                      label='GAD-7', edgecolors='white', linewidth=0.5)
            r, p = stats.spearmanr(gad_vals, discrep_gad)
            ax.annotate(f'GAD-7: rho={r:.3f}, p={p:.4f}', 
                        xy=(0.05, 0.95), xycoords='axes fraction',
                        fontsize=10, color=color, fontweight='bold')
        
        if len(phq_vals) >= 3:
            ax.scatter(phq_vals, discrep_phq, c='orange', alpha=0.7, s=60, 
                      label='PHQ-9', edgecolors='white', linewidth=0.5, marker='s')
            r2, p2 = stats.spearmanr(phq_vals, discrep_phq)
            ax.annotate(f'PHQ-9: rho={r2:.3f}, p={p2:.4f}', 
                        xy=(0.05, 0.85), xycoords='axes fraction',
                        fontsize=10, color='orange', fontweight='bold')
        
        ax.set_xlabel('Scale Score', fontsize=12)
        ax.set_ylabel('Discrep Density (Difference/Counterfactual Words)', fontsize=11)
        ax.set_title(f'{labels[ds_name]}', fontsize=13, fontweight='bold')
        ax.legend(loc='upper right')
        ax.grid(True, alpha=0.3)
    
    fig.suptitle('Discrep vs. Anxiety/Depression Scales: Scatter Plot (Spearman Correlation)', fontsize=14, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.96])
    
    fig.text(0.5, 0.01, 
             'GAD-7: Generalized Anxiety Disorder 7-item scale  |  PHQ-9: Patient Health Questionnaire-9  |  Discrep: Difference/Counterfactual word density',
             ha='center', fontsize=9, style='italic', wrap=True)
    
    plt.savefig(f'{output_dir}/discrep_scatter.png', dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'Scatter plot saved: {output_dir}/discrep_scatter.png')


def create_discrep_comparison(data, output_dir):
    """Create discrep comparison bar chart"""
    fig, ax = plt.subplots(figsize=(10, 6))
    
    datasets = ['with_caregiver', 'without_caregiver']
    scales = ['GAD-7', 'PHQ-9']
    
    x = np.arange(len(datasets))
    width = 0.35
    
    r_values = []
    p_values = []
    
    for scale in scales:
        rs = []
        ps = []
        for ds in datasets:
            corr = data['correlations'][ds]['correlations'].get(f'{scale}_mean', {}).get('discrep', {})
            rs.append(corr.get('spearman_r', 0) or 0)
            ps.append(corr.get('spearman_p', 1) or 1)
        r_values.append(rs)
        p_values.append(ps)
    
    bars1 = ax.bar(x - width/2, r_values[0], width, label='GAD-7', color='#3498db')
    bars2 = ax.bar(x + width/2, r_values[1], width, label='PHQ-9', color='#e74c3c')
    
    for i, (rs, ps) in enumerate(zip(r_values, p_values)):
        for j, (r, p) in enumerate(zip(rs, ps)):
            sig = '*' if p < 0.05 else ('+' if p < 0.1 else '')
            y_offset = 0.02 if r >= 0 else -0.05
            ax.annotate(f'{r:.3f}{sig}', 
                       xy=(x[j] + (width/2 if i == 1 else -width/2), r + y_offset),
                       ha='center', va='bottom' if r >= 0 else 'top', fontsize=11, fontweight='bold')
    
    ax.set_ylabel('Spearman rho', fontsize=12)
    ax.set_title('Discrep Correlation with GAD-7 and PHQ-9 (Spearman, *=p<0.05, +=p<0.1)', fontsize=13, fontweight='bold')
    ax.set_xticks(x)
    ax.set_xticklabels(['With Caregiver\n(Pediatric)', 'Without Caregiver\n(Adult)'])
    ax.legend(loc='upper right')
    ax.axhline(y=0, color='gray', linestyle='-', linewidth=0.5)
    ax.grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    fig.text(0.5, 0.01, 
             'GAD-7: Generalized Anxiety Disorder 7-item scale  |  PHQ-9: Patient Health Questionnaire-9  |  Discrep: Difference/Counterfactual word density',
             ha='center', fontsize=9, style='italic')
    
    plt.savefig(f'{output_dir}/discrep_comparison.png', dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()
    print(f'Comparison plot saved: {output_dir}/discrep_comparison.png')


def create_discrep_heatmap(data, output_dir):
    """Create heatmap with discrep features - split into two separate figures"""
    scales = ['GAD-7_mean', 'PHQ-9_mean']
    features = ['discrep', 'posemo', 'negemo', 'anx', 'sad', 'insight', 'cause', 
                'tentat', 'certain', 'feel', 'see', 'hear', 'pronoun_density', 'health']
    
    feature_labels = [
        'discrep', 'posemo', 'negemo', 'anx', 'sad', 'insight', 'cause',
        'tentat', 'certain', 'feel', 'see', 'hear', 'pronoun', 'health'
    ]
    
    feature_defs = (
        "Feature Definitions: discrep=Difference words | posemo=Positive emotion | negemo=Negative emotion | "
        "anx=Anxiety | sad=Sadness | insight=Insight | cause=Causal | tentat=Tentative | "
        "certain=Certainty | feel=Feeling | see=See | hear=Hear | pronoun=Pronoun density | health=Health-related words"
    )
    
    datasets_info = {
        'with_caregiver': {'title': 'Figure 1: LIWC characteristics and Spearman correlation analysis between GAD-7 and PHQ-9', 'filename': 'discrep_heatmap_caregiver.png'},
        'without_caregiver': {'title': 'Figure 2: LIWC characteristics and Spearman correlation analysis between GAD-7 and PHQ-9', 'filename': 'discrep_heatmap_no_caregiver.png'}
    }
    
    for ds_name, info in datasets_info.items():
        fig, ax = plt.subplots(figsize=(12.5, 5.8))
        
        corr_matrix = []
        sig_matrix = []
        p_matrix = []
        for scale in scales:
            row = []
            sig_row = []
            p_row = []
            for feat in features:
                corr = data['correlations'][ds_name]['correlations'].get(scale, {}).get(feat, {})
                r = corr.get('spearman_r', 0) or 0
                p = corr.get('spearman_p', 1) or 1
                row.append(r)
                sig_row.append(p < 0.05)
                p_row.append(p)
            corr_matrix.append(row)
            sig_matrix.append(sig_row)
            p_matrix.append(p_row)
        
        im = ax.imshow(corr_matrix, cmap='RdBu_r', aspect='auto', vmin=-0.6, vmax=0.6)
        
        ax.set_xticks(range(len(features)))
        ax.set_xticklabels(feature_labels, rotation=45, ha='right', fontsize=10)
        ax.set_yticks(range(len(scales)))
        ax.set_yticklabels([s.replace('_mean', '') for s in scales], fontsize=12)
        
        ax.set_title(info['title'], fontsize=14, fontweight='bold', pad=10)
        
        for i in range(len(scales)):
            for j in range(len(features)):
                val = corr_matrix[i][j]
                p_val = p_matrix[i][j]
                is_sig = sig_matrix[i][j]
                color = 'white' if abs(val) > 0.35 else 'black'
                marker = '*' if is_sig else ''
                ax.text(
                    j,
                    i,
                    f'{val:.2f}{marker}\n(p={p_val:.3f})',
                    ha='center',
                    va='center',
                    color=color,
                    fontsize=9,
                    fontweight='bold' if is_sig else 'normal',
                    linespacing=1.15,
                )
        
        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label('Spearman rho', fontsize=11)
        
        plt.tight_layout()
        fig.text(0.5, 0.02, feature_defs, ha='center', fontsize=8, style='italic',
                 bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8, edgecolor='gray'))
        fig.text(0.5, 0.01, '', ha='center', fontsize=9)
        
        plt.savefig(f'{output_dir}/{info["filename"]}', dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f'Heatmap saved: {output_dir}/{info["filename"]}')


def create_negative_factor_heatmap(data, output_dir):
    """Create split heatmaps for negative-correlation candidate factors."""
    scales = ['GAD-7_mean', 'PHQ-9_mean']
    factor_names = ['time', 'PastM', 'funct', 'space', 'feel', 'pronoun_density']
    factor_labels = ['time', 'PastM', 'funct', 'space', 'feel', 'pronoun\ndensity']

    factor_defs = (
        "Factor Definitions: time=temporal words | PastM=past-tense markers | funct=function words | "
        "space=spatial words | feel=feeling/perception words | pronoun_density=pronoun density"
    )

    datasets_info = {
        'with_caregiver': {
            'title': 'Figure 2: Negative-correlation candidate factors and Spearman analysis between GAD-7 and PHQ-9',
            'filename': 'negative_factor_heatmap_caregiver.png'
        },
        'without_caregiver': {
            'title': 'Figure 4: Negative-correlation candidate factors and Spearman analysis between GAD-7 and PHQ-9',
            'filename': 'negative_factor_heatmap_no_caregiver.png'
        }
    }

    for ds_name, info in datasets_info.items():
        fig, ax = plt.subplots(figsize=(9.4, 5.8))

        corr_matrix = []
        sig_matrix = []
        p_matrix = []

        for scale in scales:
            scale_corrs = data['correlations'][ds_name]['correlations'].get(scale, {})

            row = []
            sig_row = []
            p_row = []

            for factor_name in factor_names:
                corr = scale_corrs.get(factor_name, {})
                r = corr.get('spearman_r', 0) or 0
                p = corr.get('spearman_p', 1) or 1
                row.append(float(r))
                sig_row.append(p < 0.05)
                p_row.append(float(p))

            corr_matrix.append(row)
            sig_matrix.append(sig_row)
            p_matrix.append(p_row)

        im = ax.imshow(corr_matrix, cmap='RdBu_r', aspect='auto', vmin=-0.6, vmax=0.6)

        ax.set_xticks(range(len(factor_labels)))
        ax.set_xticklabels(factor_labels, fontsize=11)
        ax.set_yticks(range(len(scales)))
        ax.set_yticklabels([s.replace('_mean', '') for s in scales], fontsize=12)
        ax.set_title(info['title'], fontsize=13, fontweight='bold', pad=10, loc='center')

        for i in range(len(scales)):
            for j in range(len(factor_labels)):
                val = corr_matrix[i][j]
                p_val = p_matrix[i][j]
                is_sig = sig_matrix[i][j]
                color = 'white' if abs(val) > 0.35 else 'black'
                marker = '*' if is_sig else ''
                ax.text(
                    j,
                    i,
                    f'{val:.2f}{marker}\n(p={p_val:.3f})',
                    ha='center',
                    va='center',
                    color=color,
                    fontsize=9,
                    fontweight='bold' if is_sig else 'normal',
                    linespacing=1.15,
                )

        cbar = plt.colorbar(im, ax=ax, shrink=0.8)
        cbar.set_label('Spearman rho', fontsize=11)

        plt.tight_layout()
        fig.text(0.5, 0.02, factor_defs, ha='center', fontsize=8, style='italic',
                 bbox=dict(boxstyle='round', facecolor='lightyellow', alpha=0.8, edgecolor='gray'))

        plt.savefig(f'{output_dir}/{info["filename"]}', dpi=150, bbox_inches='tight', facecolor='white')
        plt.close()
        print(f'Heatmap saved: {output_dir}/{info["filename"]}')


def main():
    output_dir = '.'
    
    print('Loading data...')
    data = load_raw_data()
    
    print('Generating scatter plot...')
    create_discrep_scatter(data, output_dir)
    
    print('Generating comparison plot...')
    create_discrep_comparison(data, output_dir)
    
    print('Generating heatmap...')
    create_discrep_heatmap(data, output_dir)

    print('Generating negative-factor heatmap...')
    create_negative_factor_heatmap(data, output_dir)

    print('\nAll plots generated!')


if __name__ == '__main__':
    main()
