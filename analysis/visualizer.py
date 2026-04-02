import matplotlib.pyplot as plt
import numpy as np
from scipy import stats
from typing import Dict, List, Any
import matplotlib
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
matplotlib.rcParams['axes.unicode_minus'] = False


class ScatterVisualizer:
    """散点图可视化器"""

    COLORS = {
        'with_caregiver': '#3498db',
        'without_caregiver': '#e74c3c'
    }

    LABELS = {
        'with_caregiver': 'with_caregiver (家长语言)',
        'without_caregiver': 'without_caregiver (患者语言)'
    }

    def __init__(self, figsize=(14, 10)):
        self.figsize = figsize
        self.feature_cols = ['negemo', 'posemo', 'anx']
        self.feature_labels = {
            'negemo': '负向情感词密度 (negemo)',
            'posemo': '正向情感词密度 (posemo)',
            'anx': '焦虑词密度 (anx)',
            'pronoun_density': '人称代词密度 (pronoun)'
        }

    def create_combined_scatter(self, scatter_data: Dict[str, Any], output_path: str):
        """创建单张合并散点图（2x2子图）"""
        fig, axes = plt.subplots(2, 2, figsize=self.figsize)
        axes = axes.flatten()
        
        combinations = [
            ('GAD-7', 'negemo', axes[0]),
            ('GAD-7', 'posemo', axes[1]),
            ('PHQ-9', 'negemo', axes[2]),
            ('PHQ-9', 'posemo', axes[3]),
        ]
        
        for scale, feature, ax in combinations:
            self._plot_scatter_combined(ax, scatter_data, scale, feature)
        
        plt.suptitle('语言特征与量表得分相关性分析\n(单图合并版本)', fontsize=14, fontweight='bold')
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return output_path

    def create_pronoun_scatter(self, scatter_data: Dict, output_path: str):
        """创建人称代词密度与量表得分散点图"""
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))
        
        combinations = [
            ('GAD-7', axes[0]),
            ('PHQ-9', axes[1]),
        ]
        
        for scale, ax in combinations:
            self._plot_pronoun_scatter(ax, scatter_data, scale)
        
        plt.suptitle('人称代词密度与焦虑抑郁量表得分相关性分析', fontsize=14, fontweight='bold')
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return output_path

    def _plot_pronoun_scatter(self, ax, scatter_data: Dict, scale: str):
        """绘制人称代词散点图"""
        for dataset, color in [('with_caregiver', self.COLORS['with_caregiver']), 
                               ('without_caregiver', self.COLORS['without_caregiver'])]:
            data = scatter_data.get(dataset, {}).get(scale, [])
            
            if not data:
                continue
            
            x = [d['scale_value'] for d in data]
            y = [d.get('pronoun_density', 0) for d in data]
            
            ax.scatter(x, y, c=color, alpha=0.6, s=50, 
                      label=self.LABELS[dataset], edgecolors='white', linewidth=0.5)
            
            if len(x) >= 3:
                try:
                    slope, intercept, r, p, se = stats.linregress(x, y)
                    x_line = np.linspace(min(x), max(x), 100)
                    y_line = slope * x_line + intercept
                    ax.plot(x_line, y_line, color=color, linestyle='--', linewidth=2, alpha=0.8)
                    
                    ax.annotate(f'r={r:.3f}\np={p:.4f}', 
                               xy=(0.05, 0.95 - list(self.COLORS.keys()).index(dataset) * 0.12),
                               xycoords='axes fraction',
                               fontsize=9,
                               color=color,
                               fontweight='bold',
                               verticalalignment='top')
                except Exception:
                    pass
        
        ax.set_xlabel(f'{scale} 量表得分', fontsize=11)
        ax.set_ylabel('人称代词密度 (pronoun_density)', fontsize=11)
        ax.legend(loc='upper right', fontsize=9)
        ax.grid(True, alpha=0.3)

    def _plot_scatter_combined(self, ax, scatter_data: Dict, scale: str, feature: str):
        """绘制合并散点图"""
        for dataset, color in [('with_caregiver', self.COLORS['with_caregiver']), 
                               ('without_caregiver', self.COLORS['without_caregiver'])]:
            data = scatter_data.get(dataset, {}).get(scale, [])
            
            if not data:
                continue
            
            x = [d['scale_value'] for d in data]
            y = [d[feature] for d in data]
            
            ax.scatter(x, y, c=color, alpha=0.6, s=50, 
                      label=self.LABELS[dataset], edgecolors='white', linewidth=0.5)
            
            if len(x) >= 3:
                try:
                    slope, intercept, r, p, se = stats.linregress(x, y)
                    x_line = np.linspace(min(x), max(x), 100)
                    y_line = slope * x_line + intercept
                    ax.plot(x_line, y_line, color=color, linestyle='--', linewidth=2, alpha=0.8)
                    
                    ax.annotate(f'r={r:.3f}\np={p:.4f}', 
                               xy=(0.05, 0.95 - list(self.COLORS.keys()).index(dataset) * 0.12),
                               xycoords='axes fraction',
                               fontsize=9,
                               color=color,
                               fontweight='bold',
                               verticalalignment='top')
                except Exception:
                    pass
        
        ax.set_xlabel(f'{scale} 量表得分', fontsize=11)
        ax.set_ylabel(self.feature_labels.get(feature, feature), fontsize=11)
        ax.legend(loc='upper right', fontsize=9)
        ax.grid(True, alpha=0.3)

    def create_summary_figure(self, validation_results: Dict, correlations: Dict, output_path: str):
        """创建结果摘要图"""
        fig, ax = plt.subplots(figsize=(12, 6))
        
        ax.axis('off')
        
        title = "量表效度验证结果摘要"
        ax.text(0.5, 0.95, title, ha='center', va='top', fontsize=14, fontweight='bold')
        
        y_pos = 0.82
        line_height = 0.06
        
        for dataset, dataset_corr in correlations.items():
            ax.text(0.05, y_pos, f"【{dataset}】", fontsize=12, fontweight='bold')
            y_pos -= line_height * 1.5
            
            for hyp_id, result in validation_results.get(dataset, {}).items():
                status = result['overall']
                status_color = 'green' if status == 'supported' else 'red'
                
                ax.text(0.08, y_pos, f"{result['hypothesis']}", fontsize=10)
                ax.text(0.55, y_pos, f"[{result['supported_ratio']}]", fontsize=10, color=status_color)
                ax.text(0.75, y_pos, status.upper(), fontsize=10, color=status_color, fontweight='bold')
                y_pos -= line_height
            
            y_pos -= line_height * 0.5
        
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return output_path

    def create_correlation_heatmap(self, correlations: Dict, output_path: str):
        """创建相关性热力图（完整版：全特征 + 显著性标记）"""
        datasets = list(correlations.keys())
        features = [
            'negemo', 'posemo', 'anx', 'sad', 'anger',
            'health', 'humans', 'insight', 'cause', 'discrep',
            'negate', 'certain', 'tentat', 'PastM', 'assent'
        ]
        scales = ['GAD-7_mean', 'PHQ-9_mean']
        
        fig, axes = plt.subplots(1, len(datasets), figsize=(16, 5))
        if len(datasets) == 1:
            axes = [axes]
        
        for idx, dataset in enumerate(datasets):
            ax = axes[idx]
            
            matrix = []
            sig_matrix = []
            for scale in scales:
                row = []
                sig_row = []
                for feat in features:
                    corr_data = correlations[dataset].get(scale, {}).get(feat, {})
                    r = corr_data.get('pearson_r', 0)
                    sig = corr_data.get('significant', False)
                    row.append(r)
                    sig_row.append(sig)
                matrix.append(row)
                sig_matrix.append(sig_row)
            
            im = ax.imshow(matrix, cmap='RdBu_r', aspect='auto', vmin=-0.6, vmax=0.6)
            
            ax.set_xticks(range(len(features)))
            ax.set_xticklabels(features, rotation=45, ha='right', fontsize=9)
            ax.set_yticks(range(len(scales)))
            ax.set_yticklabels([s.replace('_mean', '') for s in scales], fontsize=11)
            ax.set_title(f'{dataset}', fontsize=12, fontweight='bold')
            
            for i in range(len(scales)):
                for j in range(len(features)):
                    val = matrix[i][j]
                    is_sig = sig_matrix[i][j]
                    
                    if abs(val) > 0.35:
                        color = 'white'
                    else:
                        color = 'black'
                    
                    marker = '*' if is_sig else ''
                    ax.text(j, i, f'{val:.2f}{marker}', ha='center', va='center', 
                           color=color, fontsize=8, fontweight='bold' if is_sig else 'normal')
            
            plt.colorbar(im, ax=ax, shrink=0.8, label='Pearson r')
        
        plt.suptitle('语言特征与量表相关性热力图 (*=p<0.05)', fontsize=14, fontweight='bold')
        plt.tight_layout(rect=[0, 0.03, 1, 0.95])
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return output_path

    def create_descriptive_stats_table(self, desc_stats: Dict, output_path: str, dataset_name: str = ''):
        """创建描述性统计表格图（完整版）"""
        fig, ax = plt.subplots(figsize=(16, 10))
        ax.axis('off')
        
        if not desc_stats:
            ax.text(0.5, 0.5, 'No descriptive statistics available', ha='center', va='center')
            plt.savefig(output_path, dpi=150, bbox_inches='tight')
            plt.close()
            return output_path
        
        liwc_stats = desc_stats.get('liwc_features', {})
        
        # 重点研究因子 - 基于相关性分析结果
        if 'without_caregiver' in dataset_name:
            highlight_features = {'discrep', 'posemo', 'feel'}  # 显著/边缘显著
        elif 'with_caregiver' in dataset_name:
            highlight_features = {'number', 'negate', 'discrep', 'affect_ratio'}  # Spearman显著
        else:
            highlight_features = {'discrep'}  # 默认
        
        table_data = [['Feature', 'Mean%', 'SD%', 'Min%', 'Max%', '%>0']]
        for feat in ['negemo', 'posemo', 'anx', 'sad', 'anger', 'health', 
                     'humans', 'insight', 'cause', 'discrep', 'body',
                     'negate', 'certain', 'tentat', 'PastM', 'assent', 'funct']:
            if feat in liwc_stats:
                s = liwc_stats[feat]
                table_data.append([
                    feat,
                    f'{s["mean"]*100:.3f}',
                    f'{s["std"]*100:.3f}',
                    f'{s["min"]*100:.3f}',
                    f'{s["max"]*100:.3f}',
                    f'{s["pct_nonzero"]:.1f}'
                ])
        
        table = ax.table(cellText=table_data, loc='center', cellLoc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(9)
        table.scale(1.3, 1.6)
        
        for i in range(len(table_data[0])):
            table[(0, i)].set_facecolor('#4472C4')
            table[(0, i)].set_text_props(color='white', fontweight='bold')
        
        # 高亮重点因子
        for row_idx in range(1, len(table_data)):
            feat = table_data[row_idx][0]
            if feat in highlight_features:
                for col_idx in range(len(table_data[0])):
                    table[(row_idx, col_idx)].set_facecolor('#FFFACD')  # 浅黄色背景
                    table[(row_idx, col_idx)].set_text_props(fontweight='bold')
        
        plt.title(f'Descriptive Statistics (n={desc_stats.get("n_patients", "N/A")}) - 重点因子已加粗', 
                  fontsize=14, fontweight='bold', pad=20)
        plt.tight_layout()
        plt.savefig(output_path, dpi=150, bbox_inches='tight')
        plt.close()
        
        return output_path
