#!/usr/bin/env python3
"""
Mind-Echo 数据集 LIWC 分析主入口

用法:
    python run_analysis.py
"""

import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

from liwc_analyzer import LIWCAnalyzer
from dataset_comparator import DatasetComparator
from correlation_analyzer import CorrelationAnalyzer
from visualizer import ScatterVisualizer


def parse_args():
    parser = argparse.ArgumentParser(description='Mind-Echo 数据集 LIWC 分析')
    
    parser.add_argument(
        '--input-dir',
        type=Path,
        default=Path('../processed_dataset/output'),
        help='输入数据集目录'
    )
    parser.add_argument(
        '--liwc-dict',
        type=Path,
        default='D:\\vscode-project\\git-project\\Auto_CLIWC\\Auto_CLIWC\\datasets\\sc_liwc.dic',
        help='LIWC 词典路径'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default='./results',
        help='输出结果目录'
    )
    
    return parser.parse_args()


def run_comparison_analysis(args):
    """运行数据集对比分析"""
    print("\n" + "="*60)
    print("1. 数据集对比分析")
    print("="*60)
    
    comparator = DatasetComparator(
        with_caregiver_path=args.input_dir / 'anonymized_dataset_with_caregiver.json',
        without_caregiver_path=args.input_dir / 'anonymized_dataset_without_caregiver.json',
        liwc_dict_path=args.liwc_dict
    )
    
    results = comparator.run_full_analysis()
    
    print("\n基础统计对比:")
    print(f"  with_caregiver: {results['basic_stats']['with_caregiver']}")
    print(f"  without_caregiver: {results['basic_stats']['without_caregiver']}")
    
    print("\n轮次分布:")
    print(f"  with_caregiver: {results['turn_distribution']['with_caregiver']}")
    print(f"  without_caregiver: {results['turn_distribution']['without_caregiver']}")
    
    print("\n关键发现:")
    for finding in results['key_findings']:
        print(f"  - {finding}")
    
    return results


def run_correlation_analysis(args):
    """运行相关性分析"""
    print("\n" + "="*60)
    print("2. 量表相关性分析")
    print("="*60)
    
    results = {}
    all_scatter_data = {
        'with_caregiver': {'GAD-7': [], 'PHQ-9': []},
        'without_caregiver': {'GAD-7': [], 'PHQ-9': []}
    }
    
    for dataset_name, dataset_path in [
        ('with_caregiver', args.input_dir / 'anonymized_dataset_with_caregiver.json'),
        ('without_caregiver', args.input_dir / 'anonymized_dataset_without_caregiver.json')
    ]:
        print(f"\n分析 {dataset_name}...")
        
        analyzer = CorrelationAnalyzer(
            dataset_path=str(dataset_path),
            liwc_dict_path=args.liwc_dict
        )
        analyzer.load_dataset()
        
        corr_results = analyzer.analyze_correlations()
        results[dataset_name] = corr_results
        
        print(f"  样本数: {corr_results.get('sample_size', 0)}")
        print(f"  分析角色: {corr_results.get('target_role', 'N/A')}")
        print(f"  量表填写者: {corr_results.get('respondent', 'N/A')}")
        
        if 'summary' in corr_results:
            for line in corr_results['summary']:
                print(f"    {line}")
        
        scatter_data = analyzer.get_scatter_data()
        for ds in ['with_caregiver', 'without_caregiver']:
            for scale in ['GAD-7', 'PHQ-9']:
                if ds in scatter_data and scale in scatter_data[ds]:
                    all_scatter_data[ds][scale].extend(scatter_data[ds][scale])
    
    return results, all_scatter_data


def generate_visualizations(scatter_data: Dict, validation_results: Dict, correlations: Dict, output_dir: Path):
    """生成可视化图表"""
    print("\n" + "="*60)
    print("3. 生成可视化图表")
    print("="*60)
    
    output_dir.mkdir(parents=True, exist_ok=True)
    figures_dir = output_dir / 'figures'
    figures_dir.mkdir(exist_ok=True)
    
    visualizer = ScatterVisualizer()
    
    scatter_path = figures_dir / 'scatter_correlation_combined.png'
    visualizer.create_combined_scatter(scatter_data, str(scatter_path))
    print(f"  散点图已生成: {scatter_path}")
    
    pronoun_path = figures_dir / 'pronoun_correlation.png'
    visualizer.create_pronoun_scatter(scatter_data, str(pronoun_path))
    print(f"  人称代词散点图已生成: {pronoun_path}")
    
    heatmap_path = figures_dir / 'correlation_heatmap.png'
    visualizer.create_correlation_heatmap(correlations, str(heatmap_path))
    print(f"  热力图已生成: {heatmap_path}")
    
    for ds_name, ds_data in correlations.items():
        if 'descriptive_stats' in ds_data:
            desc_stats = ds_data['descriptive_stats']
            desc_path = figures_dir / f'descriptive_stats_{ds_name}.png'
            visualizer.create_descriptive_stats_table(desc_stats, str(desc_path), ds_name)
            print(f"  描述性统计表已生成: {desc_path}")
    
    return figures_dir


def save_results(comparison_results, correlation_results, output_dir):
    """保存结果到文件"""
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    output = {
        'timestamp': timestamp,
        'comparison': comparison_results,
        'correlations': correlation_results
    }
    
    output_file = output_dir / f'analysis_results_{timestamp}.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\n结果已保存到: {output_file}")
    
    summary_file = output_dir / 'summary_latest.json'
    with open(summary_file, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"摘要已保存到: {summary_file}")


def generate_markdown_report(comparison_results, correlation_results, figures_dir: Path, output_dir: Path):
    """生成 Markdown 报告"""
    report = """# Mind-Echo 数据集 LIWC 分析报告

## 语言特征与量表效度验证研究

    """
    
    report += f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
    
    report += "## 1. 研究背景\n\n"
    report += """本分析旨在通过 LIWC 语言特征与 GAD-7（焦虑量表）、PHQ-9（抑郁量表）的相关性，
验证量表得分的语言学效度，即：患者/家长的语言表达是否与其自评的心理健康状态一致。

**假设验证**:
- H1: 量表得分高 → 负向情感词(negemo)多
- H2: 量表得分高 → 正向情感词(posemo)少
- H3: 量表得分高 → 焦虑词(anx)多
- H4: 量表得分高 → 悲伤词(sad)多
- H5: 量表得分高 → 健康词(health)多
- H6: 量表得分高 → 社交词(humans)多
- H7: 量表得分高 → 认知词(cogmech)多

"""
    
    report += "## 2. 数据集概览\n\n"
    
    for key, data in comparison_results['basic_stats'].items():
        report += f"### {key}\n"
        for k, v in data.items():
            report += f"- **{k}**: {v}\n"
        report += "\n"
    
    report += "## 3. 轮次分布对比\n\n"
    
    for key, dist in comparison_results['turn_distribution'].items():
        report += f"### {key}\n"
        for role, count in dist.items():
            report += f"- {role}: {count}\n"
        report += "\n"
    
    report += "## 4. 描述性统计\n\n"
    
    for dataset_name, corr in correlation_results.items():
        desc_stats = corr.get('descriptive_stats', {})
        if desc_stats:
            report += f"### {dataset_name}\n"
            report += f"- 患者数: {desc_stats.get('n_patients', 'N/A')}\n"
            report += f"- 就诊次数: {desc_stats.get('n_visits', 'N/A')}\n\n"
            
            liwc_stats = desc_stats.get('liwc_features', {})
            if liwc_stats:
                report += "**LIWC 特征密度 (%):**\n\n"
                report += "| 特征 | Mean | SD | Min | Max | Median | %>0 |\n"
                report += "|------|------|-----|-----|-----|--------|-----|\n"
                
                for feat in ['negemo', 'posemo', 'anx', 'sad', 'anger', 'health', 'humans', 'insight', 'cause', 'body']:
                    if feat in liwc_stats:
                        s = liwc_stats[feat]
                        report += f"| {feat} | {s['mean']*100:.3f} | {s['std']*100:.3f} | {s['min']*100:.3f} | {s['max']*100:.3f} | {s['median']*100:.3f} | {s['pct_nonzero']:.1f}% |\n"
                
                report += "\n"
            
            scale_stats = desc_stats.get('scale_scores', {})
            if scale_stats:
                report += "**量表得分:**\n\n"
                report += "| 量表 | Mean | SD | Min | Max | Median |\n"
                report += "|------|------|-----|-----|-----|--------|\n"
                for scale, s in scale_stats.items():
                    report += f"| {scale} | {s['mean']:.1f} | {s['std']:.1f} | {s['min']:.1f} | {s['max']:.1f} | {s['median']:.1f} |\n"
                report += "\n"
    
    report += "## 5. 假设验证结果\n\n"
    
    for dataset_name, corr in correlation_results.items():
        report += f"### {dataset_name}\n"
        report += f"- **分析角色**: {corr.get('target_role', 'N/A')}\n"
        report += f"- **量表填写者**: {corr.get('respondent', 'N/A')}\n"
        report += f"- **样本数**: {corr.get('sample_size', 0)}\n\n"
        
        validation = corr.get('hypothesis_validation', {})
        if validation:
            report += "**假设验证结果:**\n\n"
            report += "| 假设 | 描述 | 检验数 | 支持数 | 结论 |\n"
            report += "|------|------|--------|--------|------|\n"
            
            for hyp_id, result in validation.items():
                hyp_name = result['hypothesis'].split(':')[0].strip()
                hyp_desc = result['hypothesis'].split(':')[1].strip() if ':' in result['hypothesis'] else ''
                conclusion = 'YES' if result['overall'] == 'supported' else 'NO'
                report += f"| {hyp_name} | {hyp_desc} | {result['supported_ratio']} | {conclusion} |\n"
            
            report += "\n**所有特征相关性 (Top 10 by |r|):**\n\n"
            
            all_corrs = []
            for scale, feats in corr.get('correlations', {}).items():
                for feat, vals in feats.items():
                    p_val = vals.get('pearson_p', vals.get('p_value'))
                    all_corrs.append((scale, feat, vals['pearson_r'], p_val, vals['significant']))
            
            sorted_corrs = sorted(all_corrs, key=lambda x: abs(x[2]), reverse=True)[:15]
            report += "| Scale | Feature | Pearson r | p-value | Spearman r | Sig |\n"
            report += "|-------|---------|-----------|---------|------------|-----|\n"
            for scale, feat, r, p, sig in sorted_corrs:
                sig_mark = '*' if sig else ''
                spearman_r = corr.get('correlations', {}).get(scale, {}).get(feat, {}).get('spearman_r', '-')
                if isinstance(spearman_r, float):
                    spearman_r = f'{spearman_r:.3f}'
                report += f"| {scale} | {feat} | {r:.3f} | {p:.4f} | {spearman_r} | {sig_mark} |\n"
        
        report += "\n"
    
    report += "## 6. 可视化结果\n\n"
    
    if figures_dir:
        scatter_rel_path = figures_dir.name + '/scatter_correlation_combined.png'
        heatmap_rel_path = figures_dir.name + '/correlation_heatmap.png'
        report += f"### 语言特征与量表相关性散点图\n"
        report += f"![散点图](../{scatter_rel_path})\n\n"
        report += f"### 相关性热力图 (*=p<0.05)\n"
        report += f"![热力图](../{heatmap_rel_path})\n\n"
        
        for dataset_name in correlation_results.keys():
            desc_rel_path = figures_dir.name + f'/descriptive_stats_{dataset_name}.png'
            report += f"### {dataset_name} 描述性统计\n"
            report += f"![描述性统计](../{desc_rel_path})\n\n"
    
    report += "## 7. 结论\n\n"
    report += "### 主要发现\n\n"
    
    for dataset_name, corr in correlation_results.items():
        validation = corr.get('hypothesis_validation', {})
        supported_count = sum(1 for v in validation.values() if v['overall'] == 'supported')
        total = len(validation)
        
        if total > 0:
            report += f"- **{dataset_name}**: {supported_count}/{total} 假设得到支持\n"
    
    report += "\n### 效度解读\n\n"
    report += """- 若假设多数得到支持，说明语言特征与量表得分具有一致性，量表具有语言学效度
- 若假设多数不支持，可能原因包括：
  - 样本量较小（特别是 without_caregiver 仅 21 例）
  - 门诊场景下患者/家长的语言表达受医生问诊引导
  - 量表填写者与对话叙述者可能不是同一人

"""
    
    report_file = output_dir / 'analysis_report.md'
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print(f"Markdown 报告已生成: {report_file}")
    return report_file


def main():
    args = parse_args()
    
    print("="*60)
    print("Mind-Echo 数据集 LIWC 分析")
    print("语言特征与量表效度验证")
    print("="*60)
    print(f"输入目录: {args.input_dir}")
    print(f"LIWC 词典: {args.liwc_dict}")
    print(f"输出目录: {args.output_dir}")
    
    comparison_results = run_comparison_analysis(args)
    correlation_results, scatter_data = run_correlation_analysis(args)
    
    figures_dir = generate_visualizations(scatter_data, correlation_results, correlation_results, args.output_dir)
    
    save_results(comparison_results, correlation_results, args.output_dir)
    generate_markdown_report(comparison_results, correlation_results, figures_dir, args.output_dir)
    
    print("\n" + "="*60)
    print("分析完成!")
    print("="*60)


if __name__ == '__main__':
    main()
