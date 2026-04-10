import json
from pathlib import Path
from typing import Dict, List, Any, Optional
from scipy import stats
import numpy as np

from liwc_analyzer import LIWCAnalyzer


class CorrelationAnalyzer:
    """量表相关性分析器 - 语言特征与量表效度验证"""

    HYPOTHESES = {
        'H1_negemo': {'name': 'H1: 量表得分高 → 负向情感词(negemo)多', 'expected': 'positive'},
        'H2_posemo': {'name': 'H2: 量表得分高 → 正向情感词(posemo)少', 'expected': 'negative'},
        'H3_anx': {'name': 'H3: 量表得分高 → 焦虑词(anx)多', 'expected': 'positive'},
        'H4_sad': {'name': 'H4: 量表得分高 → 悲伤词(sad)多', 'expected': 'positive'},
        'H5_health': {'name': 'H5: 量表得分高 → 健康词(health)多', 'expected': 'positive'},
        'H6_humans': {'name': 'H6: 量表得分高 → 社交词(humans)多', 'expected': 'positive'},
        'H7_cogmech': {'name': 'H7: 量表得分高 → 认知词(cogmech)多', 'expected': 'positive'},
        'H8_negate': {'name': 'H8: 量表得分高 → 否定词(negate)多', 'expected': 'positive'},
        'H9_certain': {'name': 'H9: 量表得分高 → 确定性词(certain)少', 'expected': 'negative'},
        'H10_tentat': {'name': 'H10: 量表得分高 → 试探性词(tentat)多', 'expected': 'positive'},
        'H11_past': {'name': 'H11: 量表得分高 → 过去时(PastM)词多', 'expected': 'positive'},
        'H12_assent': {'name': 'H12: 量表得分高 → 肯定词(assent)少', 'expected': 'negative'},
    }

    DESCRIPTIVE_STATS_KEYS = [
        'negemo', 'posemo', 'anx', 'sad', 'anger',
        'health', 'humans', 'insight', 'cause', 'discrep', 'body',
        'family', 'funct', 'negate', 'quant', 'number',
        'PastM', 'PresentM', 'FutureM', 'certain', 'tentat', 'assent'
    ]

    def __init__(self, dataset_path: str, liwc_dict_path: str):
        self.dataset_path = Path(dataset_path)
        self.liwc_analyzer = LIWCAnalyzer(liwc_dict_path)
        self.dataset = None
        self.dataset_name = self._infer_dataset_name()
        self.respondent = self._infer_respondent()
        self.target_role = self._infer_target_role()

    def _infer_dataset_name(self) -> str:
        """推断数据集名称"""
        path_str = str(self.dataset_path).lower()
        if 'with_caregiver' in path_str:
            return 'with_caregiver'
        elif 'without_caregiver' in path_str:
            return 'without_caregiver'
        return 'unknown'

    def _infer_respondent(self) -> str:
        """推断量表填写者身份"""
        if self.dataset_name == 'with_caregiver':
            return 'caregiver'
        elif self.dataset_name == 'without_caregiver':
            return 'patient'
        return 'unknown'

    def _infer_target_role(self) -> str:
        """推断主要分析角色（语言特征来源）"""
        if self.dataset_name == 'with_caregiver':
            return 'caregiver'
        elif self.dataset_name == 'without_caregiver':
            return 'patient'
        return 'patient'

    def load_dataset(self):
        """加载数据集"""
        with open(self.dataset_path, 'r', encoding='utf-8') as f:
            self.dataset = json.load(f)
        return self

    def extract_patient_level_features(self) -> List[Dict]:
        """提取患者级别的 LIWC 特征和量表数据"""
        results = []
        
        for patient in self.dataset.get('patients', []):
            patient_id = patient.get('patient_id')
            scales = patient.get('scales', [])
            visits = patient.get('visits', [])
            
            features = self._aggregate_role_features(visits, self.target_role)
            patient_scales = self._aggregate_scales(scales)
            
            if patient_scales and features:
                results.append({
                    'patient_id': patient_id,
                    'features': features,
                    'scales': patient_scales,
                    'respondent': self.respondent,
                    'dataset': self.dataset_name
                })
        
        return results

    def _aggregate_role_features(self, visits: List[Dict], role: str) -> Dict[str, float]:
        """聚合指定角色的语言特征"""
        all_turns = []
        for visit in visits:
            turns = visit.get('dialogue', {}).get('turns', [])
            all_turns.extend(turns)
        
        if not all_turns:
            return {}
        
        features = self.liwc_analyzer.extract_by_role(all_turns, role)
        if not features or features.get('_total_words', 0) == 0:
            return {}
        
        features.update(self.liwc_analyzer.extract_category_ratios(features))
        features.update(self.liwc_analyzer.get_key_indicators(features))
        
        return features

    def _aggregate_scales(self, scales: List[Dict]) -> Optional[Dict]:
        """聚合量表数据（取均值）"""
        if not scales:
            return None
        
        gad_totals = []
        phq_totals = []
        
        for scale in scales:
            gad = scale.get('GAD-7', {}).get('total')
            phq = scale.get('PHQ-9', {}).get('total')
            respondent_role = scale.get('respondent_role', 'unknown')
            if isinstance(gad, (int, float)):
                gad_totals.append(gad)
            if isinstance(phq, (int, float)):
                phq_totals.append(phq)
        
        result = {}
        if gad_totals:
            result['GAD-7_mean'] = round(np.mean(gad_totals), 2)
            result['GAD-7_latest'] = gad_totals[-1]
            result['respondent_role'] = respondent_role
        if phq_totals:
            result['PHQ-9_mean'] = round(np.mean(phq_totals), 2)
            result['PHQ-9_latest'] = phq_totals[-1]
        
        return result if result else None

    def _safe_pearsonr(self, x: List, y: List) -> tuple:
        """安全计算 Pearson 相关系数，避免常量输入警告"""
        x_arr = np.array(x)
        y_arr = np.array(y)
        
        if len(x_arr) < 3 or len(y_arr) < 3:
            return np.nan, np.nan
        
        x_std = np.std(x_arr)
        y_std = np.std(y_arr)
        
        if x_std == 0 or y_std == 0:
            return np.nan, np.nan
        
        return stats.pearsonr(x_arr, y_arr)

    def _cohens_d(self, x: List, y: List) -> float:
        """计算 Cohen's d 效应量"""
        x_arr = np.array(x)
        y_arr = np.array(y)
        
        n1, n2 = len(x_arr), len(y_arr)
        if n1 < 2 or n2 < 2:
            return np.nan
        
        mean1, mean2 = np.mean(x_arr), np.mean(y_arr)
        var1, var2 = np.var(x_arr, ddof=1), np.var(y_arr, ddof=1)
        
        pooled_std = np.sqrt(((n1-1)*var1 + (n2-1)*var2) / (n1+n2-2))
        
        if pooled_std == 0:
            return np.nan
        
        return (mean1 - mean2) / pooled_std

    def _interpret_cohens_d(self, d: float) -> str:
        """解释 Cohen's d 效应量"""
        d = abs(d)
        if d < 0.2:
            return 'negligible'
        elif d < 0.5:
            return 'small'
        elif d < 0.8:
            return 'medium'
        else:
            return 'large'

    def analyze_correlations(self) -> Dict[str, Any]:
        """分析 LIWC 特征与量表得分的相关性"""
        patient_data = self.extract_patient_level_features()
        
        if not patient_data:
            return {'error': 'No valid data for correlation analysis'}
        
        feature_cols = [
            'negemo', 'posemo', 'anx', 'sad', 'anger',
            'health', 'humans', 'insight', 'cause', 'body', 'family',
            'funct', 'negate', 'quant', 'number',
            'PastM', 'PresentM', 'FutureM',
            'certain', 'tentat', 'discrep', 'inhib',
            'family', 'friend',
            'see', 'hear', 'feel',
            'motion', 'space', 'time',
            'affect_ratio', 'cogmech_ratio', 'bio_ratio', 'social_ratio',
            'anxiety_index', 'depression_index', 'positive_affect', 'negative_affect',
            'pronoun_density'
        ]
        
        scale_cols = ['GAD-7_mean', 'PHQ-9_mean']
        
        correlations = {}
        raw_data = {}
        
        for scale in scale_cols:
            correlations[scale] = {}
            raw_data[scale] = {'scale_values': [], 'feature_data': {}}
            
            scale_data = [(d['patient_id'], d['scales'].get(scale), d['features']) 
                         for d in patient_data if d['scales'].get(scale)]
            
            if len(scale_data) < 3:
                continue
            
            values = [item[1] for item in scale_data]
            raw_data[scale]['scale_values'] = values
            
            for feat in feature_cols:
                feat_values = [item[2].get(feat, 0) for item in scale_data]
                
                if len(feat_values) >= 3:
                    pearson_r, pearson_p = self._safe_pearsonr(values, feat_values)
                    
                    if not np.isnan(pearson_r):
                        try:
                            spearman_r, spearman_p = stats.spearmanr(values, feat_values)
                        except:
                            spearman_r, spearman_p = np.nan, np.nan
                        
                        correlations[scale][feat] = {
                            'pearson_r': round(float(pearson_r), 4),
                            'pearson_p': round(float(pearson_p), 4),
                            'spearman_r': round(float(spearman_r), 4) if not np.isnan(spearman_r) else None,
                            'spearman_p': round(float(spearman_p), 4) if not np.isnan(spearman_p) else None,
                            'significant': bool(pearson_p < 0.05)
                        }
                        raw_data[scale]['feature_data'][feat] = feat_values
        
        validation_results = self._validate_hypotheses(correlations)
        desc_stats = self.compute_descriptive_stats(patient_data)
        
        return {
            'dataset': self.dataset_name,
            'respondent': self.respondent,
            'target_role': self.target_role,
            'sample_size': len(patient_data),
            'correlations': correlations,
            'raw_data': raw_data,
            'hypothesis_validation': validation_results,
            'descriptive_stats': desc_stats,
            'summary': self._summarize_results(correlations, validation_results)
        }

    def _validate_hypotheses(self, correlations: Dict) -> Dict[str, Any]:
        """验证假设"""
        validation = {}
        
        hypothesis_mapping = {
            'H1_negemo': {'features': ['negemo', 'negative_affect'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H2_posemo': {'features': ['posemo', 'positive_affect'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'negative'},
            'H3_anx': {'features': ['anx', 'anxiety_index'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H4_sad': {'features': ['sad', 'depression_index'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H5_health': {'features': ['health', 'bio_ratio'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H6_humans': {'features': ['humans', 'social'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H7_cogmech': {'features': ['cogmech_ratio', 'insight', 'cause'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H8_negate': {'features': ['negate'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H9_certain': {'features': ['certain'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'negative'},
            'H10_tentat': {'features': ['tentat'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H11_past': {'features': ['PastM'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'positive'},
            'H12_assent': {'features': ['assent'], 'scales': ['GAD-7_mean', 'PHQ-9_mean'], 'expected': 'negative'},
        }
        
        for hyp_id, hyp_config in hypothesis_mapping.items():
            results = []
            
            for scale in hyp_config['scales']:
                if scale not in correlations:
                    continue
                    
                for feat in hyp_config['features']:
                    if feat in correlations[scale]:
                        corr_data = correlations[scale][feat]
                        r = corr_data['pearson_r']
                        sig = corr_data['significant']
                        
                        if hyp_config['expected'] == 'positive':
                            supported = sig and r > 0
                        else:
                            supported = sig and r < 0
                        
                        results.append({
                            'scale': scale,
                            'feature': feat,
                            'r': r,
                            'p': corr_data.get('pearson_p', corr_data.get('p_value')),
                            'significant': sig,
                            'supported': supported
                        })
            
            if results:
                supported_count = sum(1 for item in results if item['supported'])
                validation[hyp_id] = {
                    'hypothesis': self.HYPOTHESES[hyp_id]['name'],
                    'tests': results,
                    'supported_ratio': f"{supported_count}/{len(results)}",
                    'overall': 'supported' if supported_count > len(results) / 2 else 'not_supported'
                }
            else:
                validation[hyp_id] = {
                    'hypothesis': self.HYPOTHESES[hyp_id]['name'],
                    'tests': [],
                    'supported_ratio': '0/0',
                    'overall': 'insufficient_data'
                }
        
        return validation

    def compute_descriptive_stats(self, patient_data: List[Dict]) -> Dict[str, Any]:
        """计算描述性统计"""
        if not patient_data:
            return {}
        
        stats = {}
        
        for key in self.DESCRIPTIVE_STATS_KEYS:
            values = [d['features'].get(key, 0) for d in patient_data if d['features'].get(key) is not None]
            if values:
                arr = np.array(values)
                stats[key] = {
                    'mean': round(float(np.mean(arr)), 6),
                    'std': round(float(np.std(arr)), 6),
                    'min': round(float(np.min(arr)), 6),
                    'max': round(float(np.max(arr)), 6),
                    'median': round(float(np.median(arr)), 6),
                    'n_zero': int(np.sum(arr == 0)),
                    'n_nonzero': int(np.sum(arr > 0)),
                    'pct_nonzero': round(float(np.sum(arr > 0) / len(arr) * 100), 2)
                }
        
        scale_stats = {}
        for key in ['GAD-7_mean', 'PHQ-9_mean']:
            values = [d['scales'].get(key) for d in patient_data if d['scales'].get(key) is not None]
            if values:
                arr = np.array(values)
                scale_stats[key] = {
                    'mean': round(float(np.mean(arr)), 2),
                    'std': round(float(np.std(arr)), 2),
                    'min': round(float(np.min(arr)), 2),
                    'max': round(float(np.max(arr)), 2),
                    'median': round(float(np.median(arr)), 2),
                    'n': len(values)
                }
        
        return {
            'liwc_features': stats,
            'scale_scores': scale_stats,
            'n_patients': len(patient_data),
            'n_visits': sum(len(d['scales']) for d in patient_data if d['scales'])
        }

    def _summarize_results(self, correlations: Dict, validation: Dict) -> List[str]:
        """生成结果摘要"""
        summary = []
        
        summary.append(f"数据集: {self.dataset_name}")
        summary.append(f"分析角色: {self.target_role} (量表填写者: {self.respondent})")
        summary.append("")
        
        for scale, feats in correlations.items():
            significant = [(f, v) for f, v in feats.items() if v.get('significant')]
            if significant:
                sorted_sig = sorted(significant, key=lambda x: abs(x[1]['pearson_r']), reverse=True)
                summary.append(f"{scale} 显著相关特征:")
                for feat, vals in sorted_sig[:3]:
                    direction = "正相关" if vals['pearson_r'] > 0 else "负相关"
                    summary.append(f"  - {feat}: r={vals['pearson_r']} ({direction})")
            else:
                summary.append(f"{scale}: 无显著相关特征")
        
        summary.append("")
        summary.append("假设验证:")
        for hyp_id, result in validation.items():
            status = result['overall']
            status_text = {
                'supported': '[OK] supported',
                'not_supported': '[X] not_supported',
                'insufficient_data': '[!] insufficient_data'
            }.get(status, status)
            summary.append(f"  {result['hypothesis']}")
            summary.append(f"    结果: {status_text} ({result['supported_ratio']})")
        
        return summary

    def get_scatter_data(self) -> Dict[str, Any]:
        """获取散点图数据"""
        patient_data = self.extract_patient_level_features()
        
        scatter_data = {
            'with_caregiver': {'GAD-7': [], 'PHQ-9': []},
            'without_caregiver': {'GAD-7': [], 'PHQ-9': []}
        }
        
        for d in patient_data:
            dataset = d['dataset']
            features = d['features']
            scales = d['scales']
            
            if dataset not in scatter_data:
                continue
            
            gad = scales.get('GAD-7_mean')
            phq = scales.get('PHQ-9_mean')
            negemo = features.get('negemo', 0)
            posemo = features.get('posemo', 0)
            anx = features.get('anx', 0)
            pronoun_density = features.get('pronoun_density', 0)
            
            if gad is not None:
                scatter_data[dataset]['GAD-7'].append({
                    'scale_value': gad,
                    'negemo': negemo,
                    'posemo': posemo,
                    'anx': anx,
                    'pronoun_density': pronoun_density,
                    'patient_id': d['patient_id']
                })
            
            if phq is not None:
                scatter_data[dataset]['PHQ-9'].append({
                    'scale_value': phq,
                    'negemo': negemo,
                    'posemo': posemo,
                    'anx': anx,
                    'pronoun_density': pronoun_density,
                    'patient_id': d['patient_id']
                })
        
        return scatter_data
