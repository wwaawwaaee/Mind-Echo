import json
from pathlib import Path
from typing import Dict, List, Any
from collections import Counter

from liwc_analyzer import LIWCAnalyzer


class DatasetComparator:
    """数据集对比分析器"""

    ROLE_MAPPING = {
        'with_caregiver': 'caregiver',
        'without_caregiver': 'patient'
    }

    def __init__(self, 
                 with_caregiver_path: str,
                 without_caregiver_path: str,
                 liwc_dict_path: str):
        self.with_cg_path = Path(with_caregiver_path)
        self.without_cg_path = Path(without_caregiver_path)
        self.liwc_analyzer = LIWCAnalyzer(liwc_dict_path)
        
        self.with_cg_data = None
        self.without_cg_data = None

    def load_datasets(self):
        """加载两个数据集"""
        with open(self.with_cg_path, 'r', encoding='utf-8') as f:
            self.with_cg_data = json.load(f)
        with open(self.without_cg_path, 'r', encoding='utf-8') as f:
            self.without_cg_data = json.load(f)
        
        return self

    def compare_basic_stats(self) -> Dict[str, Any]:
        """基础统计对比"""
        return {
            'with_caregiver': {
                'total_patients': self.with_cg_data['stats']['total_patients'],
                'total_visits': self.with_cg_data['stats']['total_visits'],
                'avg_visits_per_patient': round(
                    self.with_cg_data['stats']['total_visits'] / 
                    self.with_cg_data['stats']['total_patients'], 2
                )
            },
            'without_caregiver': {
                'total_patients': self.without_cg_data['stats']['total_patients'],
                'total_visits': self.without_cg_data['stats']['total_visits'],
                'avg_visits_per_patient': round(
                    self.without_cg_data['stats']['total_visits'] / 
                    self.without_cg_data['stats']['total_patients'], 2
                )
            }
        }

    def _count_turns_by_role(self, dataset: Dict) -> Counter:
        """统计各角色轮次数量"""
        counter = Counter()
        for patient in dataset['patients']:
            for visit in patient.get('visits', []):
                for turn in visit.get('dialogue', {}).get('turns', []):
                    counter[turn.get('role', 'other')] += 1
        return counter

    def compare_turn_distribution(self) -> Dict[str, Counter]:
        """对话轮次分布对比"""
        return {
            'with_caregiver': self._count_turns_by_role(self.with_cg_data),
            'without_caregiver': self._count_turns_by_role(self.without_cg_data)
        }

    def _extract_role_features(self, dataset: Dict, role: str) -> List[Dict[str, float]]:
        """提取指定角色的所有 LIWC 特征"""
        all_features = []
        for patient in dataset['patients']:
            for visit in patient.get('visits', []):
                turns = visit.get('dialogue', {}).get('turns', [])
                features = self.liwc_analyzer.extract_by_role(turns, role)
                if features.get('_total_words', 0) > 0:
                    features['patient_id'] = patient.get('patient_id')
                    features['visit_id'] = visit.get('visit_id')
                    all_features.append(features)
        return all_features

    def _average_features(self, features_list: List[Dict]) -> Dict[str, float]:
        """计算特征均值"""
        if not features_list:
            return {}
        
        keys = [k for k in features_list[0].keys() if k not in ['patient_id', 'visit_id']]
        return {
            k: round(sum(f.get(k, 0) for f in features_list) / len(features_list), 6)
            for k in keys
        }

    def compare_main_features(self) -> Dict[str, Any]:
        """
        主要叙述者语言特征对比
        
        根据数据集类型选择正确的分析角色：
        - with_caregiver: caregiver (家长是主要叙述者)
        - without_caregiver: patient (患者是主要叙述者)
        """
        with_role = self.ROLE_MAPPING['with_caregiver']
        without_role = self.ROLE_MAPPING['without_caregiver']
        
        with_features = self._extract_role_features(self.with_cg_data, with_role)
        without_features = self._extract_role_features(self.without_cg_data, without_role)
        
        with_avg = self._average_features(with_features)
        without_avg = self._average_features(without_features)
        
        with_avg.update(self.liwc_analyzer.get_key_indicators({'_': with_avg}))
        without_avg.update(self.liwc_analyzer.get_key_indicators({'_': without_avg}))
        
        return {
            'with_caregiver': {
                'role': with_role,
                'role_description': '家长（护理人员）',
                'valid_samples': len(with_features),
                'avg_features': with_avg
            },
            'without_caregiver': {
                'role': without_role,
                'role_description': '患者本人',
                'valid_samples': len(without_features),
                'avg_features': without_avg
            }
        }

    def analyze_caregiver_features(self) -> Dict[str, Any]:
        """护理人员特征分析（仅 with_caregiver）"""
        caregiver_features = self._extract_role_features(self.with_cg_data, 'caregiver')
        
        avg = self._average_features(caregiver_features)
        
        return {
            'total_caregiver_turns': sum(1 for p in self.with_cg_data['patients'] 
                                        for v in p.get('visits', []) 
                                        for t in v.get('dialogue', {}).get('turns', [])
                                        if t.get('role') == 'caregiver'),
            'valid_samples': len(caregiver_features),
            'avg_features': avg,
            'key_indicators': {
                'anxiety_index': avg.get('anx', 0),
                'depression_index': avg.get('sad', 0),
                'anger_index': avg.get('anger', 0),
                'positive_affect': avg.get('posemo', 0),
                'negative_affect': avg.get('negemo', 0),
                'health_focus': avg.get('health', 0)
            }
        }

    def run_full_analysis(self) -> Dict[str, Any]:
        """运行完整对比分析"""
        self.load_datasets()
        
        results = {
            'basic_stats': self.compare_basic_stats(),
            'turn_distribution': {
                'with_caregiver': dict(self.compare_turn_distribution()['with_caregiver']),
                'without_caregiver': dict(self.compare_turn_distribution()['without_caregiver'])
            },
            'main_role_features': self.compare_main_features(),
            'caregiver_analysis': self.analyze_caregiver_features(),
            'key_findings': self._generate_key_findings()
        }
        
        return results

    def _generate_key_findings(self) -> List[str]:
        """生成关键发现摘要"""
        findings = []
        
        with_turns = self.compare_turn_distribution()['with_caregiver']
        without_turns = self.compare_turn_distribution()['without_caregiver']
        
        if with_turns.get('caregiver', 0) > 0:
            findings.append(f"with_caregiver 数据集包含 {with_turns['caregiver']} 条护理人员(caregiver)轮次")
        
        findings.append(f"with_caregiver 主要叙述者: caregiver (家长), 患者轮次仅 {with_turns.get('patient', 0)} 条")
        findings.append(f"without_caregiver 主要叙述者: patient (患者), 患者轮次 {without_turns.get('patient', 0)} 条")
        
        findings.append(f"with_caregiver 患者数: {self.with_cg_data['stats']['total_patients']}")
        findings.append(f"without_caregiver 患者数: {self.without_cg_data['stats']['total_patients']}")
        
        findings.append("结论: with_caregiver 为儿科场景(家长代述), without_caregiver 为成人场景(患者自述)")
        
        return findings
