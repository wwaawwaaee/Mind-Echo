import re
import jieba
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional


class LIWCAnalyzer:
    """基于 Auto_CLIWC 的中文 LIWC 分析器"""

    CATEGORIES = {
        'affect': {'posemo', 'negemo', 'anx', 'anger', 'sad'},
        'social': {'family', 'friend', 'humans'},
        'cogmech': {'insight', 'cause', 'discrep', 'tentat', 'certain', 'inhib'},
        'bio': {'body', 'health', 'sexual', 'ingest'},
        'percept': {'see', 'hear', 'feel'},
        'relativ': {'motion', 'space', 'time'},
        'personal_concerns': {'work', 'achievef', 'leisure', 'home', 'money', 'relig', 'death'}
    }

    PRONOUN_WORDS = {'我', '我们', '你', '你们', '他', '他们', '她', '她们', '它', '它们', '自己', '本人', '咱们', '俺', '俺们', '咱', '咱们', '别人', '他人', '大家', '旁人', '自己', '自身', '本人', '自己'}  # Chinese pronouns

    def __init__(self, dict_path: str):
        self.dict_path = Path(dict_path)
        self.type2name: Dict[int, str] = {}
        self.word2types: Dict[str, set] = defaultdict(set)
        self.name2id: Dict[str, int] = {}
        self._load_dict()

    def _load_dict(self):
        """加载 sc_liwc.dic 词典文件"""
        with open(self.dict_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()

        parse_categories = False
        for line in lines:
            line = line.strip()
            if line.startswith('%'):
                parse_categories = True
                continue
            if not parse_categories:
                if line:
                    parts = line.split()
                    type_id = int(parts[0])
                    type_name = parts[1]
                    self.type2name[type_id] = type_name
                    self.name2id[type_name] = type_id
            else:
                if not line:
                    continue
                parts = line.split()
                word = parts[0]
                categories = [int(c) for c in parts[1:]]
                self.word2types[word] = set(categories)

    def _tokenize(self, text: str) -> List[str]:
        """使用 jieba 分词"""
        words = list(jieba.cut(text))
        return [w for w in words if w.strip() and re.match(r'^[\u4e00-\u9fa5]+$', w)]

    def extract_features(self, text: str) -> Dict[str, float]:
        """提取文本的 LIWC 特征密度"""
        words = self._tokenize(text)
        total_words = len(words)
        if total_words == 0:
            return self._empty_features()

        matched_counts = defaultdict(int)
        total_matched = 0

        for word in words:
            if word in self.word2types:
                total_matched += 1
                for cat_id in self.word2types[word]:
                    cat_name = self.type2name.get(cat_id)
                    if cat_name:
                        matched_counts[cat_name] += 1

        features = self._empty_features()
        for cat_name, count in matched_counts.items():
            features[cat_name] = round(count / total_words, 6)
        
        features['_total_words'] = total_words
        features['_matched_words'] = total_matched
        features['_match_ratio'] = round(total_matched / total_words, 6)
        
        return features

    def _empty_features(self) -> Dict[str, float]:
        """返回空特征字典"""
        features = {'_total_words': 0, '_matched_words': 0, '_match_ratio': 0.0}
        for cat_name in self.type2name.values():
            features[cat_name] = 0.0
        for group_name, group_cats in self.CATEGORIES.items():
            features[f'{group_name}_ratio'] = 0.0
        return features

    def extract_by_role(self, turns: List[Dict], target_role: str) -> Dict[str, float]:
        """按角色提取 LIWC 特征"""
        combined_text = ' '.join(
            turn.get('text', '') 
            for turn in turns 
            if turn.get('role') == target_role
        )
        features = self.extract_features(combined_text)
        features['pronoun_density'] = self.extract_pronoun_density(combined_text)
        return features

    def extract_all_roles(self, turns: List[Dict]) -> Dict[str, Dict[str, float]]:
        """提取所有角色的 LIWC 特征"""
        roles = set(turn.get('role') for turn in turns)
        return {
            role: self.extract_by_role(turns, role)
            for role in roles
        }

    def extract_category_ratios(self, features: Dict[str, float]) -> Dict[str, float]:
        """计算类别级聚合特征"""
        ratios = {}
        for group_name, group_cats in self.CATEGORIES.items():
            total = sum(features.get(cat, 0.0) for cat in group_cats)
            ratios[f'{group_name}_ratio'] = round(total, 6)
        return ratios

    def get_key_indicators(self, features: Dict[str, float]) -> Dict[str, float]:
        """提取关键心理指标"""
        return {
            'anxiety_index': features.get('anx', 0.0),
            'depression_index': features.get('sad', 0.0),
            'anger_index': features.get('anger', 0.0),
            'positive_affect': features.get('posemo', 0.0),
            'negative_affect': features.get('negemo', 0.0),
            'cognitive_complexity': features.get('insight', 0.0) + features.get('cause', 0.0),
            'health_focus': features.get('health', 0.0),
            'social_reference': features.get('humans', 0.0),
            'pronoun_density': features.get('pronoun_density', 0.0)
        }

    def extract_pronoun_density(self, text: str) -> float:
        """计算人称代词密度"""
        words = self._tokenize(text)
        total_words = len(words)
        if total_words == 0:
            return 0.0
        
        pronoun_count = sum(1 for word in words if word in self.PRONOUN_WORDS)
        return round(pronoun_count / total_words, 6)
