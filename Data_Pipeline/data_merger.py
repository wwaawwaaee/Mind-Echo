#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据合并与转换模块 (Data Merger Module)

本模块负责将Excel量表数据和对话文本数据合并，转换为统一的结构化JSON格式。
按照指定的数据格式要求，生成包含患者人口统计学信息、纵向就诊记录和对话数据的完整数据集。

主要功能：
1. 合并Excel数据和对话数据
2. 构建纵向就诊记录结构
3. 转换数据格式为指定的JSON结构
4. 处理数据缺失和异常情况
5. 生成符合训练要求的数据集

作者: 数据管道团队
版本: 1.0.0
创建日期: 2026-01-28
"""

import os
import json
import logging
from typing import Dict, List, Any, Optional
from datetime import datetime

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class DataMerger:
    """
    数据合并与转换器
    
    负责将Excel量表数据和对话文本数据合并，
    转换为符合要求的JSON格式数据集。
    
    属性:
        dataset_id (str): 数据集标识符
        default_visit_types (dict): 默认就诊类型映射
    """
    
    def __init__(self, dataset_id: str = "outpatient_depression_v1"):
        """
        初始化数据合并器
        
        参数:
            dataset_id (str): 数据集标识符，默认为"outpatient_depression_v1"
        """
        self.dataset_id = dataset_id
        
        # 就诊类型映射
        self.default_visit_types = {
            "initial": ["initial", "first", "1", "初诊", "首次"],
            "follow_up": ["follow_up", "followup", "second", "2", "复诊", "随访"],
            "regular": ["regular", "routine", "常规", "定期"]
        }
        
        logger.info(f"数据合并器初始化完成，数据集ID: {dataset_id}")
    
    def merge_patient_data(self, 
                          excel_data: Dict[str, Dict[str, Any]], 
                          dialogue_data: Dict[str, Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        合并Excel数据和对话数据
        
        参数:
            excel_data (dict): Excel处理后的患者数据
            dialogue_data (dict): 对话处理后的数据
            
        返回:
            dict: 合并后的患者数据
        """
        logger.info("开始合并Excel数据和对话数据...")
        
        # 初始化结果字典
        merged_data = {}
        
        # 首先处理有Excel数据的患者
        for patient_id, excel_info in excel_data.items():
            if patient_id not in merged_data:
                merged_data[patient_id] = {
                    "excel_data": excel_info,
                    "dialogues": [],
                    "visit_sessions": {}
                }
        
        # 处理对话数据
        for filename, dialogue_info in dialogue_data.items():
            file_info = dialogue_info.get("file_info", {})
            patient_id = file_info.get("patient_id", "unknown")
            
            if patient_id == "unknown":
                # 尝试从文件名提取患者ID
                import re
                match = re.match(r"(P\d+)", filename)
                if match:
                    patient_id = match.group(1)
            
            if patient_id not in merged_data:
                merged_data[patient_id] = {
                    "excel_data": None,
                    "dialogues": [],
                    "visit_sessions": {}
                }
            
            # 添加对话信息
            dialogue_entry = {
                "filename": filename,
                "file_info": file_info,
                "dialogue": dialogue_info.get("dialogue", []),
                "raw_content": dialogue_info.get("raw_content", "")
            }
            
            merged_data[patient_id]["dialogues"].append(dialogue_entry)
            
            # 根据就诊类型组织会话
            visit_type = self._determine_visit_type(file_info.get("visit_type", ""))
            session_id = file_info.get("session_id", f"{patient_id}_{visit_type}")
            
            if session_id not in merged_data[patient_id]["visit_sessions"]:
                merged_data[patient_id]["visit_sessions"][session_id] = {
                    "session_id": session_id,
                    "visit_type": visit_type,
                    "visit_number": file_info.get("visit_number"),
                    "is_initial": file_info.get("is_initial", False),
                    "dialogues": [],
                    "date": file_info.get("date")
                }
            
            merged_data[patient_id]["visit_sessions"][session_id]["dialogues"].append(dialogue_entry)
        
        logger.info(f"数据合并完成: 共 {len(merged_data)} 个患者")
        return merged_data
    
    def _determine_visit_type(self, visit_type_str: str) -> str:
        """
        确定标准化的就诊类型
        
        参数:
            visit_type_str (str): 原始就诊类型字符串
            
        返回:
            str: 标准化的就诊类型
        """
        visit_type_str_lower = str(visit_type_str).lower()
        
        for standardized_type, variants in self.default_visit_types.items():
            if visit_type_str_lower in variants:
                return standardized_type
        
        # 尝试匹配部分关键词
        if "initial" in visit_type_str_lower or "first" in visit_type_str_lower or "初诊" in visit_type_str_lower:
            return "initial"
        elif "follow" in visit_type_str_lower or "second" in visit_type_str_lower or "复诊" in visit_type_str_lower:
            return "follow_up"
        elif "regular" in visit_type_str_lower or "routine" in visit_type_str_lower or "常规" in visit_type_str_lower:
            return "regular"
        else:
            return "unknown"
    
    def build_longitudinal_data(self, patient_id: str, patient_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        为单个患者构建纵向数据
        
        参数:
            patient_id (str): 患者ID
            patient_data (dict): 患者合并数据
            
        返回:
            list: 纵向就诊记录列表
        """
        longitudinal_data = []
        
        # 获取Excel数据
        excel_data = patient_data.get("excel_data", {})
        demographics = excel_data.get("demographics", {}) if excel_data else {}
        phq9_assessment = excel_data.get("phq9_assessment", {}) if excel_data else {}
        
        # 获取会话数据
        visit_sessions = patient_data.get("visit_sessions", {})
        
        # 如果没有会话数据，创建一个默认会话
        if not visit_sessions:
            session_id = f"{patient_id}_visit_1"
            visit_sessions[session_id] = {
                "session_id": session_id,
                "visit_type": "initial",
                "dialogues": patient_data.get("dialogues", [])
            }
        
        # 为每个会话创建就诊记录
        for session_id, session_info in visit_sessions.items():
            # 获取该会话的对话
            session_dialogues = session_info.get("dialogues", [])
            
            # 提取对话内容
            dialogue_turns = []
            for dialogue_entry in session_dialogues:
                dialogue = dialogue_entry.get("dialogue", [])
                for turn in dialogue:
                    # 确保对话格式符合要求
                    formatted_turn = {
                        "turn_id": turn.get("turn_id", len(dialogue_turns) + 1),
                        "role": turn.get("role", "unknown"),
                        "content": turn.get("content", "")
                    }
                    dialogue_turns.append(formatted_turn)
            
            # 构建PHQ-9评估数据
            phq9_data = None
            if phq9_assessment:
                phq9_data = {
                    "total_score": phq9_assessment.get("total_score"),
                    "severity": phq9_assessment.get("severity"),
                    "items": phq9_assessment.get("items", [])
                }
            
            # 构建就诊记录
            visit_record = {
                "session_id": session_id,
                "visit_type": session_info.get("visit_type", "unknown"),
                "date": session_info.get("date") or demographics.get("visit_date"),
                "phq9_assessment": phq9_data,
                "dialogue": dialogue_turns
            }
            
            # 移除空值
            visit_record = {k: v for k, v in visit_record.items() if v is not None}
            
            longitudinal_data.append(visit_record)
        
        # 按就诊类型排序（初诊在前）
        longitudinal_data.sort(key=lambda x: (x.get("visit_type") != "initial", x.get("session_id", "")))
        
        return longitudinal_data
    
    def convert_to_final_format(self, merged_data: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        转换为最终的JSON格式
        
        参数:
            merged_data (dict): 合并后的患者数据
            
        返回:
            list: 符合要求的JSON数据列表
        """
        logger.info("开始转换为最终JSON格式...")
        
        final_dataset = []
        
        for patient_id, patient_data in merged_data.items():
            # 获取Excel数据
            excel_data = patient_data.get("excel_data", {})
            demographics_info = excel_data.get("demographics", {}) if excel_data else {}
            
            # 构建人口统计学信息
            demographics = {
                "age": demographics_info.get("age"),
                "gender": demographics_info.get("gender"),
                "occupation": demographics_info.get("occupation"),
                "education": demographics_info.get("education"),
                "marital_status": demographics_info.get("marital_status")
            }
            
            # 移除空值
            demographics = {k: v for k, v in demographics.items() if v is not None}
            
            # 构建纵向数据
            longitudinal_data = self.build_longitudinal_data(patient_id, patient_data)
            
            # 构建最终的患者记录
            patient_record = {
                "dataset_id": self.dataset_id,
                "patient_id": patient_id,
                "demographics": demographics if demographics else None,
                "longitudinal_data": longitudinal_data
            }
            
            final_dataset.append(patient_record)
        
        logger.info(f"格式转换完成: 共 {len(final_dataset)} 条记录")
        return final_dataset
    
    def save_as_json(self, data: List[Dict[str, Any]], output_path: str, indent: int = 2):
        """
        保存数据为JSON文件
        
        参数:
            data (list): 要保存的数据
            output_path (str): 输出文件路径
            indent (int): JSON缩进，默认为2
        """
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=indent)
            
            logger.info(f"数据已保存到: {output_path}")
            logger.info(f"文件大小: {os.path.getsize(output_path)} 字节")
            
        except Exception as e:
            logger.error(f"保存JSON文件失败: {e}")
            raise
    
    def save_as_jsonl(self, data: List[Dict[str, Any]], output_path: str):
        """
        保存数据为JSONL文件（每行一个JSON对象）
        
        参数:
            data (list): 要保存的数据
            output_path (str): 输出文件路径
        """
        # 确保输出目录存在
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                for record in data:
                    json_line = json.dumps(record, ensure_ascii=False)
                    f.write(json_line + '\n')
            
            logger.info(f"数据已保存到: {output_path}")
            logger.info(f"文件大小: {os.path.getsize(output_path)} 字节")
            logger.info(f"记录数量: {len(data)}")
            
        except Exception as e:
            logger.error(f"保存JSONL文件失败: {e}")
            raise
    
    def generate_statistics(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        生成数据统计信息
        
        参数:
            data (list): 数据集
            
        返回:
            dict: 统计信息
        """
        stats = {
            "total_patients": len(data),
            "patients_with_demographics": 0,
            "patients_with_phq9": 0,
            "total_visits": 0,
            "total_dialogue_turns": 0,
            "visit_type_distribution": {},
            "gender_distribution": {}
        }
        
        for record in data:
            # 人口统计学信息
            if record.get("demographics"):
                stats["patients_with_demographics"] += 1
                
                # 性别分布
                gender = record["demographics"].get("gender")
                if gender:
                    stats["gender_distribution"][gender] = stats["gender_distribution"].get(gender, 0) + 1
            
            # 纵向数据
            longitudinal_data = record.get("longitudinal_data", [])
            stats["total_visits"] += len(longitudinal_data)
            
            for visit in longitudinal_data:
                # PHQ-9评估
                if visit.get("phq9_assessment"):
                    stats["patients_with_phq9"] += 1
                
                # 就诊类型分布
                visit_type = visit.get("visit_type", "unknown")
                stats["visit_type_distribution"][visit_type] = stats["visit_type_distribution"].get(visit_type, 0) + 1
                
                # 对话轮次
                dialogue = visit.get("dialogue", [])
                stats["total_dialogue_turns"] += len(dialogue)
        
        return stats


# 便捷函数
def merge_and_convert(excel_data: Dict[str, Dict[str, Any]], 
                     dialogue_data: Dict[str, Dict[str, Any]], 
                     dataset_id: str = "outpatient_depression_v1") -> List[Dict[str, Any]]:
    """
    便捷函数：合并并转换数据
    
    参数:
        excel_data: Excel处理后的患者数据
        dialogue_data: 对话处理后的数据
        dataset_id: 数据集标识符
        
    返回:
        list: 转换后的JSON数据
    """
    merger = DataMerger(dataset_id=dataset_id)
    merged_data = merger.merge_patient_data(excel_data, dialogue_data)
    return merger.convert_to_final_format(merged_data)


def save_dataset(data: List[Dict[str, Any]], output_path: str, format: str = "json"):
    """
    便捷函数：保存数据集
    
    参数:
        data: 要保存的数据
        output_path: 输出文件路径
        format: 输出格式，"json"或"jsonl"
    """
    merger = DataMerger()
    
    if format.lower() == "jsonl":
        merger.save_as_jsonl(data, output_path)
    else:
        merger.save_as_json(data, output_path)


if __name__ == "__main__":
    # 模块测试代码
    import sys
    
    print("数据合并与转换模块")
    print("=" * 50)
    
    # 创建示例数据
    sample_excel_data = {
        "P10086": {
            "demographics": {
                "age": 34,
                "gender": "female",
                "occupation": "teacher",
                "education": "master",
                "visit_date": "2023-10-01"
            },
            "phq9_assessment": {
                "total_score": 18,
                "severity": "Moderately severe",
                "items": [3, 2, 1, 3, 2, 2, 2, 2, 1]
            }
        }
    }
    
    sample_dialogue_data = {
        "P10086_visit_1.txt": {
            "file_info": {
                "patient_id": "P10086",
                "session_id": "P10086_visit_1",
                "visit_type": "initial",
                "visit_number": 1,
                "is_initial": True
            },
            "dialogue": [
                {"turn_id": 1, "role": "doctor", "content": "最近睡眠情况怎么样？"},
                {"turn_id": 2, "role": "patient", "content": "非常糟糕，入睡很困难。"},
                {"turn_id": 3, "role": "doctor", "content": "这种情况持续多久了？"},
                {"turn_id": 4, "role": "patient", "content": "大概有两个月了。"}
            ]
        },
        "P10086_visit_2.txt": {
            "file_info": {
                "patient_id": "P10086",
                "session_id": "P10086_visit_2",
                "visit_type": "follow_up",
                "visit_number": 2,
                "is_initial": False
            },
            "dialogue": [
                {"turn_id": 1, "role": "doctor", "content": "上次开的药感觉怎么样？"},
                {"turn_id": 2, "role": "patient", "content": "感觉好一些了，睡眠有所改善。"},
                {"turn_id": 3, "role": "doctor", "content": "情绪方面呢？"},
                {"turn_id": 4, "role": "patient", "content": "还是容易感到沮丧，但比之前好一点。"}
            ]
        }
    }
    
    print("示例数据:")
    print(f"  Excel数据: {len(sample_excel_data)} 个患者")
    print(f"  对话数据: {len(sample_dialogue_data)} 个文件")
    
    # 合并和转换数据
    try:
        final_data = merge_and_convert(sample_excel_data, sample_dialogue_data)
        
        print(f"\n转换后的数据: {len(final_data)} 条记录")
        
        if final_data:
            sample_record = final_data[0]
            print(f"\n示例记录结构:")
            print(f"  数据集ID: {sample_record.get('dataset_id')}")
            print(f"  患者ID: {sample_record.get('patient_id')}")
            print(f"  就诊记录数: {len(sample_record.get('longitudinal_data', []))}")
            
            # 显示统计信息
            stats = DataMerger().generate_statistics(final_data)
            print(f"\n数据统计:")
            print(f"  总患者数: {stats['total_patients']}")
            print(f"  总就诊次数: {stats['total_visits']}")
            print(f"  总对话轮次: {stats['total_dialogue_turns']}")
            
    except Exception as e:
        print(f"数据处理失败: {e}")