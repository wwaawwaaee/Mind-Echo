#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel量表数据读取模块 (Excel Reader Module)

本模块负责读取和解析Excel格式的量表数据，特别是PHQ-9抑郁量表数据。
支持从Excel文件中提取患者基本信息、PHQ-9总分、细项分和严重程度评估。

主要功能：
1. 读取Excel文件，支持多种格式（xlsx, xls）
2. 解析PHQ-9量表数据，计算严重程度
3. 提取患者人口统计学信息
4. 数据清洗和验证
5. 转换为结构化字典格式

作者: 数据管道团队
版本: 1.0.0
创建日期: 2026-01-28
"""

import os
import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass, field
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class PHQ9Assessment:
    """PHQ-9评估结果数据类"""
    total_score: int
    severity: str
    items: List[int]  # 9个项目的分数列表
    assessment_date: Optional[str] = None
    notes: Optional[str] = None


@dataclass
class PatientDemographics:
    """患者人口统计学信息数据类"""
    patient_id: str
    age: Optional[int] = None
    gender: Optional[str] = None
    occupation: Optional[str] = None
    education: Optional[str] = None
    marital_status: Optional[str] = None
    visit_date: Optional[str] = None
    other_info: Dict[str, Any] = field(default_factory=dict)


class ExcelReader:
    """
    Excel量表数据读取器
    
    负责从Excel文件中读取患者信息和PHQ-9量表数据，
    进行数据清洗、验证和结构化转换。
    
    属性:
        engine (str): pandas读取引擎，默认为'openpyxl'
        dtype (dict): 列数据类型映射
        na_values (list): 识别为NaN的值列表
    """
    
    # PHQ-9严重程度分级标准
    PHQ9_SEVERITY_THRESHOLDS = {
        (0, 4): "Minimal",
        (5, 9): "Mild",
        (10, 14): "Moderate",
        (15, 19): "Moderately severe",
        (20, 27): "Severe"
    }
    
    # 预期的PHQ-9项目列名（支持多种命名）
    PHQ9_ITEM_COLUMNS = {
        'phq9_1', 'phq9_2', 'phq9_3', 'phq9_4', 'phq9_5', 
        'phq9_6', 'phq9_7', 'phq9_8', 'phq9_9',
        'item1', 'item2', 'item3', 'item4', 'item5',
        'item6', 'item7', 'item8', 'item9',
        'q1', 'q2', 'q3', 'q4', 'q5', 'q6', 'q7', 'q8', 'q9'
    }
    
    # 常见的人口统计学列名映射
    DEMOGRAPHIC_COLUMN_MAPPING = {
        'patient_id': ['patient_id', 'id', 'patientid', 'pid', '患者id', '编号'],
        'age': ['age', '年龄', '年纪', '岁数'],
        'gender': ['gender', '性别', 'sex', 'gender_code'],
        'occupation': ['occupation', '职业', 'job', '工作'],
        'education': ['education', '教育程度', '学历', '教育水平'],
        'marital_status': ['marital_status', '婚姻状况', '婚姻状态', 'marital'],
        'visit_date': ['visit_date', 'date', '就诊日期', 'assessment_date', '日期']
    }
    
    def __init__(self, engine: str = 'openpyxl', dtype: Optional[Dict] = None):
        """
        初始化Excel读取器
        
        参数:
            engine (str): pandas读取引擎，默认为'openpyxl'
            dtype (Dict): 列数据类型映射，默认为None（自动推断）
        """
        self.engine = engine
        self.dtype = dtype or {'patient_id': str}  # 默认将patient_id作为字符串
        
        # 配置NaN值识别
        self.na_values = ['', 'NA', 'N/A', 'NaN', 'null', 'NULL', 'None', '未知', '不详']
        
        logger.info(f"Excel读取器初始化完成，使用引擎: {engine}")
    
    def read_excel(self, file_path: str, sheet_name: Union[str, int, None] = 0) -> pd.DataFrame:
        """
        读取Excel文件
        
        参数:
            file_path (str): Excel文件路径
            sheet_name: 工作表名称或索引，默认为0（第一个工作表）
            
        返回:
            pd.DataFrame: 读取的DataFrame
            
        异常:
            FileNotFoundError: 文件不存在时抛出
            ValueError: 文件格式不支持时抛出
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel文件不存在: {file_path}")
        
        try:
            # 读取Excel文件
            df = pd.read_excel(
                file_path,
                engine=self.engine,
                dtype=self.dtype,
                na_values=self.na_values,
                sheet_name=sheet_name
            )
            
            # 如果读取的是字典（多个工作表），取第一个
            if isinstance(df, dict):
                first_sheet = list(df.keys())[0]
                df = df[first_sheet]
                logger.info(f"读取了多个工作表，使用第一个: {first_sheet}")
            
            logger.info(f"成功读取Excel文件: {file_path}")
            logger.info(f"数据形状: {df.shape[0]} 行 × {df.shape[1]} 列")
            logger.info(f"列名: {list(df.columns)}")
            
            return df
            
        except Exception as e:
            logger.error(f"读取Excel文件失败 {file_path}: {e}")
            raise
    
    def standardize_column_names(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        标准化列名，便于后续处理
        
        参数:
            df (pd.DataFrame): 原始DataFrame
            
        返回:
            pd.DataFrame: 列名标准化后的DataFrame
        """
        df_clean = df.copy()
        
        # 1. 去除列名中的空格和特殊字符
        df_clean.columns = df_clean.columns.astype(str).str.strip()
        
        # 2. 转换为小写（保留原始列名的映射）
        original_to_standard = {}
        new_columns = []
        
        for col in df_clean.columns:
            col_lower = col.lower()
            original_to_standard[col] = col_lower
            new_columns.append(col_lower)
        
        df_clean.columns = new_columns
        
        # 3. 记录列名映射
        self.column_mapping = original_to_standard
        logger.debug(f"列名标准化映射: {original_to_standard}")
        
        return df_clean
    
    def extract_demographics(self, row: pd.Series) -> PatientDemographics:
        """
        从数据行中提取人口统计学信息
        
        参数:
            row (pd.Series): 包含患者数据的行
            
        返回:
            PatientDemographics: 人口统计学信息对象
        """
        # 查找patient_id
        patient_id = None
        for possible_names in self.DEMOGRAPHIC_COLUMN_MAPPING['patient_id']:
            if possible_names in row.index:
                patient_id_value = row[possible_names]
                if pd.notna(patient_id_value):
                    patient_id = str(patient_id_value)
                break
        
        if not patient_id:
            logger.warning(f"未找到有效的patient_id: {row.to_dict()}")
            patient_id = "unknown"
        
        # 创建人口统计学对象
        demographics = PatientDemographics(patient_id=patient_id)
        
        # 提取其他人口统计学信息
        for field, possible_names in self.DEMOGRAPHIC_COLUMN_MAPPING.items():
            if field == 'patient_id':
                continue
                
            for name in possible_names:
                if name in row.index:
                    value = row[name]
                    if pd.isna(value):
                        continue
                    
                    # 特殊处理某些字段
                    if field == 'age':
                        try:
                            # 尝试转换为整数
                            if isinstance(value, (int, float, np.integer)):
                                value = int(value)
                            elif isinstance(value, str):
                                # 提取数字
                                import re
                                numbers = re.findall(r'\d+', value)
                                if numbers:
                                    value = int(numbers[0])
                                else:
                                    value = None
                            else:
                                value = None
                        except (ValueError, TypeError):
                            value = None
                    else:
                        # 其他字段转换为字符串
                        value = str(value)
                    
                    # 设置属性
                    setattr(demographics, field, value)
                    break
        
        # 收集其他未映射的列作为额外信息
        other_info = {}
        for col in row.index:
            if col not in demographics.__dict__:
                value = row[col]
                if pd.notna(value):
                    # 检查是否属于PHQ-9项目
                    is_phq9_item = any(phq_col in col for phq_col in self.PHQ9_ITEM_COLUMNS)
                    if not is_phq9_item:
                        other_info[col] = value
        
        demographics.other_info = other_info
        
        logger.debug(f"提取人口统计学信息: {patient_id} - 年龄: {demographics.age}, 性别: {demographics.gender}")
        return demographics
    
    def extract_phq9_assessment(self, row: pd.Series) -> Optional[PHQ9Assessment]:
        """
        从数据行中提取PHQ-9评估信息
        
        参数:
            row (pd.Series): 包含PHQ-9数据的行
            
        返回:
            Optional[PHQ9Assessment]: PHQ-9评估对象，如果未找到则返回None
        """
        # 查找PHQ-9项目分数
        phq9_items = []
        item_scores = {}
        
        for col in row.index:
            # 检查是否是PHQ-9项目列
            is_phq9_item = False
            for phq_pattern in self.PHQ9_ITEM_COLUMNS:
                if phq_pattern in col:
                    is_phq9_item = True
                    break
            
            if is_phq9_item:
                value = row[col]
                if pd.notna(value):
                    try:
                        score = int(float(value))
                        if 0 <= score <= 3:  # PHQ-9项目分数范围0-3
                            item_scores[col] = score
                    except (ValueError, TypeError):
                        logger.warning(f"PHQ-9项目分数格式错误: {col}={value}")
        
        # 如果没有找到PHQ-9项目，尝试查找总分
        if not item_scores:
            # 查找总分列
            total_score = None
            for total_col in ['total_score', 'phq9_total', 'total', '总分', 'phq9总分']:
                if total_col in row.index:
                    value = row[total_col]
                    if pd.notna(value):
                        try:
                            total_score = int(float(value))
                            break
                        except (ValueError, TypeError):
                            continue
            
            if total_score is None:
                logger.debug("未找到PHQ-9评估数据")
                return None
            
            # 如果没有细项分，创建占位符
            phq9_items = [0] * 9
        else:
            # 按项目编号排序
            sorted_items = sorted(item_scores.items(), key=lambda x: self._extract_item_number(x[0]))
            phq9_items = [score for _, score in sorted_items]
            
            # 计算总分
            total_score = sum(phq9_items)
        
        # 确定严重程度
        severity = self._determine_severity(total_score)
        
        # 查找评估日期
        assessment_date = None
        for date_col in ['assessment_date', 'date', 'test_date', '评估日期']:
            if date_col in row.index:
                value = row[date_col]
                if pd.notna(value):
                    assessment_date = str(value)
                    break
        
        # 创建PHQ-9评估对象
        assessment = PHQ9Assessment(
            total_score=total_score,
            severity=severity,
            items=phq9_items,
            assessment_date=assessment_date
        )
        
        logger.debug(f"提取PHQ-9评估: 总分={total_score}, 严重程度={severity}, 项目数={len(phq9_items)}")
        return assessment
    
    def _extract_item_number(self, column_name: str) -> int:
        """
        从列名中提取项目编号
        
        参数:
            column_name (str): 列名
            
        返回:
            int: 项目编号，如果无法提取则返回0
        """
        import re
        numbers = re.findall(r'\d+', column_name)
        if numbers:
            return int(numbers[0])
        return 0
    
    def _determine_severity(self, total_score: int) -> str:
        """
        根据PHQ-9总分确定严重程度
        
        参数:
            total_score (int): PHQ-9总分
            
        返回:
            str: 严重程度描述
        """
        for (min_score, max_score), severity in self.PHQ9_SEVERITY_THRESHOLDS.items():
            if min_score <= total_score <= max_score:
                return severity
        
        # 如果分数超出范围
        if total_score < 0:
            return "Invalid"
        elif total_score > 27:
            return "Severe"
        else:
            return "Unknown"
    
    def process_excel_data(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """
        处理整个Excel数据，提取所有患者信息
        
        参数:
            df (pd.DataFrame): 原始Excel数据
            
        返回:
            Dict[str, Dict[str, Any]]: 按patient_id组织的患者数据字典
        """
        # 标准化列名
        df_clean = self.standardize_column_names(df)
        
        # 初始化结果字典
        patients_data = {}
        
        # 处理每一行
        for idx, row in df_clean.iterrows():
            try:
                # 提取人口统计学信息
                demographics = self.extract_demographics(row)
                patient_id = demographics.patient_id
                
                # 提取PHQ-9评估
                phq9_assessment = self.extract_phq9_assessment(row)
                
                # 构建患者数据
                patient_data = {
                    "demographics": {
                        "age": demographics.age,
                        "gender": demographics.gender,
                        "occupation": demographics.occupation,
                        "education": demographics.education,
                        "marital_status": demographics.marital_status,
                        "visit_date": demographics.visit_date,
                        "other_info": demographics.other_info
                    },
                    "phq9_assessment": None
                }
                
                if phq9_assessment:
                    patient_data["phq9_assessment"] = {
                        "total_score": phq9_assessment.total_score,
                        "severity": phq9_assessment.severity,
                        "items": phq9_assessment.items,
                        "assessment_date": phq9_assessment.assessment_date
                    }
                
                # 添加到结果字典
                patients_data[patient_id] = patient_data
                
                logger.debug(f"处理患者数据: {patient_id} - 行 {idx+1}")
                
            except Exception as e:
                logger.error(f"处理行 {idx+1} 时出错: {e}")
                continue
        
        logger.info(f"Excel数据处理完成: 共处理 {len(patients_data)} 个患者")
        return patients_data
    
    def read_and_process(self, file_path: str) -> Dict[str, Dict[str, Any]]:
        """
        读取并处理Excel文件的完整流程
        
        参数:
            file_path (str): Excel文件路径
            
        返回:
            Dict[str, Dict[str, Any]]: 处理后的患者数据
        """
        logger.info(f"开始处理Excel文件: {file_path}")
        
        # 1. 读取Excel
        df = self.read_excel(file_path)
        
        # 2. 处理数据
        patients_data = self.process_excel_data(df)
        
        # 3. 统计信息
        total_patients = len(patients_data)
        patients_with_phq9 = sum(1 for data in patients_data.values() if data["phq9_assessment"])
        
        logger.info(f"处理完成: {total_patients} 个患者，其中 {patients_with_phq9} 个有PHQ-9评估")
        
        return patients_data


# 便捷函数
def read_excel_data(file_path: str) -> Dict[str, Dict[str, Any]]:
    """
    便捷函数：读取并处理Excel文件
    
    参数:
        file_path (str): Excel文件路径
        
    返回:
        Dict[str, Dict[str, Any]]: 处理后的患者数据
    """
    reader = ExcelReader()
    return reader.read_and_process(file_path)


def get_phq9_severity(total_score: int) -> str:
    """
    便捷函数：根据PHQ-9总分获取严重程度
    
    参数:
        total_score (int): PHQ-9总分
        
    返回:
        str: 严重程度描述
    """
    reader = ExcelReader()
    return reader._determine_severity(total_score)


if __name__ == "__main__":
    # 模块测试代码
    import sys
    
    if len(sys.argv) > 1:
        excel_file = sys.argv[1]
        
        if os.path.exists(excel_file):
            print(f"测试Excel文件处理: {ex