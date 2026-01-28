#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Excel量表数据读取模块 (Excel Reader Module) - 简化版

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
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ExcelReader:
    """
    Excel量表数据读取器
    
    负责从Excel文件中读取患者信息和PHQ-9量表数据，
    进行数据清洗、验证和结构化转换。
    """
    
    # PHQ-9严重程度分级标准
    PHQ9_SEVERITY_THRESHOLDS = {
        (0, 4): "Minimal",
        (5, 9): "Mild",
        (10, 14): "Moderate",
        (15, 19): "Moderately severe",
        (20, 27): "Severe"
    }
    
    def __init__(self, engine='openpyxl'):
        """
        初始化Excel读取器
        
        参数:
            engine: pandas读取引擎，默认为'openpyxl'
        """
        self.engine = engine
        logger.info(f"Excel读取器初始化完成，使用引擎: {engine}")
    
    def read_excel(self, file_path, sheet_name=0):
        """
        读取Excel文件
        
        参数:
            file_path: Excel文件路径
            sheet_name: 工作表名称或索引，默认为0（第一个工作表）
            
        返回:
            pandas DataFrame: 读取的数据
            
        异常:
            FileNotFoundError: 文件不存在时抛出
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Excel文件不存在: {file_path}")
        
        try:
            # 读取Excel文件
            df = pd.read_excel(
                file_path,
                engine=self.engine,
                dtype={'patient_id': str},
                na_values=['', 'NA', 'N/A', 'NaN', 'null', 'NULL', 'None', '未知', '不详'],
                sheet_name=sheet_name
            )
            
            # 如果读取的是字典（多个工作表），取第一个
            if isinstance(df, dict):
                first_sheet = list(df.keys())[0]
                df = df[first_sheet]
                logger.info(f"读取了多个工作表，使用第一个: {first_sheet}")
            
            logger.info(f"成功读取Excel文件: {file_path}")
            logger.info(f"数据形状: {df.shape[0]} 行 × {df.shape[1]} 列")
            
            return df
            
        except Exception as e:
            logger.error(f"读取Excel文件失败 {file_path}: {e}")
            raise
    
    def standardize_column_names(self, df):
        """
        标准化列名，便于后续处理
        
        参数:
            df: 原始DataFrame
            
        返回:
            DataFrame: 列名标准化后的DataFrame
        """
        df_clean = df.copy()
        
        # 去除列名中的空格和特殊字符，转换为小写
        df_clean.columns = [str(col).strip().lower() for col in df_clean.columns]
        
        logger.debug(f"标准化后的列名: {list(df_clean.columns)}")
        return df_clean
    
    def extract_patient_info(self, row):
        """
        从数据行中提取患者信息
        
        参数:
            row: 包含患者数据的行（Series）
            
        返回:
            dict: 包含患者信息的字典
        """
        # 查找patient_id
        patient_id = None
        patient_id_keys = ['patient_id', 'id', 'patientid', 'pid', '患者id', '编号']
        
        for key in patient_id_keys:
            if key in row.index:
                value = row[key]
                if pd.notna(value):
                    patient_id = str(value)
                    break
        
        if not patient_id:
            logger.warning(f"未找到有效的patient_id")
            patient_id = "unknown"
        
        # 提取人口统计学信息
        demographics = {
            "age": None,
            "gender": None,
            "occupation": None,
            "education": None,
            "marital_status": None,
            "visit_date": None,
            "other_info": {}
        }
        
        # 年龄
        age_keys = ['age', '年龄', '年纪', '岁数']
        for key in age_keys:
            if key in row.index:
                value = row[key]
                if pd.notna(value):
                    try:
                        demographics["age"] = int(float(value))
                    except (ValueError, TypeError):
                        pass
                    break
        
        # 性别
        gender_keys = ['gender', '性别', 'sex', 'gender_code']
        for key in gender_keys:
            if key in row.index:
                value = row[key]
                if pd.notna(value):
                    demographics["gender"] = str(value)
                    break
        
        # 职业
        occupation_keys = ['occupation', '职业', 'job', '工作']
        for key in occupation_keys:
            if key in row.index:
                value = row[key]
                if pd.notna(value):
                    demographics["occupation"] = str(value)
                    break
        
        # 就诊日期
        date_keys = ['visit_date', 'date', '就诊日期', 'assessment_date', '日期']
        for key in date_keys:
            if key in row.index:
                value = row[key]
                if pd.notna(value):
                    demographics["visit_date"] = str(value)
                    break
        
        # 收集其他信息
        for col in row.index:
            if col not in ['patient_id', 'id'] and col not in age_keys + gender_keys + occupation_keys + date_keys:
                value = row[col]
                if pd.notna(value):
                    demographics["other_info"][col] = value
        
        return {
            "patient_id": patient_id,
            "demographics": demographics
        }
    
    def extract_phq9_assessment(self, row):
        """
        从数据行中提取PHQ-9评估信息
        
        参数:
            row: 包含PHQ-9数据的行
            
        返回:
            dict: PHQ-9评估信息，如果未找到则返回None
        """
        # PHQ-9项目列名模式
        phq9_patterns = ['phq9_', 'item', 'q']
        
        # 查找PHQ-9项目分数
        item_scores = {}
        
        for col in row.index:
            # 检查是否是PHQ-9项目列
            is_phq9_item = False
            for pattern in phq9_patterns:
                if pattern in col:
                    is_phq9_item = True
                    break
            
            if is_phq9_item:
                value = row[col]
                if pd.notna(value):
                    try:
                        score = float(value)
                        if score.is_integer() and 0 <= score <= 3:
                            item_scores[col] = int(score)
                    except (ValueError, TypeError):
                        pass
        
        # 如果没有找到PHQ-9项目，尝试查找总分
        if not item_scores:
            # 查找总分列
            total_score = None
            total_keys = ['total_score', 'phq9_total', 'total', '总分', 'phq9总分']
            
            for key in total_keys:
                if key in row.index:
                    value = row[key]
                    if pd.notna(value):
                        try:
                            total_score = int(float(value))
                            break
                        except (ValueError, TypeError):
                            continue
            
            if total_score is None:
                return None
            
            # 如果没有细项分，创建占位符
            phq9_items = [0] * 9
        else:
            # 按项目编号排序
            def extract_number(col_name):
                import re
                numbers = re.findall(r'\d+', col_name)
                return int(numbers[0]) if numbers else 0
            
            sorted_items = sorted(item_scores.items(), key=lambda x: extract_number(x[0]))
            phq9_items = [score for _, score in sorted_items]
            
            # 计算总分
            total_score = sum(phq9_items)
        
        # 确定严重程度
        severity = self._determine_severity(total_score)
        
        # 查找评估日期
        assessment_date = None
        date_keys = ['assessment_date', 'date', 'test_date', '评估日期']
        for key in date_keys:
            if key in row.index:
                value = row[key]
                if pd.notna(value):
                    assessment_date = str(value)
                    break
        
        return {
            "total_score": total_score,
            "severity": severity,
            "items": phq9_items,
            "assessment_date": assessment_date
        }
    
    def _determine_severity(self, total_score):
        """
        根据PHQ-9总分确定严重程度
        
        参数:
            total_score: PHQ-9总分
            
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
    
    def process_excel_data(self, df):
        """
        处理整个Excel数据，提取所有患者信息
        
        参数:
            df: 原始Excel数据
            
        返回:
            dict: 按patient_id组织的患者数据字典
        """
        # 标准化列名
        df_clean = self.standardize_column_names(df)
        
        # 初始化结果字典
        patients_data = {}
        
        # 处理每一行
        for idx, row in df_clean.iterrows():
            try:
                # 提取患者基本信息
                patient_info = self.extract_patient_info(row)
                patient_id = patient_info["patient_id"]
                
                # 提取PHQ-9评估
                phq9_assessment = self.extract_phq9_assessment(row)
                
                # 构建患者数据
                patient_data = {
                    "demographics": patient_info["demographics"],
                    "phq9_assessment": phq9_assessment
                }
                
                # 添加到结果字典
                patients_data[patient_id] = patient_data
                
                logger.debug(f"处理患者数据: {patient_id} - 行 {idx+1}")
                
            except Exception as e:
                logger.error(f"处理行 {idx+1} 时出错: {e}")
                continue
        
        logger.info(f"Excel数据处理完成: 共处理 {len(patients_data)} 个患者")
        return patients_data
    
    def read_and_process(self, file_path):
        """
        读取并处理Excel文件的完整流程
        
        参数:
            file_path: Excel文件路径
            
        返回:
            dict: 处理后的患者数据
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
def read_excel_data(file_path):
    """
    便捷函数：读取并处理Excel文件
    
    参数:
        file_path: Excel文件路径
        
    返回:
        dict: 处理后的患者数据
    """
    reader = ExcelReader()
    return reader.read_and_process(file_path)


def get_phq9_severity(total_score):
    """
    便捷函数：根据PHQ-9总分获取严重程度
    
    参数:
        total_score: PHQ-9总分
        
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
            print(f"测试Excel文件处理: {excel_file}")
            
            try:
                patients_data = read_excel_data(excel_file)
                print(f"成功处理 {len(patients_data)} 个患者")
                
                # 显示前3个患者的信息
                for i, (patient_id, data) in enumerate(list(patients_data.items())[:3]):
                    print(f"\n患者 {i+1}: {patient_id}")
                    print(f"  年龄: {data['demographics']['age']}")
                    print(f"  性别: {data['demographics']['gender']}")
                    if data['phq9_assessment']:
                        print(f"  PHQ-9总分: {data['phq9_assessment']['total_score']}")
                        print(f"  严重程度: {data['phq9_assessment']['severity']}")
                
            except Exception as e:
                print(f"处理失败: {e}")
        else:
            print(f"文件不存在: {excel_file}")
    else:
        print("Excel读取模块 - 简化版")
        print("用法: python excel_reader_fixed.py <Excel文件路径>")
        print("\n示例:")
        print("  python excel_reader_fixed.py ./raw_data/scores.xlsx")