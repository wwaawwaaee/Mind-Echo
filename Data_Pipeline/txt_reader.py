#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
对话文本文件读取模块 (TXT Reader Module)

本模块负责读取和解析对话文本文件，将原始文本转换为结构化的对话数据。
支持从txt文件中提取医生和病人的对话轮次，并生成符合OpenAI格式的对话结构。

主要功能：
1. 读取单个或多个txt文件
2. 解析对话文本，识别说话者角色
3. 提取对话轮次并添加turn_id
4. 支持多种对话格式（中文/英文标识）

作者: 数据管道团队
版本: 1.0.0
创建日期: 2026-01-28
"""

import os
import re
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class DialogueTurn:
    """对话轮次数据类"""
    turn_id: int
    role: str  # "doctor", "patient", "unknown"
    content: str
    raw_line: str  # 原始文本行


class TXTReader:
    """
    对话文本文件读取器
    
    负责从txt文件中读取对话内容，解析为结构化的对话轮次列表。
    支持自动识别说话者角色，处理多种对话格式。
    
    属性:
        encoding (str): 文件编码，默认为utf-8
        role_patterns (dict): 角色识别模式字典
    """
    
    def __init__(self, encoding: str = "utf-8"):
        """
        初始化TXT读取器
        
        参数:
            encoding (str): 文件编码格式，默认为utf-8
        """
        self.encoding = encoding
        
        # 定义角色识别模式（支持中文和英文）
        self.role_patterns = {
            "doctor": [
                r"^医生[:：]\s*(.*)",      # 中文格式：医生：
                r"^Doctor[:：]\s*(.*)",    # 英文格式：Doctor:
                r"^Dr\.\s*(.*)",           # 英文缩写：Dr.
                r"^D[:：]\s*(.*)",         # 简写：D:
            ],
            "patient": [
                r"^病人[:：]\s*(.*)",      # 中文格式：病人：
                r"^Patient[:：]\s*(.*)",   # 英文格式：Patient:
                r"^P[:：]\s*(.*)",         # 简写：P:
                r"^患者[:：]\s*(.*)",      # 中文：患者：
            ]
        }
    
    def read_file(self, file_path: str) -> str:
        """
        读取单个txt文件内容
        
        参数:
            file_path (str): txt文件路径
            
        返回:
            str: 文件内容字符串
            
        异常:
            FileNotFoundError: 文件不存在时抛出
            UnicodeDecodeError: 编码错误时抛出
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")
        
        try:
            with open(file_path, 'r', encoding=self.encoding) as f:
                content = f.read()
            logger.info(f"成功读取文件: {file_path} (大小: {len(content)} 字符)")
            return content
        except UnicodeDecodeError as e:
            logger.error(f"编码错误: {file_path} - {e}")
            raise
    
    def parse_dialogue(self, text: str) -> List[Dict[str, Any]]:
        """
        解析对话文本，转换为结构化对话列表
        
        参数:
            text (str): 原始对话文本
            
        返回:
            List[Dict[str, Any]]: 结构化对话列表，每个元素包含turn_id, role, content
            
        示例:
            >>> reader = TXTReader()
            >>> dialogue = reader.parse_dialogue("医生：你好吗？\\n病人：我很好")
            >>> print(dialogue)
            [
                {"turn_id": 1, "role": "doctor", "content": "你好吗？"},
                {"turn_id": 2, "role": "patient", "content": "我很好"}
            ]
        """
        lines = text.split('\n')
        dialogue_turns = []
        turn_id = 1
        
        for line_num, line in enumerate(lines, 1):
            line = line.strip()
            if not line:  # 跳过空行
                continue
            
            # 识别角色和内容
            role, content = self._identify_role_and_content(line)
            
            # 创建对话轮次
            turn = {
                "turn_id": turn_id,
                "role": role,
                "content": content,
                "line_number": line_num
            }
            
            dialogue_turns.append(turn)
            turn_id += 1
        
        logger.debug(f"解析完成: 共 {len(dialogue_turns)} 个对话轮次")
        return dialogue_turns
    
    def _identify_role_and_content(self, line: str) -> tuple[str, str]:
        """
        识别一行文本中的说话者角色和内容
        
        参数:
            line (str): 原始文本行
            
        返回:
            tuple[str, str]: (角色, 内容)
        """
        # 默认值
        role = "unknown"
        content = line
        
        # 检查医生模式
        for pattern in self.role_patterns["doctor"]:
            match = re.match(pattern, line)
            if match:
                role = "doctor"
                content = match.group(1).strip()
                return role, content
        
        # 检查病人模式
        for pattern in self.role_patterns["patient"]:
            match = re.match(pattern, line)
            if match:
                role = "patient"
                content = match.group(1).strip()
                return role, content
        
        # 如果没有匹配到任何模式，尝试基于关键词判断
        if "医生" in line or "Doctor" in line or "Dr." in line:
            role = "doctor"
            # 尝试提取冒号后的内容
            parts = re.split(r'[:：]', line, 1)
            if len(parts) > 1:
                content = parts[1].strip()
        elif "病人" in line or "Patient" in line or "患者" in line:
            role = "patient"
            parts = re.split(r'[:：]', line, 1)
            if len(parts) > 1:
                content = parts[1].strip()
        
        return role, content
    
    def extract_session_info(self, filename: str) -> Dict[str, Any]:
        """
        从文件名中提取会话信息
        
        参数:
            filename (str): 文件名（如 "P10086_visit_1.txt"）
            
        返回:
            Dict[str, Any]: 包含patient_id, session_id, visit_type等信息的字典
            
        示例:
            >>> info = reader.extract_session_info("P10086_visit_1.txt")
            >>> print(info)
            {
                "patient_id": "P10086",
                "session_id": "P10086_visit_1",
                "visit_type": "visit_1",
                "visit_number": 1,
                "is_initial": True
            }
        """
        # 移除扩展名
        name_without_ext = os.path.splitext(filename)[0]
        
        # 尝试解析文件名格式：patientId_visitNumber 或 patientId_sessionType
        info = {
            "original_filename": filename,
            "patient_id": "unknown",
            "session_id": name_without_ext,
            "visit_type": "unknown",
            "visit_number": None,
            "is_initial": False
        }
        
        # 常见模式匹配
        patterns = [
            r"^(P\d+)_(visit|session|v|s)_?(\d+)$",  # P10086_visit_1
            r"^(P\d+)_(\d+)$",                       # P10086_1
            r"^(P\d+)_(initial|followup|follow_up)$", # P10086_initial
            r"^(P\d+)_(first|second|third)$",        # P10086_first
        ]
        
        for pattern in patterns:
            match = re.match(pattern, name_without_ext, re.IGNORECASE)
            if match:
                info["patient_id"] = match.group(1)
                
                if len(match.groups()) >= 2:
                    visit_part = match.group(2).lower()
                    
                    # 确定就诊类型
                    if visit_part in ["initial", "first", "1"]:
                        info["visit_type"] = "initial"
                        info["is_initial"] = True
                    elif visit_part in ["followup", "follow_up", "second", "2"]:
                        info["visit_type"] = "follow_up"
                    else:
                        info["visit_type"] = visit_part
                    
                    # 提取就诊编号
                    if len(match.groups()) >= 3:
                        try:
                            info["visit_number"] = int(match.group(3))
                        except ValueError:
                            info["visit_number"] = None
                
                break
        
        # 如果模式不匹配，尝试简单分割
        if info["patient_id"] == "unknown" and "_" in name_without_ext:
            parts = name_without_ext.split("_")
            if len(parts) >= 2:
                info["patient_id"] = parts[0]
                info["visit_type"] = parts[1]
        
        logger.debug(f"从文件名提取信息: {filename} -> {info}")
        return info
    
    def process_directory(self, directory_path: str) -> Dict[str, List[Dict[str, Any]]]:
        """
        处理目录下的所有txt文件
        
        参数:
            directory_path (str): 包含对话txt文件的目录路径
            
        返回:
            Dict[str, List[Dict[str, Any]]]: 按文件名组织的对话数据字典
            
        异常:
            NotADirectoryError: 路径不是目录时抛出
        """
        if not os.path.isdir(directory_path):
            raise NotADirectoryError(f"路径不是目录: {directory_path}")
        
        all_dialogues = {}
        txt_files = [f for f in os.listdir(directory_path) if f.endswith('.txt')]
        
        logger.info(f"在目录 {directory_path} 中找到 {len(txt_files)} 个txt文件")
        
        for filename in txt_files:
            try:
                file_path = os.path.join(directory_path, filename)
                content = self.read_file(file_path)
                dialogue = self.parse_dialogue(content)
                
                all_dialogues[filename] = {
                    "file_info": self.extract_session_info(filename),
                    "dialogue": dialogue,
                    "raw_content": content[:500] + "..." if len(content) > 500 else content  # 保留部分原始内容
                }
                
                logger.debug(f"处理文件完成: {filename} - {len(dialogue)} 个对话轮次")
                
            except Exception as e:
                logger.error(f"处理文件失败 {filename}: {e}")
                all_dialogues[filename] = {
                    "file_info": self.extract_session_info(filename),
                    "dialogue": [],
                    "error": str(e)
                }
        
        return all_dialogues


# 便捷函数
def read_and_parse_txt(file_path: str, encoding: str = "utf-8") -> List[Dict[str, Any]]:
    """
    便捷函数：读取并解析单个txt文件
    
    参数:
        file_path (str): txt文件路径
        encoding (str): 文件编码，默认为utf-8
        
    返回:
        List[Dict[str, Any]]: 结构化对话列表
    """
    reader = TXTReader(encoding=encoding)
    content = reader.read_file(file_path)
    return reader.parse_dialogue(content)


def process_txt_directory(directory_path: str, encoding: str = "utf-8") -> Dict[str, List[Dict[str, Any]]]:
    """
    便捷函数：处理目录下的所有txt文件
    
    参数:
        directory_path (str): 包含对话txt文件的目录路径
        encoding (str): 文件编码，默认为utf-8
        
    返回:
        Dict[str, List[Dict[str, Any]]]: 按文件名组织的对话数据字典
    """
    reader = TXTReader(encoding=encoding)
    return reader.process_directory(directory_path)


if __name__ == "__main__":
    # 模块测试代码
    import sys
    
    if len(sys.argv) > 1:
        test_file = sys.argv[1]
        if os.path.isdir(test_file):
            print(f"测试目录处理: {test_file}")
            result = process_txt_directory(test_file)
            print(f"处理了 {len(result)} 个文件")
            for filename, data in result.items():
                print(f"\n{filename}:")
                print(f"  患者ID: {data['file_info']['patient_id']}")
                print(f"  就诊类型: {data['file_info']['visit_type']}")
                print(f"  对话轮次: {len(data['dialogue'])}")
        else:
            print(f"测试单个文件: {test_file}")
            dialogue = read_and_parse_txt(test_file)
            print(f"解析了 {len(dialogue)} 个对话轮次:")
            for turn in dialogue[:5]:  # 只显示前5个
                print(f"  [{turn['turn_id']}] {turn['role']}: {turn['content']}")
            if len(dialogue) > 5:
                print(f"  ... 还有 {len(dialogue)-5} 个轮次")
    else:
        print("TXT读取模块")
        print("用法: python txt_reader.py <文件或目录路径>")
        print("\n示例:")
        print("  python txt_reader.py ./raw_data/dialogues/")
        print("  python txt_reader.py ./raw_data/dialogues/P10086_visit_1.txt")