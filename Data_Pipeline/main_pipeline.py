#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
主数据处理流水线 (Main Data Processing Pipeline)

本程序整合所有数据处理模块，实现完整的数据处理流程：
1. 读取Excel量表数据
2. 读取对话文本数据
3. 合并并转换数据格式
4. 生成符合要求的JSON数据集

模块依赖：
- txt_reader.py: 对话文本读取模块
- excel_reader_fixed.py: Excel数据读取模块
- data_merger.py: 数据合并与转换模块

作者: 数据管道团队
版本: 1.0.0
创建日期: 2026-01-28
"""

import os
import sys
import argparse
import logging
from datetime import datetime

# 添加当前目录到Python路径，以便导入模块
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# 导入自定义模块
try:
    from txt_reader import process_txt_directory
    from excel_reader_fixed import read_excel_data
    from data_merger import merge_and_convert, save_dataset, DataMerger
except ImportError as e:
    print(f"导入模块失败: {e}")
    print("请确保以下文件存在:")
    print("  - txt_reader.py")
    print("  - excel_reader_fixed.py")
    print("  - data_merger.py")
    sys.exit(1)

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"data_pipeline_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class DataPipeline:
    """
    主数据处理流水线
    
    协调各个模块，完成从原始数据到结构化JSON的完整处理流程。
    """
    
    def __init__(self, config=None):
        """
        初始化数据处理流水线
        
        参数:
            config: 配置参数，字典类型
        """
        self.config = config or {}
        
        # 默认配置
        self.default_config = {
            "excel_path": "./raw_data/scores.xlsx",
            "dialogue_dir": "./raw_data/dialogues/",
            "output_dir": "./processed_data/",
            "dataset_id": "outpatient_depression_v1",
            "output_format": "json",  # json 或 jsonl
            "encoding": "utf-8"
        }
        
        # 更新配置
        self.default_config.update(self.config)
        self.config = self.default_config
        
        logger.info("数据处理流水线初始化完成")
        logger.info(f"配置参数: {self.config}")
    
    def validate_paths(self) -> bool:
        """
        验证输入路径是否存在
        
        返回:
            bool: 所有路径有效返回True，否则返回False
        """
        errors = []
        
        # 检查Excel文件
        excel_path = self.config["excel_path"]
        if not os.path.exists(excel_path):
            errors.append(f"Excel文件不存在: {excel_path}")
        
        # 检查对话目录
        dialogue_dir = self.config["dialogue_dir"]
        if not os.path.exists(dialogue_dir):
            errors.append(f"对话目录不存在: {dialogue_dir}")
        elif not os.path.isdir(dialogue_dir):
            errors.append(f"对话路径不是目录: {dialogue_dir}")
        
        # 检查输出目录（如果不存在则创建）
        output_dir = self.config["output_dir"]
        if not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir, exist_ok=True)
                logger.info(f"创建输出目录: {output_dir}")
            except Exception as e:
                errors.append(f"无法创建输出目录 {output_dir}: {e}")
        
        if errors:
            for error in errors:
                logger.error(error)
            return False
        
        return True
    
    def read_excel_data(self) -> dict:
        """
        读取Excel量表数据
        
        返回:
            dict: Excel处理后的患者数据
        """
        excel_path = self.config["excel_path"]
        logger.info(f"开始读取Excel数据: {excel_path}")
        
        try:
            excel_data = read_excel_data(excel_path)
            logger.info(f"Excel数据读取完成: {len(excel_data)} 个患者")
            return excel_data
        except Exception as e:
            logger.error(f"读取Excel数据失败: {e}")
            raise
    
    def read_dialogue_data(self) -> dict:
        """
        读取对话文本数据
        
        返回:
            dict: 对话处理后的数据
        """
        dialogue_dir = self.config["dialogue_dir"]
        encoding = self.config["encoding"]
        logger.info(f"开始读取对话数据: {dialogue_dir}")
        
        try:
            # 使用txt_reader模块处理目录
            from txt_reader import TXTReader
            reader = TXTReader(encoding=encoding)
            dialogue_data = reader.process_directory(dialogue_dir)
            
            # 统计信息
            total_files = len(dialogue_data)
            total_turns = 0
            for data in dialogue_data.values():
                if isinstance(data, dict) and "dialogue" in data:
                    total_turns += len(data["dialogue"])
            
            logger.info(f"对话数据读取完成: {total_files} 个文件，{total_turns} 个对话轮次")
            return dialogue_data
        except Exception as e:
            logger.error(f"读取对话数据失败: {e}")
            raise
    
    def merge_and_convert_data(self, excel_data: dict, dialogue_data: dict) -> list:
        """
        合并并转换数据
        
        参数:
            excel_data (dict): Excel处理后的患者数据
            dialogue_data (dict): 对话处理后的数据
            
        返回:
            list: 转换后的JSON数据
        """
        dataset_id = self.config["dataset_id"]
        logger.info(f"开始合并和转换数据，数据集ID: {dataset_id}")
        
        try:
            final_data = merge_and_convert(excel_data, dialogue_data, dataset_id)
            logger.info(f"数据转换完成: {len(final_data)} 条记录")
            return final_data
        except Exception as e:
            logger.error(f"数据合并转换失败: {e}")
            raise
    
    def save_output(self, data: list):
        """
        保存输出数据
        
        参数:
            data (list): 要保存的数据
        """
        output_dir = self.config["output_dir"]
        output_format = self.config["output_format"]
        dataset_id = self.config["dataset_id"]
        
        # 生成输出文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if output_format.lower() == "jsonl":
            output_filename = f"{dataset_id}_{timestamp}.jsonl"
            output_path = os.path.join(output_dir, output_filename)
            
            logger.info(f"保存为JSONL格式: {output_path}")
            save_dataset(data, output_path, format="jsonl")
        else:
            output_filename = f"{dataset_id}_{timestamp}.json"
            output_path = os.path.join(output_dir, output_filename)
            
            logger.info(f"保存为JSON格式: {output_path}")
            save_dataset(data, output_path, format="json")
        
        # 同时保存一个统计信息文件
        stats_filename = f"{dataset_id}_{timestamp}_stats.json"
        stats_path = os.path.join(output_dir, stats_filename)
        
        try:
            merger = DataMerger(dataset_id=dataset_id)
            stats = merger.generate_statistics(data)
            
            import json
            with open(stats_path, 'w', encoding='utf-8') as f:
                json.dump(stats, f, ensure_ascii=False, indent=2)
            
            logger.info(f"统计信息已保存: {stats_path}")
            
            # 打印统计摘要
            self._print_statistics(stats)
            
        except Exception as e:
            logger.warning(f"保存统计信息失败: {e}")
    
    def _print_statistics(self, stats: dict):
        """
        打印统计信息
        
        参数:
            stats (dict): 统计信息
        """
        print("\n" + "="*60)
        print("数据统计摘要")
        print("="*60)
        print(f"总患者数: {stats.get('total_patients', 0)}")
        print(f"有人口统计学信息的患者: {stats.get('patients_with_demographics', 0)}")
        print(f"有PHQ-9评估的患者: {stats.get('patients_with_phq9', 0)}")
        print(f"总就诊次数: {stats.get('total_visits', 0)}")
        print(f"总对话轮次: {stats.get('total_dialogue_turns', 0)}")
        
        # 就诊类型分布
        visit_dist = stats.get('visit_type_distribution', {})
        if visit_dist:
            print("\n就诊类型分布:")
            for visit_type, count in visit_dist.items():
                print(f"  {visit_type}: {count}")
        
        # 性别分布
        gender_dist = stats.get('gender_distribution', {})
        if gender_dist:
            print("\n性别分布:")
            for gender, count in gender_dist.items():
                print(f"  {gender}: {count}")
        
        print("="*60)
    
    def run(self) -> bool:
        """
        运行完整的数据处理流水线
        
        返回:
            bool: 处理成功返回True，否则返回False
        """
        logger.info("="*60)
        logger.info("开始数据处理流水线")
        logger.info("="*60)
        
        try:
            # 1. 验证路径
            if not self.validate_paths():
                return False
            
            # 2. 读取Excel数据
            excel_data = self.read_excel_data()
            
            # 3. 读取对话数据
            dialogue_data = self.read_dialogue_data()
            
            # 4. 合并和转换数据
            final_data = self.merge_and_convert_data(excel_data, dialogue_data)
            
            if not final_data:
                logger.warning("没有生成任何数据记录")
                return False
            
            # 5. 保存输出
            self.save_output(final_data)
            
            logger.info("="*60)
            logger.info("数据处理流水线完成")
            logger.info("="*60)
            
            return True
            
        except Exception as e:
            logger.error(f"数据处理流水线失败: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False


def parse_arguments():
    """
    解析命令行参数
    
    返回:
        argparse.Namespace: 解析后的参数
    """
    parser = argparse.ArgumentParser(
        description="数据处理流水线 - 将Excel量表数据和对话文本数据合并为结构化JSON"
    )
    
    parser.add_argument(
        "--excel",
        "-e",
        type=str,
        default="./raw_data/scores.xlsx",
        help="Excel量表数据文件路径 (默认: ./raw_data/scores.xlsx)"
    )
    
    parser.add_argument(
        "--dialogues",
        "-d",
        type=str,
        default="./raw_data/dialogues/",
        help="对话文本文件目录路径 (默认: ./raw_data/dialogues/)"
    )
    
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="./processed_data/",
        help="输出目录路径 (默认: ./processed_data/)"
    )
    
    parser.add_argument(
        "--dataset-id",
        "-i",
        type=str,
        default="outpatient_depression_v1",
        help="数据集标识符 (默认: outpatient_depression_v1)"
    )
    
    parser.add_argument(
        "--format",
        "-f",
        type=str,
        choices=["json", "jsonl"],
        default="json",
        help="输出格式: json 或 jsonl (默认: json)"
    )
    
    parser.add_argument(
        "--encoding",
        "-c",
        type=str,
        default="utf-8",
        help="文本文件编码 (默认: utf-8)"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="启用详细日志输出"
    )
    
    return parser.parse_args()


def main():
    """
    主函数
    """
    # 解析命令行参数
    args = parse_arguments()
    
    # 设置日志级别
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # 构建配置
    config = {
        "excel_path": args.excel,
        "dialogue_dir": args.dialogues,
        "output_dir": args.output,
        "dataset_id": args.dataset_id,
        "output_format": args.format,
        "encoding": args.encoding
    }
    
    # 创建并运行流水线
    pipeline = DataPipeline(config)
    success = pipeline.run()
    
    if success:
        print("\n✅ 数据处理完成！")
        return 0
    else:
        print("\n❌ 数据处理失败！")
        return 1


if __name__ == "__main__":
    # 运行主函数
    sys.exit(main())