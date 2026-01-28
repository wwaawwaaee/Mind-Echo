#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据处理流水线测试脚本

本脚本用于测试各个数据处理模块的功能，确保整个流水线能够正常工作。
测试内容包括：
1. txt_reader模块测试
2. excel_reader_fixed模块测试  
3. data_merger模块测试
4. 完整流水线测试

作者: 数据管道团队
版本: 1.0.0
创建日期: 2026-01-28
"""

import os
import sys
import json
import tempfile
import shutil
from pathlib import Path

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

print("="*60)
print("数据处理流水线测试")
print("="*60)

def test_txt_reader():
    """测试txt_reader模块"""
    print("\n1. 测试txt_reader模块...")
    
    try:
        from txt_reader import TXTReader
        
        # 创建测试对话文件
        test_content = """医生：最近睡眠情况怎么样？
病人：非常糟糕，入睡很困难。
医生：这种情况持续多久了？
病人：大概有两个月了。"""
        
        # 创建临时目录和文件
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "P10086_visit_1.txt")
            with open(test_file, 'w', encoding='utf-8') as f:
                f.write(test_content)
            
            # 测试读取和解析
            reader = TXTReader()
            content = reader.read_file(test_file)
            dialogue = reader.parse_dialogue(content)
            
            print(f"  [OK] 成功读取文件: {len(content)} 字符")
            print(f"  [OK] 成功解析对话: {len(dialogue)} 个轮次")
            
            # 验证对话结构
            assert len(dialogue) == 4, f"预期4个对话轮次，实际得到{len(dialogue)}"
            assert dialogue[0]['role'] == 'doctor', "第一个轮次应该是医生"
            assert dialogue[1]['role'] == 'patient', "第二个轮次应该是病人"
            
            print("  [OK] 对话结构验证通过")
            
            # 测试文件名解析
            file_info = reader.extract_session_info("P10086_visit_1.txt")
            print(f"  [OK] 文件名解析: patient_id={file_info['patient_id']}, visit_type={file_info['visit_type']}")
            
            return True
            
    except Exception as e:
        print(f"  [FAIL] txt_reader测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_excel_reader():
    """测试excel_reader_fixed模块"""
    print("\n2. 测试excel_reader_fixed模块...")
    
    try:
        from excel_reader_fixed import ExcelReader
        
        # 创建测试Excel数据
        import pandas as pd
        import numpy as np
        
        test_data = {
            'patient_id': ['P10086', 'P10087', 'P10088'],
            'age': [34, 28, 45],
            'gender': ['female', 'male', 'female'],
            'occupation': ['teacher', 'engineer', 'doctor'],
            'phq9_total': [18, 12, 22],
            'phq9_1': [3, 2, 3],
            'phq9_2': [2, 1, 3],
            'phq9_3': [1, 1, 2]
        }
        
        # 创建临时Excel文件
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test_scores.xlsx")
            df = pd.DataFrame(test_data)
            df.to_excel(test_file, index=False)
            
            # 测试读取和解析
            reader = ExcelReader()
            patients_data = reader.read_and_process(test_file)
            
            print(f"  ✓ 成功读取Excel文件")
            print(f"  ✓ 处理了 {len(patients_data)} 个患者")
            
            # 验证数据
            if 'P10086' in patients_data:
                patient_data = patients_data['P10086']
                print(f"  ✓ 患者P10086数据:")
                print(f"    年龄: {patient_data['demographics']['age']}")
                print(f"    性别: {patient_data['demographics']['gender']}")
                
                if patient_data['phq9_assessment']:
                    phq9 = patient_data['phq9_assessment']
                    print(f"    PHQ-9总分: {phq9['total_score']}")
                    print(f"    严重程度: {phq9['severity']}")
            
            return True
            
    except Exception as e:
        print(f"  ✗ excel_reader测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_data_merger():
    """测试data_merger模块"""
    print("\n3. 测试data_merger模块...")
    
    try:
        from data_merger import DataMerger, merge_and_convert
        
        # 创建测试数据
        sample_excel_data = {
            "P10086": {
                "demographics": {
                    "age": 34,
                    "gender": "female",
                    "occupation": "teacher"
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
                    {"turn_id": 2, "role": "patient", "content": "非常糟糕，入睡很困难。"}
                ]
            }
        }
        
        # 测试合并和转换
        final_data = merge_and_convert(sample_excel_data, sample_dialogue_data)
        
        print(f"  ✓ 成功合并和转换数据")
        print(f"  ✓ 生成 {len(final_data)} 条记录")
        
        if final_data:
            record = final_data[0]
            print(f"  ✓ 记录结构验证:")
            print(f"    数据集ID: {record.get('dataset_id')}")
            print(f"    患者ID: {record.get('patient_id')}")
            print(f"    就诊记录数: {len(record.get('longitudinal_data', []))}")
            
            # 验证数据结构
            assert record['patient_id'] == 'P10086', "患者ID不匹配"
            assert 'demographics' in record, "缺少人口统计学信息"
            assert 'longitudinal_data' in record, "缺少纵向数据"
            
            print("  ✓ 数据结构验证通过")
        
        return True
        
    except Exception as e:
        print(f"  ✗ data_merger测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def test_integration():
    """测试完整流水线集成"""
    print("\n4. 测试完整流水线集成...")
    
    try:
        # 检查原始数据文件是否存在
        excel_path = "./raw_data/scores.xlsx"
        dialogue_dir = "./raw_data/dialogues/"
        
        if not os.path.exists(excel_path):
            print(f"  ⚠  Excel文件不存在: {excel_path}")
            print(f"    跳过实际数据测试")
            return True
        
        if not os.path.exists(dialogue_dir):
            print(f"  ⚠  对话目录不存在: {dialogue_dir}")
            print(f"    跳过实际数据测试")
            return True
        
        # 测试各个模块的导入
        print("  ✓ 模块导入测试:")
        
        try:
            from txt_reader import TXTReader
            print("    - txt_reader模块导入成功")
        except ImportError as e:
            print(f"    - txt_reader模块导入失败: {e}")
            return False
        
        try:
            from excel_reader_fixed import ExcelReader
            print("    - excel_reader_fixed模块导入成功")
        except ImportError as e:
            print(f"    - excel_reader_fixed模块导入失败: {e}")
            return False
        
        try:
            from data_merger import DataMerger
            print("    - data_merger模块导入成功")
        except ImportError as e:
            print(f"    - data_merger模块导入失败: {e}")
            return False
        
        try:
            from main_pipeline import DataPipeline
            print("    - main_pipeline模块导入成功")
        except ImportError as e:
            print(f"    - main_pipeline模块导入失败: {e}")
            return False
        
        # 测试主流水线配置
        config = {
            "excel_path": excel_path,
            "dialogue_dir": dialogue_dir,
            "output_dir": "./processed_data/test_output/",
            "dataset_id": "test_dataset_v1",
            "output_format": "json",
            "encoding": "utf-8"
        }
        
        pipeline = DataPipeline(config)
        
        # 验证路径
        if pipeline.validate_paths():
            print("  ✓ 路径验证通过")
        else:
            print("  ✗ 路径验证失败")
            return False
        
        print("  ✓ 完整流水线集成测试通过")
        return True
        
    except Exception as e:
        print(f"  ✗ 集成测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False

def create_sample_data():
    """创建示例数据用于测试"""
    print("\n5. 创建示例数据...")
    
    try:
        # 创建示例Excel文件
        import pandas as pd
        
        sample_excel_data = {
            'patient_id': ['P10086', 'P10087'],
            'age': [34, 28],
            'gender': ['female', 'male'],
            'occupation': ['teacher', 'engineer'],
            'phq9_total': [18, 12],
            'visit_date': ['2023-10-01', '2023-10-15']
        }
        
        df = pd.DataFrame(sample_excel_data)
        excel_path = "./raw_data/test_scores.xlsx"
        df.to_excel(excel_path, index=False)
        
        print(f"  ✓ 创建示例Excel文件: {excel_path}")
        
        # 创建示例对话文件
        dialogue_dir = "./raw_data/test_dialogues/"
        os.makedirs(dialogue_dir, exist_ok=True)
        
        # 患者1的对话
        dialogue1 = """医生：最近睡眠情况怎么样？
病人：非常糟糕，入睡很困难。
医生：这种情况持续多久了？
病人：大概有两个月了。"""
        
        with open(os.path.join(dialogue_dir, "P10086_visit_1.txt"), 'w', encoding='utf-8') as f:
            f.write(dialogue1)
        
        # 患者2的对话
        dialogue2 = """医生：今天感觉怎么样？
病人：比上次好一些，但还是容易焦虑。
医生：睡眠有改善吗？
病人：稍微好一点，但还是会早醒。"""
        
        with open(os.path.join(dialogue_dir, "P10087_visit_1.txt"), 'w', encoding='utf-8') as f:
            f.write(dialogue2)
        
        print(f"  ✓ 创建示例对话文件: {dialogue_dir}")
        
        return True
        
    except Exception as e:
        print(f"  ✗ 创建示例数据失败: {e}")
        return False

def run_complete_test():
    """运行完整的测试流程"""
    print("\n" + "="*60)
    print("运行完整测试流程")
    print("="*60)
    
    test_results = []
    
    # 运行各个测试
    test_results.append(("txt_reader模块", test_txt_reader()))
    test_results.append(("excel_reader模块", test_excel_reader()))
    test_results.append(("data_merger模块", test_data_merger()))
    test_results.append(("完整流水线集成", test_integration()))
    
    # 创建示例数据并测试
    if create_sample_data():
        test_results.append(("示例数据创建", True))
        
        # 使用示例数据测试主流水线
        print("\n6. 使用示例数据测试主流水线...")
        try:
            from main_pipeline import DataPipeline
            
            config = {
                "excel_path": "./raw_data/test_scores.xlsx",
                "dialogue_dir": "./raw_data/test_dialogues/",
                "output_dir": "./processed_data/test_output/",
                "dataset_id": "test_pipeline_v1",
                "output_format": "json",
                "encoding": "utf-8"
            }
            
            pipeline = DataPipeline(config)
            success = pipeline.run()
            
            if success:
                print("  ✓ 主流水线运行成功")
                test_results.append(("主流水线运行", True))
            else:
                print("  ✗ 主流水线运行失败")
                test_results.append(("主流水线运行", False))
                
        except Exception as e:
            print(f"  ✗ 主流水线测试失败: {e}")
            test_results.append(("主流水线运行", False))
    
    # 输出测试结果摘要
    print("\n" + "="*60)
    print("测试结果摘要")
    print("="*60)
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{test_name:30} {status}")
        
        if result:
            passed += 1
        else:
            failed += 1
    
    print("\n" + "="*60)
    print(f"总计: {len(test_results)} 个测试")
    print(f"通过: {passed}")
    print(f"失败: {failed}")
    print("="*60)
    
    if failed == 0:
        print("\n✅ 所有测试通过！数据处理流水线准备就绪。")
        return True
    else:
        print("\n❌ 部分测试失败，请检查错误信息。")
        return False

def main():
    """主函数"""
    try:
        success = run_complete_test()
        
        if success:
            print("\n使用说明:")
            print("1. 将Excel量表数据放入: ./raw_data/scores.xlsx")
            print("2. 将对话文本文件放入: ./raw_data/dialogues/")
            print("3. 运行主流水线: python main_pipeline.py")
            print("4. 或使用命令行参数:")
            print("   python main_pipeline.py --excel ./raw_data/scores.xlsx --dialogues ./raw_data/dialogues/")
            
            return 0
        else:
            return 1
            
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        return 1
    except Exception as e:
        print(f"\n测试过程中发生未预期错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())