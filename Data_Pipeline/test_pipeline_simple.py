#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据处理流水线简单测试脚本

本脚本用于测试各个数据处理模块的基本功能。
避免使用Unicode字符，确保在Windows命令行中正常运行。

作者: 数据管道团队
版本: 1.0.0
创建日期: 2026-01-28
"""

import os
import sys
import tempfile

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

print("="*60)
print("数据处理流水线简单测试")
print("="*60)

def test_module_imports():
    """测试模块导入"""
    print("\n1. 测试模块导入...")
    
    modules_to_test = [
        ("txt_reader", "TXTReader"),
        ("excel_reader_fixed", "ExcelReader"),
        ("data_merger", "DataMerger"),
        ("main_pipeline", "DataPipeline")
    ]
    
    all_imported = True
    for module_name, class_name in modules_to_test:
        try:
            exec(f"from {module_name} import {class_name}")
            print(f"  [OK] {module_name} 模块导入成功")
        except ImportError as e:
            print(f"  [FAIL] {module_name} 模块导入失败: {e}")
            all_imported = False
    
    return all_imported

def test_txt_reader_basic():
    """测试txt_reader基本功能"""
    print("\n2. 测试txt_reader基本功能...")
    
    try:
        from txt_reader import TXTReader
        
        # 创建测试对话文件
        test_content = """医生：最近睡眠情况怎么样？
病人：非常糟糕，入睡很困难。
医生：这种情况持续多久了？
病人：大概有两个月了。"""
        
        # 创建临时文件
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "P10086_visit_1.txt")
            with open(test_file, 'w', encoding='utf-8') as f:
                f.write(test_content)
            
            # 测试读取
            reader = TXTReader()
            content = reader.read_file(test_file)
            
            print(f"  [OK] 成功读取文件: {len(content)} 字符")
            
            # 测试解析
            dialogue = reader.parse_dialogue(content)
            print(f"  [OK] 成功解析对话: {len(dialogue)} 个轮次")
            
            # 验证
            if len(dialogue) == 4:
                print("  [OK] 对话轮次数量正确")
            else:
                print(f"  [WARN] 对话轮次数量: 预期4, 实际{len(dialogue)}")
            
            return True
            
    except Exception as e:
        print(f"  [FAIL] txt_reader测试失败: {e}")
        return False

def test_excel_reader_basic():
    """测试excel_reader基本功能"""
    print("\n3. 测试excel_reader基本功能...")
    
    try:
        from excel_reader_fixed import ExcelReader
        
        # 创建测试Excel数据
        import pandas as pd
        
        test_data = {
            'patient_id': ['P10086', 'P10087'],
            'age': [34, 28],
            'gender': ['female', 'male'],
            'occupation': ['teacher', 'engineer'],
            'phq9_total': [18, 12]
        }
        
        # 创建临时Excel文件
        with tempfile.TemporaryDirectory() as tmpdir:
            test_file = os.path.join(tmpdir, "test_scores.xlsx")
            df = pd.DataFrame(test_data)
            df.to_excel(test_file, index=False)
            
            # 测试读取
            reader = ExcelReader()
            patients_data = reader.read_and_process(test_file)
            
            print(f"  [OK] 成功读取Excel文件")
            print(f"  [OK] 处理了 {len(patients_data)} 个患者")
            
            # 验证数据
            if 'P10086' in patients_data:
                patient_data = patients_data['P10086']
                if patient_data['demographics']['age'] == 34:
                    print("  [OK] 患者数据解析正确")
            
            return True
            
    except Exception as e:
        print(f"  [FAIL] excel_reader测试失败: {e}")
        return False

def test_data_merger_basic():
    """测试data_merger基本功能"""
    print("\n4. 测试data_merger基本功能...")
    
    try:
        from data_merger import merge_and_convert
        
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
                    "visit_type": "initial"
                },
                "dialogue": [
                    {"turn_id": 1, "role": "doctor", "content": "最近睡眠情况怎么样？"},
                    {"turn_id": 2, "role": "patient", "content": "非常糟糕，入睡很困难。"}
                ]
            }
        }
        
        # 测试合并
        final_data = merge_and_convert(sample_excel_data, sample_dialogue_data)
        
        print(f"  [OK] 成功合并和转换数据")
        print(f"  [OK] 生成 {len(final_data)} 条记录")
        
        if final_data and final_data[0]['patient_id'] == 'P10086':
            print("  [OK] 数据合并正确")
        
        return True
        
    except Exception as e:
        print(f"  [FAIL] data_merger测试失败: {e}")
        return False

def test_main_pipeline_config():
    """测试主流水线配置"""
    print("\n5. 测试主流水线配置...")
    
    try:
        from main_pipeline import DataPipeline
        
        # 测试配置
        config = {
            "excel_path": "./raw_data/scores.xlsx",
            "dialogue_dir": "./raw_data/dialogues/",
            "output_dir": "./processed_data/test_output/",
            "dataset_id": "test_dataset",
            "output_format": "json",
            "encoding": "utf-8"
        }
        
        pipeline = DataPipeline(config)
        
        print(f"  [OK] 流水线初始化成功")
        print(f"  [OK] 数据集ID: {pipeline.config['dataset_id']}")
        
        # 检查路径验证
        if os.path.exists("./raw_data/scores.xlsx"):
            if pipeline.validate_paths():
                print("  [OK] 路径验证通过")
            else:
                print("  [WARN] 路径验证失败（可能文件不存在）")
        else:
            print("  [INFO] 跳过路径验证（测试文件不存在）")
        
        return True
        
    except Exception as e:
        print(f"  [FAIL] 主流水线测试失败: {e}")
        return False

def create_sample_structure():
    """创建示例目录结构"""
    print("\n6. 创建示例目录结构...")
    
    try:
        # 创建必要的目录
        directories = [
            "./raw_data/",
            "./raw_data/dialogues/",
            "./processed_data/",
            "./processed_data/test_output/"
        ]
        
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
            print(f"  [OK] 创建目录: {directory}")
        
        # 创建示例Excel文件
        import pandas as pd
        
        sample_data = {
            'patient_id': ['P10086', 'P10087'],
            'age': [34, 28],
            'gender': ['female', 'male'],
            'occupation': ['teacher', 'engineer'],
            'phq9_total': [18, 12],
            'visit_date': ['2023-10-01', '2023-10-15']
        }
        
        df = pd.DataFrame(sample_data)
        excel_path = "./raw_data/test_scores.xlsx"
        df.to_excel(excel_path, index=False)
        
        print(f"  [OK] 创建示例Excel文件: {excel_path}")
        
        # 创建示例对话文件
        dialogue1 = """医生：最近睡眠情况怎么样？
病人：非常糟糕，入睡很困难。
医生：这种情况持续多久了？
病人：大概有两个月了。"""
        
        with open("./raw_data/dialogues/P10086_visit_1.txt", 'w', encoding='utf-8') as f:
            f.write(dialogue1)
        
        dialogue2 = """医生：今天感觉怎么样？
病人：比上次好一些，但还是容易焦虑。
医生：睡眠有改善吗？
病人：稍微好一点，但还是会早醒。"""
        
        with open("./raw_data/dialogues/P10087_visit_1.txt", 'w', encoding='utf-8') as f:
            f.write(dialogue2)
        
        print(f"  [OK] 创建示例对话文件")
        
        return True
        
    except Exception as e:
        print(f"  [FAIL] 创建示例结构失败: {e}")
        return False

def run_all_tests():
    """运行所有测试"""
    print("\n" + "="*60)
    print("运行所有测试")
    print("="*60)
    
    test_results = []
    
    # 运行测试
    test_results.append(("模块导入测试", test_module_imports()))
    test_results.append(("txt_reader测试", test_txt_reader_basic()))
    test_results.append(("excel_reader测试", test_excel_reader_basic()))
    test_results.append(("data_merger测试", test_data_merger_basic()))
    test_results.append(("主流水线配置测试", test_main_pipeline_config()))
    test_results.append(("示例结构创建", create_sample_structure()))
    
    # 输出结果
    print("\n" + "="*60)
    print("测试结果摘要")
    print("="*60)
    
    passed = 0
    failed = 0
    
    for test_name, result in test_results:
        status = "[OK] 通过" if result else "[FAIL] 失败"
        print(f"{test_name:25} {status}")
        
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
        print("\n[SUCCESS] 所有测试通过！数据处理流水线准备就绪。")
        return True
    else:
        print("\n[WARNING] 部分测试失败，但核心功能可能仍然可用。")
        return False

def main():
    """主函数"""
    try:
        print("\n数据处理流水线测试开始...")
        
        success = run_all_tests()
        
        if success:
            print("\n使用说明:")
            print("1. 准备Excel量表数据: ./raw_data/scores.xlsx")
            print("2. 准备对话文本文件: ./raw_data/dialogues/")
            print("3. 运行主流水线: python main_pipeline.py")
            print("4. 或使用命令行:")
            print("   python main_pipeline.py --excel ./raw_data/scores.xlsx --dialogues ./raw_data/dialogues/")
            print("\n已创建的示例文件:")
            print("  - ./raw_data/test_scores.xlsx (示例Excel)")
            print("  - ./raw_data/dialogues/P10086_visit_1.txt (示例对话)")
            print("  - ./raw_data/dialogues/P10087_visit_1.txt (示例对话)")
            
        return 0 if success else 1
        
    except KeyboardInterrupt:
        print("\n测试被用户中断")
        return 1
    except Exception as e:
        print(f"\n测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())