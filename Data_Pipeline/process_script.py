# ================= 第一步：拿工具 =================
import os           # 操作系统工具，用来找文件路径
import json         # JSON工具，用来最后打包数据
import pandas as pd # 表格处理工具，简称 pd
import re           # 正则表达式工具，用来清理文本中的乱码

# ================= 第二步：告诉电脑文件在哪里 =================
# __file__ 代表当前代码文件，os.path.dirname 获取它所在的目录
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 拼装路径：基础目录 + raw_data文件夹 + 你的excel文件名
# 对应结构：project/raw_data/scores.xlsx
EXCEL_PATH = os.path.join(BASE_DIR, 'raw_data', 'scores.xlsx')

# 对应结构：project/raw_data/dialogues/
DIALOGUE_DIR = os.path.join(BASE_DIR, 'raw_data', 'dialogues')

# 对应结构：project/processed_data/output.jsonl
OUTPUT_PATH = os.path.join(BASE_DIR, 'processed_data', 'output.jsonl')

# ================= 第三步：定义处理逻辑（小工具） =================

# 小工具1：把txt里的文本切分成"医生说"和"病人说"
def parse_dialogue(text):
    """
    输入：一大段文本字符串
    输出：一个列表，里面装着一句句话的字典
    """
    lines = text.split('\n') # 按换行符切开，变成一句句
    result_list = []         # 准备一个空盒子装结果
    
    for line in lines:       # 遍历每一行
        line = line.strip()  # 去掉首尾空格
        if not line: continue # 如果是空行，直接跳过
        
        # 判断是谁说的
        role = "unknown"
        content = line
        
        # startswith 检查字符串是不是以某个词开头
        if line.startswith("医生") or line.startswith("Doctor"):
            role = "doctor"
            # 这里的切片操作 [3:] 意思是把前3个字符（医生：）去掉，只留后面的话
            # 但更稳妥的是用 split
            content = line.split('：')[-1] # 冒号分割，取最后一部分
            
        elif line.startswith("病人") or line.startswith("Patient"):
            role = "patient"
            content = line.split('：')[-1]
            
        # 把这一句装进小字典
        message = {"role": role, "content": content}
        
        # 把小字典丢进结果列表
        result_list.append(message)
        
    return result_list

# ================= 第四步：主程序流水线 =================
def main():
    print("Step 1: 正在读取 Excel 量表...")
    # pandas 读取 excel，就像打开 excel 软件一样
    # dtype=str 意思是强制把所有内容当成文本读进来，防止 001 变成 1
    df = pd.read_excel(EXCEL_PATH, engine='openpyxl', dtype=str)
    
    # 把 excel 转换成一个大字典，方便查询
    # orient='index' 意思是把第一列（ID）作为查找的Key
    # 结果长这样：{'P001': {'age': '24', 'score': '15'}, 'P002': ...}
    # 假设你的ID列头叫 'patient_id'，如果不叫这个，得改
    excel_data = df.set_index('patient_id').to_dict(orient='index')
    
    print("Step 2: 正在扫描对话文件...")
    # os.listdir 列出文件夹下所有文件名
    all_files = os.listdir(DIALOGUE_DIR)
    
    # 准备一个大字典，用来存放所有处理好的病人数据
    final_dataset = {} 
    
    # === 开始流水线循环 ===
    for filename in all_files:
        # 只要 .txt 文件
        if not filename.endswith('.txt'):
            continue
            
        # 解析文件名：假设文件叫 "P001_first.txt"
        # os.path.splitext 把后缀去掉 -> "P001_first"
        name_part = os.path.splitext(filename)[0]
        
        # 尝试用下划线切割 ID 和 阶段
        try:
            p_id, stage = name_part.split('_') # p_id="P001", stage="first"
        except:
            print(f"警告：文件 {filename} 命名格式不对，跳过")
            continue
            
        # 读取这个 txt 的内容
        full_path = os.path.join(DIALOGUE_DIR, filename)
        with open(full_path, 'r', encoding='utf-8') as f:
            text_content = f.read()
            
        # 使用上面的小工具1，把文本变成结构化列表
        structured_dialogue = parse_dialogue(text_content)
        
        # === 组装数据 (核心逻辑) ===
        
        # 如果这个病人还没在 final_dataset 里，先帮他建个档案
        if p_id not in final_dataset:
            # 去 excel_data 里查这个人的信息
            # .get(p_id, {}) 的意思是：查p_id，查不到就返回空字典，防止报错
            info = excel_data.get(p_id, {})
            
            final_dataset[p_id] = {
                "id": p_id,
                "demographics": info, # 把 Excel 里该行的所有列都放进去
                "visits": []          # 准备一个空列表放就诊记录
            }
        
        # 创建一条就诊记录
        visit_record = {
            "visit_type": stage,     # "first" 或 "second"
            "dialogue": structured_dialogue
        }
        
        # 把这次记录加到病人的 visits 列表里
        final_dataset[p_id]["visits"].append(visit_record)
        
    # ================= 第五步：保存结果 =================
    print(f"Step 3: 正在保存到 {OUTPUT_PATH}")
    
    # 确保输出文件夹存在，没有就自动创建
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f_out:
        # 遍历大字典里的每一个病人
        for p_id, data in final_dataset.items():
            # json.dumps 把字典变成字符串
            # ensure_ascii=False 是为了让中文正常显示，不乱码
            line = json.dumps(data, ensure_ascii=False)
            f_out.write(line + '\n') # 写入一行并换行

    print("大功告成！")

# 这是 Python 的入口开关，告诉电脑“如果是直接运行这个文件，就开始跑 main 函数”
if __name__ == "__main__":
    main()