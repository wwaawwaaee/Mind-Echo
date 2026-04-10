#!/usr/bin/env python3
"""
生成词云图 - 基于jieba分词
过滤无意义虚词和单字
"""

import json
import jieba
from collections import Counter
from wordcloud import WordCloud
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
matplotlib.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei']
matplotlib.rcParams['axes.unicode_minus'] = False


STOPWORDS = {
    # Single characters (except meaningful ones)
    '的', '了', '是', '在', '我', '有', '和', '就', '不', '人', '都', '一', '一个', '上', '也', '很', '到', '说', '要', '去',
    '你', '会', '着', '没有', '看', '好', '自己', '这', '那', '他', '她', '它', '们', '吗', '呢', '吧', '啊', '呀', '哦',
    '嗯', '哈', '噢', '呃', '唉', '喂', '嘿', '哎', '哟', '嘻', '哼', '嗷', '哇', '么', '什么', '怎么', '这样', '那样',
    '如何', '为什么', '多少', '几', '这个', '那个', '哪个', '谁', '哪', '哪里', '多么', '还', '又', '再', '才', '就',
    '才', '刚', '正好', '正好', '已经', '曾经', '正在', '将要', '会', '能', '可以', '应该', '必须', '得', '想', '要',
    '想', '觉得', '知道', '明白', '懂', '记得', '忘记', '希望', '相信', '认为', '感觉', '以为', '看来', '好像', '仿佛',
    '似乎', '差不多', '大约', '左右', '大概', '也许', '可能', '或许', '难道', '居然', '竟然', '果然', '当然', '当然',
    '原来', '其实', '当然', '反正', '究竟', '到底', '毕竟', '总之', '一般', '通常', '往往', '常常', '经常', '有时',
    '偶尔', '从来', '向来', '一直', '总是', '始终', '反正', '幸亏', '多亏', '幸好', '好在', '本来', '原本', '当初',
    '后来', '然后', '接着', '随后', '最后', '终于', '终究', '反正', '总之', '总而言之', '换句话说', '也就是说',
    '不过', '但是', '然而', '可是', '但', '却', '反而', '倒是', '只是', '不过', '然而', '虽然', '尽管', '即使',
    '哪怕', '就算', '纵然', '宁可', '宁愿', '与其', '不如', '倒不如', '莫斯科', '巴黎', '北京', '上海', '中国',
    # Common verbs that are too generic
    '来', '去', '进', '出', '起', '过', '把', '被', '让', '给', '对', '向', '跟', '比', '跟', '同', '和', '或', '或者',
    '及', '以及', '并且', '而且', '同时', '于是', '因此', '所以', '因为', '由于', '为了', '以便', '便于', '借以',
    # Quantifiers and numbers
    '两', '三', '四', '五', '六', '七', '八', '九', '十', '百', '千', '万', '亿', '第', '次', '些', '点', '种', '类',
    '个', '只', '条', '张', '本', '件', '把', '杯', '碗', '盘', '盒', '袋', '箱', '辆', '架', '艘', '列', '趟', '次',
    # Question words
    '怎', '咋', '吗', '呢', '吧', '呗', '咧', '嘞', '哟', '啰', '哇', '呀', '呵', '嘿', '哼', '嗯', '哎', '喂', '嗨',
}


def load_datasets():
    base_path = '../../../processed_dataset/output'
    
    with open(f'{base_path}/anonymized_dataset_with_caregiver.json', encoding='utf-8') as f:
        with_caregiver = json.load(f)
    
    with open(f'{base_path}/anonymized_dataset_without_caregiver.json', encoding='utf-8') as f:
        without_caregiver = json.load(f)
    
    return with_caregiver, without_caregiver


def extract_text_by_role(dataset, target_role):
    """提取指定角色的所有文本"""
    texts = []
    for patient in dataset.get('patients', []):
        for visit in patient.get('visits', []):
            for turn in visit.get('dialogue', {}).get('turns', []):
                if turn.get('role') == target_role:
                    text = turn.get('text', '')
                    if text:
                        texts.append(text)
    return ' '.join(texts)


def tokenize_and_filter(text):
    """使用jieba分词并过滤"""
    words = jieba.cut(text)
    filtered = []
    for word in words:
        word = word.strip()
        # Filter: length >= 2, not in stopwords, is Chinese
        if len(word) >= 2 and word not in STOPWORDS:
            # Check if all characters are Chinese
            if all('\u4e00' <= c <= '\u9fff' for c in word):
                filtered.append(word)
    return filtered


def generate_wordcloud(text, output_path, title):
    """生成词云"""
    words = tokenize_and_filter(text)
    word_freq = Counter(words)
    
    print(f"  总词数: {len(words)}, 唯一词: {len(word_freq)}")
    print(f"  Top 10: {word_freq.most_common(10)}")
    
    if len(word_freq) == 0:
        print(f"  警告: 无有效词汇生成词云")
        return
    
    # Use system font that supports Chinese
    font_paths = [
        'C:/Windows/Fonts/simhei.ttf',
        'C:/Windows/Fonts/msyh.ttc',
        '/System/Library/Fonts/STHeiti Light.ttc',
        '/usr/share/fonts/truetype/wqy/wqy-microhei.ttc',
    ]
    
    font_path = None
    import os
    for fp in font_paths:
        if os.path.exists(fp):
            font_path = fp
            break
    
    wc = WordCloud(
        font_path=font_path,
        width=1200,
        height=800,
        background_color='white',
        max_words=200,
        max_font_size=100,
        random_state=42,
        colormap='viridis'
    )
    
    wc.generate_from_frequencies(word_freq)
    
    plt.figure(figsize=(14, 10))
    plt.imshow(wc, interpolation='bilinear')
    plt.axis('off')
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  词云已保存: {output_path}")


def main():
    print("加载数据集...")
    with_caregiver, without_caregiver = load_datasets()
    
    output_dir = '.'
    
    # with_caregiver: 角色为 caregiver
    print("\n=== with_caregiver (caregiver) ===")
    text_wc = extract_text_by_role(with_caregiver, 'caregiver')
    print(f"  文本长度: {len(text_wc)} 字符")
    generate_wordcloud(text_wc, f'{output_dir}/wordcloud_with_caregiver_caregiver.png', 
                       'with_caregiver: 家长语言 (caregiver)')
    
    # without_caregiver: 角色为 patient
    print("\n=== without_caregiver (patient) ===")
    text_wo = extract_text_by_role(without_caregiver, 'patient')
    print(f"  文本长度: {len(text_wo)} 字符")
    generate_wordcloud(text_wo, f'{output_dir}/wordcloud_without_caregiver_patient.png',
                       'without_caregiver: 患者语言 (patient)')
    
    # Also create comparison figure
    print("\n=== 生成对比图 ===")
    fig, axes = plt.subplots(1, 2, figsize=(20, 8))
    
    for idx, (text, title, path) in enumerate([
        (text_wc, 'with_caregiver (家长)', f'{output_dir}/wordcloud_with_caregiver_caregiver.png'),
        (text_wo, 'without_caregiver (患者)', f'{output_dir}/wordcloud_without_caregiver_patient.png')
    ]):
        words = tokenize_and_filter(text)
        word_freq = Counter(words)
        
        font_paths = [
            'C:/Windows/Fonts/simhei.ttf',
            'C:/Windows/Fonts/msyh.ttc',
        ]
        font_path = None
        import os
        for fp in font_paths:
            if os.path.exists(fp):
                font_path = fp
                break
        
        wc = WordCloud(
            font_path=font_path,
            width=800,
            height=600,
            background_color='white',
            max_words=150,
            max_font_size=80,
            random_state=42,
            colormap='viridis'
        )
        wc.generate_from_frequencies(word_freq)
        
        axes[idx].imshow(wc, interpolation='bilinear')
        axes[idx].axis('off')
        axes[idx].set_title(title, fontsize=14, fontweight='bold')
    
    plt.suptitle('词云对比: with_caregiver vs without_caregiver', fontsize=16, fontweight='bold')
    plt.tight_layout(rect=[0, 0, 1, 0.95])
    plt.savefig(f'{output_dir}/wordcloud_comparison.png', dpi=150, bbox_inches='tight')
    plt.close()
    print(f"  对比图已保存: {output_dir}/wordcloud_comparison.png")
    
    print("\n词云生成完成!")


if __name__ == '__main__':
    main()
