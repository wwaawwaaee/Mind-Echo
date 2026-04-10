import json
import matplotlib.pyplot as plt
import numpy as np

plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

with open('processed_dataset/output/anonymized_dataset_with_caregiver.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

patients = data['patients']
ages = []
genders = {'M': 0, 'F': 0, 'unknown': 0}
visits_per_patient = []
gad7_scores = []
phq9_scores = []

for p in patients:
    if p.get('age'):
        try:
            ages.append(int(p['age']))
        except:
            pass
    
    g = p.get('gender', 'unknown')
    if g in ['男', 'M']:
        genders['M'] += 1
    elif g in ['女', 'F']:
        genders['F'] += 1
    else:
        genders['unknown'] += 1
    
    visits_per_patient.append(len(p.get('visits', [])))
    
    for scale in p.get('scales', []):
        if 'GAD-7' in scale and 'total' in scale['GAD-7']:
            gad7_scores.append(scale['GAD-7']['total'])
        if 'PHQ-9' in scale and 'total' in scale['PHQ-9']:
            phq9_scores.append(scale['PHQ-9']['total'])

age_buckets = {'0-6': 0, '7-12': 0, '13-17': 0}
for a in ages:
    if a <= 6:
        age_buckets['0-6'] += 1
    elif a <= 12:
        age_buckets['7-12'] += 1
    else:
        age_buckets['13-17'] += 1

print(f"n_patients: {len(patients)}")
print(f"n_visits: {data['stats']['total_visits']}")
print(f"genders: {genders}")
print(f"ages: min={min(ages)}, max={max(ages)}, mean={np.mean(ages):.1f}, median={np.median(ages)}")
print(f"age_buckets: {age_buckets}")
print(f"visits: min={min(visits_per_patient)}, max={max(visits_per_patient)}, mean={np.mean(visits_per_patient):.1f}")
print(f"gad7: count={len(gad7_scores)}, mean={np.mean(gad7_scores):.1f}, median={np.median(gad7_scores)}")
print(f"phq9: count={len(phq9_scores)}, mean={np.mean(phq9_scores):.1f}, median={np.median(phq9_scores)}")

gad7_severity = {'minimal': 0, 'mild': 0, 'moderate': 0, 'severe': 0}
for s in gad7_scores:
    if s <= 4: gad7_severity['minimal'] += 1
    elif s <= 9: gad7_severity['mild'] += 1
    elif s <= 14: gad7_severity['moderate'] += 1
    else: gad7_severity['severe'] += 1

phq9_severity = {'minimal': 0, 'mild': 0, 'moderate': 0, 'moderately_severe': 0, 'severe': 0}
for s in phq9_scores:
    if s <= 4: phq9_severity['minimal'] += 1
    elif s <= 9: phq9_severity['mild'] += 1
    elif s <= 14: phq9_severity['moderate'] += 1
    elif s <= 19: phq9_severity['moderately_severe'] += 1
    else: phq9_severity['severe'] += 1

print(f"gad7_severity: {gad7_severity}")
print(f"phq9_severity: {phq9_severity}")

fig = plt.figure(figsize=(14, 11))

gs = fig.add_gridspec(3, 2, height_ratios=[1, 1, 0.08], hspace=0.35, wspace=0.25)

fig.suptitle('Baseline Characteristics of Pediatric Patients with Adenoid Hypertrophy\n腺样体肥大患儿基线特征分析', fontsize=16, fontweight='bold', y=0.97)

ax1 = fig.add_subplot(gs[0, 0])
labels = list(age_buckets.keys())
values = list(age_buckets.values())
bars = ax1.bar(labels, values, color=['#4CAF50', '#2196F3', '#FF9800'], edgecolor='black', linewidth=1.2)
ax1.set_title('Age Distribution / 年龄分布', fontsize=13, fontweight='bold', pad=10)
ax1.set_xlabel('Age Group / 年龄段', fontsize=10)
ax1.set_ylabel('Number of Patients / 患者数', fontsize=10)
ax1.set_ylim(0, max(values) * 1.3)
for bar, v in zip(bars, values):
    ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5, str(v), ha='center', fontsize=12, fontweight='bold')
ax1.text(0.95, 0.95, f'n={len(patients)}', transform=ax1.transAxes, ha='right', va='top', fontsize=10, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

ax2 = fig.add_subplot(gs[0, 1])
labels_g = ['Male / 男', 'Female / 女', 'Unknown / 未知']
sizes = [genders['M'], genders['F'], genders['unknown']]
colors = ['#2196F3', '#FF69B4', '#9E9E9E']
wedges, texts, autotexts = ax2.pie(sizes, labels=labels_g, colors=colors, autopct='%1.1f%%', startangle=90, textprops={'fontsize': 9})
ax2.set_title('Gender Distribution / 性别分布', fontsize=13, fontweight='bold', pad=10)

ax3 = fig.add_subplot(gs[1, 0])
gad7_labels = ['Minimal\n极轻(0-4)', 'Mild\n轻度(5-9)', 'Moderate\n中度(10-14)', 'Severe\n重度(15-21)']
gad7_values = [gad7_severity['minimal'], gad7_severity['mild'], gad7_severity['moderate'], gad7_severity['severe']]
colors_gad = ['#4CAF50', '#FFC107', '#FF9800', '#F44336']
bars = ax3.bar(gad7_labels, gad7_values, color=colors_gad, edgecolor='black', linewidth=1.2)
ax3.set_title(f'GAD-7 Anxiety Scale / GAD-7焦虑量表\n(Mean/均值: {np.mean(gad7_scores):.1f}, Median/中位数: {np.median(gad7_scores):.0f})', fontsize=12, fontweight='bold', pad=10)
ax3.set_ylabel('Count / 计数', fontsize=10)
ax3.set_ylim(0, max(gad7_values) * 1.3)
for bar, v in zip(bars, gad7_values):
    ax3.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, str(v), ha='center', fontsize=11, fontweight='bold')

ax4 = fig.add_subplot(gs[1, 1])
phq9_labels = ['Minimal\n极轻(0-4)', 'Mild\n轻度(5-9)', 'Moderate\n中度(10-14)', 'Mod.Sev.\n中重(15-19)', 'Severe\n重度(20-27)']
phq9_values = [phq9_severity['minimal'], phq9_severity['mild'], phq9_severity['moderate'], phq9_severity['moderately_severe'], phq9_severity['severe']]
colors_phq = ['#4CAF50', '#FFC107', '#FF9800', '#FF5722', '#F44336']
bars = ax4.bar(phq9_labels, phq9_values, color=colors_phq, edgecolor='black', linewidth=1.2)
ax4.set_title(f'PHQ-9 Depression Scale / PHQ-9抑郁量表\n(Mean/均值: {np.mean(phq9_scores):.1f}, Median/中位数: {np.median(phq9_scores):.0f})', fontsize=12, fontweight='bold', pad=10)
ax4.set_ylabel('Count / 计数', fontsize=10)
ax4.set_ylim(0, max(phq9_values) * 1.3)
for bar, v in zip(bars, phq9_values):
    ax4.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 1, str(v), ha='center', fontsize=11, fontweight='bold')

ax_footer = fig.add_subplot(gs[2, :])
ax_footer.axis('off')
footer_text = (
    "Notes / 注释:\n"
    "• GAD-7: Generalized Anxiety Disorder 7-item scale / 广泛性焦虑障碍量表 (7项)\n"
    "• PHQ-9: Patient Health Questionnaire-9 / 患者健康问卷抑郁量表 (9项)\n"
    "• Minimal (<5), Mild (5-9), Moderate (10-14), Mod.Severe (15-19), Severe (20-27) / 极轻(<5), 轻度(5-9), 中度(10-14), 中重(15-19), 重度(20-27)\n"
    "• Dataset: Pediatric patients with adenoid hypertrophy / 数据来源: 腺样体肥大患儿"
)
ax_footer.text(0.5, 0.5, footer_text, transform=ax_footer.transAxes, fontsize=9, ha='center', va='center',
               bbox=dict(boxstyle='round,pad=0.5', facecolor='lightgray', alpha=0.3))

plt.savefig('basic_status_summary/adenoid_hypertrophy_stats.png', dpi=150, bbox_inches='tight', facecolor='white')
print('Chart saved to basic_status_summary/adenoid_hypertrophy_stats.png')
