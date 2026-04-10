import matplotlib.pyplot as plt
import numpy as np

plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
fig.suptitle('统计图表', fontsize=16, fontweight='bold')

# 1. 患者分布饼图
ax1 = axes[0, 0]
labels = ['有家长陪同', '无家长陪同']
sizes = [63, 21]
colors = ['#4CAF50', '#FF9800']
explode = (0.05, 0)
ax1.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
        shadow=True, startangle=90, textprops={'fontsize': 11})
ax1.set_title('患者分布', fontsize=12, fontweight='bold')

# 2. 对话角色分布
ax2 = axes[0, 1]
roles = ['医生', '家长', '患者']
turns = [2824, 1889, 818]
colors2 = ['#2196F3', '#4CAF50', '#FF9800']
bars = ax2.bar(roles, turns, color=colors2, edgecolor='black', linewidth=1.2)
ax2.set_title('对话角色分布 (总轮次: 5531)', fontsize=12, fontweight='bold')
ax2.set_ylabel('轮次')
for bar, turn in zip(bars, turns):
    pct = turn / sum(turns) * 100
    ax2.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 50,
             f'{turn}\n({pct:.1f}%)', ha='center', va='bottom', fontsize=10)
ax2.set_ylim(0, max(turns) * 1.2)

# 3. GAD-7 焦虑量表分布对比
ax3 = axes[1, 0]
severity_levels = ['轻度', '中度', '重度']
with_caregiver_gad = [18, 33, 8]
without_caregiver_gad = [5, 11, 4]

x = np.arange(len(severity_levels))
width = 0.35

bars1 = ax3.bar(x - width/2, with_caregiver_gad, width, label='有家长(n=63)', color='#4CAF50', edgecolor='black')
bars2 = ax3.bar(x + width/2, without_caregiver_gad, width, label='无家长(n=21)', color='#FF9800', edgecolor='black')

ax3.set_xlabel('焦虑程度')
ax3.set_ylabel('患者数')
ax3.set_title('GAD-7 焦虑量表分布对比', fontsize=12, fontweight='bold')
ax3.set_xticks(x)
ax3.set_xticklabels(severity_levels)
ax3.legend()
ax3.bar_label(bars1, padding=3)
ax3.bar_label(bars2, padding=3)

# 4. PHQ-9 抑郁量表分布对比
ax4 = axes[1, 1]
with_caregiver_phq = [7, 35, 2]
without_caregiver_phq = [4, 7, 1]

bars3 = ax4.bar(x - width/2, with_caregiver_phq, width, label='有家长(n=63)', color='#4CAF50', edgecolor='black')
bars4 = ax4.bar(x + width/2, without_caregiver_phq, width, label='无家长(n=21)', color='#FF9800', edgecolor='black')

ax4.set_xlabel('抑郁程度')
ax4.set_ylabel('患者数')
ax4.set_title('PHQ-9 抑郁量表分布对比', fontsize=12, fontweight='bold')
ax4.set_xticks(x)
ax4.set_xticklabels(severity_levels)
ax4.legend()
ax4.bar_label(bars3, padding=3)
ax4.bar_label(bars4, padding=3)

plt.tight_layout()
plt.savefig(r'D:\vscode-project\git-project\Mind-Echo\basic_status_summary\caregiver_statistics.png', dpi=150, bbox_inches='tight')
plt.show()
print('图表已保存到 caregiver_statistics.png')
