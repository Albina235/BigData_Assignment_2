import matplotlib.pyplot as plt
import numpy as np

sql_times = [268.71, 140.24, 0.26]
mongo_times = [1355.41, 434.54, 0.45]
neo4j_times = [21.07, 6.13, 0]

labels = ['Q1: Campaign Analysis', 'Q2: Recommendations', 'Q3: Full Text Search']
x = np.arange(len(labels))
width = 0.25

fig, ax = plt.subplots(figsize=(10, 6))

rects1 = ax.bar(x - width, sql_times, width, label='PostgreSQL (SQL)', color='#1f77b4')
rects2 = ax.bar(x, mongo_times, width, label='MongoDB (NoSQL)', color='#2ca02c')
rects3 = ax.bar(x + width, neo4j_times, width, label='Neo4j (Graph)', color='#ff7f0e')

ax.set_ylabel('Average Execution Time (ms)')
ax.set_title('Query Execution Times Comparison by Database Type')
ax.set_xticks(x)
ax.set_xticklabels(labels)
ax.legend()

def autolabel(rects, is_na=False):
    for rect in rects:
        height = rect.get_height()
        if height == 0 and is_na:
            ax.annotate('N/A',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)
        elif height > 0:
            ax.annotate(f'{height:.2f}',
                        xy=(rect.get_x() + rect.get_width() / 2, height),
                        xytext=(0, 3),
                        textcoords="offset points",
                        ha='center', va='bottom', fontsize=9)

autolabel(rects1)
autolabel(rects2)
autolabel(rects3, is_na=True)

ax.set_yscale('log')
ax.set_ylabel('Average Execution Time (ms) - Log Scale')

fig.tight_layout()

plt.savefig('screenshots/execution_times_chart.png', dpi=300)