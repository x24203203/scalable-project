import matplotlib.pyplot as plt

# Data
nodes = [3, 5, 7]
execution_time_seconds = [904, 634, 582]

# Plot
plt.figure(figsize=(8, 5))
plt.plot(nodes, execution_time_seconds, marker='o', linewidth=2, color='blue')
plt.ylim(bottom=0)  # y-axis starts at 0

# Annotate points
for i, txt in enumerate(execution_time_seconds):
    plt.annotate(f"{txt}s", (nodes[i], execution_time_seconds[i] + 15), ha='center')

# Labels and title
plt.title('Execution Time vs Number of Core Nodes', fontsize=14)
plt.xlabel('Number of Core Nodes', fontsize=12)
plt.ylabel('Execution Time (seconds)', fontsize=12)

# Grid and style
plt.grid(True)
plt.xticks(nodes)
plt.tight_layout()

# Save and show
plt.savefig('execution_time_vs_nodes.png')
plt.show()

