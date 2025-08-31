import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from IPython import embed; 
import argparse
import datetime
import os
import tiga_common
import ruamel.yaml
from pandas.api.types import CategoricalDtype

plt.rcParams.update({'font.size': 12, "font.weight":"normal"})

cat_size_order = CategoricalDtype(
            ["read-optim-", "read-ordinary-"],
            ordered=True
        ) 

region_categories =[
    "Local-1",
    "Local-2",
    "Local-3",
    "Remote",
    "All"
]

fig_size = (3.3,2)
x_categories = ["read-optim-", "read-ordinary-"]
x_category_labels = ["Tiga-Read-Optimized", "Tiga"]
x_category_colors = ['red',  'limegreen']
x_category_markers =['s', 'o']
x_category_markersizes =[5,5]
x_category_linestyles = ['solid', 'solid','solid', 'solid', 'solid', 'solid', 'solid','solid', 'solid']
# ['dashed',  'dashdot', 'dotted', 'solid', 'solid']
y_category = "50p"
y_category_label = '50p Latency ($10^3$ ms)'

y_range = [0, 1000]
x_label = r"Per-Coordinator Rate ($10^3$ txns/s)"
x_ranges =[0,50]
x_tick_interval = 10
y_tick_interval =200


def MyPlot(df, region_idx, show_legend=True, tag="Tag"):
    region_category = region_categories[region_idx]
    # Define your grid layout
    fig = plt.figure(figsize=fig_size)
    # Adjust the horizontal space between subplots
    plt.subplots_adjust(wspace=0.4)  # Increase the value to add more space
    ax = fig.add_subplot(1, 1, 1) 
    xmin = x_ranges[0]
    xmax = x_ranges[1]
    ymin = y_range[0]
    ymax = y_range[1] + 10

    x_ticks = np.arange(0, xmax+1, x_tick_interval)
    y_ticks = np.arange( 0, ymax, y_tick_interval)
    ax.set_ylim([ymin, ymax])
    ax.set_yticks(y_ticks)
    ax.set_xlim([xmin, xmax])
    ax.set_xticks(x_ticks)
    
    for j in range(len(x_category_labels)):
        # print(x_categories)
        # embed()
        # exit(0)
        x = df[df["Prefix"] == x_categories[j]]["Rate"] / 1000
        # x = df[df["Prefix"] == x_categories[j]]["Throughput"]
        x_category = x_categories[j]
        x_category_label = x_category_labels[j]
        y = df[df['Prefix'] == x_category][y_category]
        alpha = 1
        # Plot on the current axis
        ax.plot(x, y, color=x_category_colors[j],
                marker=x_category_markers[j],
                markersize= x_category_markersizes[j],
                linestyle=x_category_linestyles[j],
                label=x_category_label, alpha=alpha)
    ax.set_ylabel(f"{y_category_label}",  labelpad=1) 
    fig.text(0.5, -0.1, x_label, ha='center', va='center', fontsize=12)
    # Add a single legend for all subplots
    handles, labels = ax.get_legend_handles_labels()
    if show_legend:
        fig.legend(
            handles, labels, loc='upper center', 
            bbox_to_anchor=(0.5, 1.1), ncol= len(x_category_labels), 
            fancybox=False, shadow=False, 
            prop={'size': 12, 'weight': 'normal'}, frameon=False,
            handletextpad=0.3, columnspacing=1.5)

    # Save the figure
    with_legend = "-legend" if show_legend else ""
    output_file = f"{tag}-{region_category}{with_legend}.pdf"
    plt.savefig(
        f"{tiga_common.FIGS_PATH}/{output_file}", 
        bbox_inches='tight', dpi=1200)
    tiga_common.print_info(f"saved {tiga_common.FIGS_PATH}/{output_file}")






def MyPlot2(df_list, region_indices, label1, label2,
    show_legend=True, tag="Tag"):
    """
    Plot multiple regions side-by-side in one figure
    
    Args:
        df_list: List of dataframes for each region
        region_indices: List of region indices to plot
        show_legend: Whether to show legend
        tag: Tag for output filename
    """
    # Create figure with subplots
    fig, axes = plt.subplots(1, len(region_indices), 
        figsize=(fig_size[0]*len(region_indices), fig_size[1]), 
        sharey=True)
    
    # Handle case where there's only one subplot
    if len(region_indices) == 1:
        axes = [axes]
    
    plt.subplots_adjust(wspace=0.15)  # Reduce space between subplots since they share y-axis
    
    # Set up axis ranges and ticks
    xmin, xmax = x_ranges[0], x_ranges[1]
    ymin, ymax = y_range[0], y_range[1] + 10
    x_ticks = np.arange(0, xmax+1, x_tick_interval)
    y_ticks = np.arange(0, ymax, y_tick_interval)
    
    # Plot each region
    for i, region_idx in enumerate(region_indices):
        ax = axes[i]
        df = df_list[region_idx]
        region_category = region_categories[region_idx]
        
        # Set axis limits and ticks
        ax.set_ylim([ymin, ymax])
        ax.set_xlim([xmin, xmax])
        ax.set_xticks(x_ticks)
        if i == 0:  # Only show y-ticks on leftmost plot
            ax.set_yticks(y_ticks)
        else:
            ax.set_yticks(y_ticks)
            # ax.set_yticklabels([])  # Hide y-tick labels for shared axis
        
        # Plot data for each category
        for j in range(len(x_category_labels)):
            x = df[df["Prefix"] == x_categories[j]]["Rate"] / 1000
            x_category = x_categories[j]
            x_category_label = x_category_labels[j]
            y = df[df['Prefix'] == x_category][y_category]
            alpha = 1
            # Plot on the current axis
            ax.plot(x, y, color=x_category_colors[j],
                    marker=x_category_markers[j],
                    markersize=x_category_markersizes[j],
                    linestyle=x_category_linestyles[j],
                    label=x_category_label if i == 0 else "", 
                    alpha=alpha)  # Only label on first subplot
        
    # Set single x-axis label centered across both subplots
    fig.text(0.5, -0.05, x_label, ha='center', va='center', fontsize=12)
    if label1 !='':
        fig.text(0.28, -0.15, label1, 
            ha='center', va='center', 
            fontsize=12, fontweight='bold')
    if label2 !='':
        fig.text(0.72, -0.15, label2, 
            ha='center', va='center', 
            fontsize=12, fontweight='bold')

    # Set y-axis label only on the leftmost subplot
    axes[0].set_ylabel(f"{y_category_label}", labelpad=1)
    
    # Add shared legend
    if show_legend:
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(
            handles, labels, loc='upper center', 
            bbox_to_anchor=(0.45, 1.1), ncol=len(x_category_labels), 
            fancybox=False, shadow=False, 
            prop={'size': 12, 'weight': 'normal'}, frameon=False,
            handletextpad=0.3, columnspacing=5)

    # Save the figure
    with_legend = "-legend" if show_legend else ""
    region_names = "-".join([region_categories[idx] for idx in region_indices])
    output_file = f"{tag}-{region_names}{with_legend}.pdf"
    plt.savefig(
        f"{tiga_common.FIGS_PATH}/{output_file}", 
        bbox_inches='tight', dpi=1200)
    tiga_common.print_info(f"saved {tiga_common.FIGS_PATH}/{output_file}")




if __name__ == '__main__':
    parser = tiga_common.argparse.ArgumentParser(
        description='Process some integers.')
    parser.add_argument(
        '--tag',  
        type=str, 
        default = "Clock",
        help='Specify the tag')
    parser.add_argument(
        '--test_plan',  
        type=str, 
        default = tiga_common.TEST_PLAN_FILE,
        help='Specify the path of the test_plan yaml file')
    args = parser.parse_args()
    test_plan_file = args.test_plan
    print("test plan file: ", test_plan_file)

    stats_csv_files = [
        test_plan_file.split(".")[0]+f"-region-{r}.csv" 
        for r in range(len(tiga_common.REGION_PROXIES))
    ] + [
        test_plan_file.split(".")[0]+f"-region-all.csv" 
    ]
    dfs = [ 
        pd.read_csv(
            f"{tiga_common.SUMMARY_STATS_PATH}/{stats_csv_file}", 
            delimiter = "\t")
        for stats_csv_file in stats_csv_files
    ]
    summary_dfs = []
    for df in dfs:
        df = df.drop('BenchType', axis=1)
        df = df.drop('TestType', axis=1)
        summary_df = df.groupby(
            ['Prefix','Rate'],  
            as_index = False).median().round(2)
        summary_df["Prefix"] = summary_df["Prefix"].astype(cat_size_order)
        summary_dfs.append(summary_df)
    os.system(f"sudo mkdir -m777 -p {tiga_common.FIGS_PATH}")
    # for region_idx in range(len(summary_dfs)):
    #     MyPlot(
    #         summary_dfs[region_idx], 
    #         region_idx, 
    #         show_legend=True, 
    #         tag=args.tag)
    # region_idx = 0
    # MyPlot(
    #         summary_dfs[region_idx], 
    #         region_idx, 
    #         show_legend=True, 
    #         tag=args.tag)

    # region_idx = 3
    # MyPlot(
    #         summary_dfs[region_idx], 
    #         region_idx, 
    #         show_legend=True, 
    #         tag=args.tag)

    MyPlot2(
        summary_dfs, 
        region_indices=[0, 3],  # Local-1 and Remote regions
        label1 ='South Carolina',
        label2 = 'Hong Kong',
        show_legend=True, 
        tag=args.tag)