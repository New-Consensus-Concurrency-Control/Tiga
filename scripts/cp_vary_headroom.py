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
            ["2pl", "occ", "janus", "tapir", "detock", 
             "calvin", "ncc", "ncc-ft", "tiga", 
            "tiga-logical","tiga-skeen", "tiga-bad-clock",
            "tiga-ntp"],
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
x_categories = ["2pl",  "occ",   "tapir", "janus", "calvin", "ncc", "ncc-ft", "detock", "tiga" ]
x_category_labels = ["2PL+Paxos", "OCC+Paxos", "Tapir",   "Janus", "Calvin+", "NCC", "NCC+", "Detock", "Tiga" ]
x_category_colors = ['b','c','m','r', 'orange', 'dodgerblue', 'slategray', 'brown',  'limegreen']
x_category_markers =['s','P', 'D', '^','*', 'x', 'X', 'v', 'o']
x_category_markersizes =[5,5,5,5,7,7,7,5,5]
x_category_linestyles = ['solid', 'solid','solid', 'solid', 'solid', 'solid', 'solid','solid', 'solid']
# ['dashed',  'dashdot', 'dotted', 'solid', 'solid']
y_categories = [
    "Throughput", "CommitRate", 
    "50p", "90p", 
               ]
y_category_labels = [
    r"Throughput ($10^3$ txns/s)",
    r"Commit Rate(%)",
    r"50p Latency ($10^3$ ms)",
    r"90p Latency ($10^3$ ms)",
]


y_ranges = [
    (0, 50), (0, 105), (0, 10), (0, 10)
]
x_label = r"Skew Factor"
x_ranges =[0.5,1]
x_tick_interval = 0.1
y_tick_intervals =[
    10,50,1,1
]


def MyPlotSkew(df, region_idx, show_legend=True, tag="Tag"):
    region_category = region_categories[region_idx]
    # Define your grid layout
    fig, axes = plt.subplots(
        1, len(y_categories), 
        figsize=(fig_size[0] * len(y_categories), 
        fig_size[1]))
    # Adjust the horizontal space between subplots
    plt.subplots_adjust(wspace=0.4)  # Increase the value to add more space
    # Iterate over y-categories
    for i, y_category in enumerate(y_categories):    
        ax = axes[i]  # Access the corresponding subplot
        xmin = x_ranges[0]
        xmax = x_ranges[1]
        ymin = y_ranges[i][0]
        ymax = y_ranges[i][1]
        x_ticks = np.arange(xmin, xmax+ x_tick_interval*0.5, x_tick_interval)
        y_ticks = np.arange(ymin, ymax+y_tick_intervals[i]*0.5, y_tick_intervals[i])
        for j in range(len(x_category_labels)):
            skew_factor = df[df["TestType"] == x_categories[j]]["Skew"]
            x = list(skew_factor)
            x_category = x_categories[j]
            x_category_label = x_category_labels[j]
            y = df[df['TestType'] == x_category][y_category]
            y_category_label =y_category_labels[i]
            if "Latency" in y_category_label:
                y = y * 0.001
                ax.set_yscale("log")
                ax.set_ylim([0.1, 10])
                ax.set_yticks([0.1, 1, 10])  # Tick locations
                ax.set_yticklabels(["0.1", "1", "10"])  # Custom labels
            alpha = 1
            # Set titles and labels for the subplot
            ax.set_xlim([xmin, xmax])
            ax.set_xticks(x_ticks)
            if "Latency" not in y_category_label:
                ax.set_ylim([ymin, ymax])
                ax.set_yticks(y_ticks)
            if "Throughput" in y_category_label:
                ax.set_ylim([0, 20])
                if region_idx ==len(tiga_common.REGION_PROXIES):
                    ax.set_ylim([0,100])
                    y_ticks = [0,50,100]
                    ax.set_yticks([0,50,100])  # Tick locations
                    ax.set_yticklabels(["0", "50", "100"])  # Custom labels
                    if x_category =='tiga': 
                        alpha = 0.5
                    
            if "Commit" in y_category_label:
                if x_category =='tiga' or x_category=='janus':
                    alpha = 0.5
            # Plot on the current axis
            ax.plot(x, y, color=x_category_colors[j],
                    marker=x_category_markers[j],
                    markersize= x_category_markersizes[j],
                    linestyle=x_category_linestyles[j],
                    label=x_category_label)
        ax.set_ylabel(f"{y_category_labels[i]}",  labelpad=1)  # Unique y-label for each subplot
    fig.text(0.5, -0.1, x_label, ha='center', va='center', fontsize=12)
    # Add a single legend for all subplots
    handles, labels = axes[0].get_legend_handles_labels()
    if show_legend:
        fig.legend(handles, labels, loc='upper center', bbox_to_anchor=(0.5, 1.1), ncol= len(x_category_labels), 
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

if __name__ == '__main__':
    parser = tiga_common.argparse.ArgumentParser(
        description='Process some integers.')
    parser.add_argument(
        '--tag',  
        type=str, 
        default = "Micro",
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
        summary_df = df.groupby(
            ['TestType','Skew'],  
            as_index = False).median().round(2)
        summary_df["TestType"] = summary_df["TestType"].astype(cat_size_order)
        summary_dfs.append(summary_df)
    os.system(f"sudo mkdir -m777 -p {tiga_common.FIGS_PATH}")
    for region_idx in range(len(summary_dfs)):
        MyPlotSkew(
            summary_dfs[region_idx], 
            region_idx, 
            show_legend=True, 
            tag=args.tag)
    
