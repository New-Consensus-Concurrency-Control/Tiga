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

fig_size = (3.5,2)

region_categories =[
    "Local-1",
    "Local-2",
    "Local-3",
    "Remote",
    "All"
]


cat_size_order = CategoricalDtype(
            ["headroom-", "origin-"],
            ordered=True
        ) 

y_categories = [ "50p", "Rollback" ]
y_category_labels = [
    r"50p Latency ($10^3$ ms)",
    r"Rollback Rate(%)",
]
y_category_colors = ['red',  'limegreen']
y_category_markers =['s', 'o']
y_category_markersizes =[5,5]
y_category_linestyles = ['solid', 'solid']

x_range = (-85, 50)
scatter_x_loc = -80
x_tick_interval = 25
x_ticks = [-50, -25, 0, 25, 50]
y_ranges = [
    (0, 800), (0, 105)
]
x_category = "OWDDelta"
x_label = r"Headroom Delta (ms)"
y_tick_intervals =[
    200,25
]


def MyPlotHeadRoom(isolated_df, df, region_idx, show_legend=True, tag="Tag"):
    df.loc[:, x_category] = df.loc[:, x_category] - 10
    df = df[df[x_category]<=x_range[1]]
    df = df[df[x_category]>=x_range[0]]
    region_category = region_categories[region_idx]
    # Define your grid layout
    fig, ax0 = plt.subplots(figsize=fig_size)
    # # Adjust the horizontal space between subplots
    # plt.subplots_adjust(wspace=0.4)  # Increase the value to add more space
    # Iterate over y-categories
    ax0.set_xlabel(x_label)
    ax0.set_ylabel(y_category_labels[0], color=y_category_colors[0])
    line0, = ax0.plot(
        df[x_category], 
        df[y_categories[0]], 
        marker=y_category_markers[0], 
        color=y_category_colors[0], 
        label=y_category_labels[0],
        linestyle = y_category_linestyles[1]
    )
    add_point = list(isolated_df[y_categories[0]])[0]
    # print(f"add point {add_point}")
    ax0.scatter(scatter_x_loc, [add_point],
        marker=y_category_markers[0], 
        color=y_category_colors[0], 
        facecolors='none',   
        label=y_category_labels[0],
        linestyle = y_category_linestyles[1]
    ) 

    # print(list(df[x_category]))
    # print(list(df[y_categories[0]]))

    ax0.set_xlim(x_range)
    ax0.set_ylim(y_ranges[0])
    ax0.set_xticks( [scatter_x_loc]+ x_ticks)
    ax0.set_xticklabels(['0-Hdrm']+x_ticks)

    ymax = y_ranges[0][1]
    y_ticks = np.arange(
        0, ymax+y_tick_intervals[0]*0.5, y_tick_intervals[0])
    ax0.set_yticks(y_ticks)
        
    ax1 = ax0.twinx()
    ax1.set_ylabel(y_category_labels[1], color=y_category_colors[1])
    line1, = ax1.plot(
        df[x_category], 
        df[y_categories[1]], 
        marker=y_category_markers[1], 
        color=y_category_colors[1], 
        label=y_category_labels[1],
        linestyle = y_category_linestyles[1]
    )
    ax1.tick_params(axis='y', labelcolor=y_category_colors[1])
    ax1.set_ylim(y_ranges[1])

    add_point = list(isolated_df[y_categories[1]])[0]
    # print(f"add point {add_point}")
    ax1.scatter(scatter_x_loc, [add_point],
        facecolors='none',   
        marker=y_category_markers[1], 
        color=y_category_colors[1], 
        label=y_category_labels[1],
        linestyle = y_category_linestyles[1]
    ) 

    ymax = y_ranges[1][1]
    y_ticks = np.arange(
        0, ymax+y_tick_intervals[1]*0.5, y_tick_intervals[1])
    ax1.set_yticks(y_ticks)

    # Add shared legend
    lines = [line0, line1]
    labels = [l.get_label() for l in lines]

    ax1.legend(lines, labels, loc='upper center', 
                bbox_to_anchor=(0.5, 1.18), 
                ncol= len(y_category_labels),
                fancybox=False, shadow=False, 
                prop={'size': 12, 'weight': 'normal'}, frameon=False,
                handletextpad=0.3, columnspacing=1.5)
                
    fig.tight_layout()
    # Save the figure
    with_legend = "-legend" if show_legend else ""
    output_file = f"{tag}-{region_category}{with_legend}.pdf"
    plt.savefig(
        f"{tiga_common.FIGS_PATH}/{output_file}", 
        bbox_inches='tight', dpi=1200)
    tiga_common.print_info(f"saved {tiga_common.FIGS_PATH}/{output_file}")



def MyPlotHeadRoomDouble(
    isolated_dfs, summary_dfs, 
    idx1, idx2, label1='', label2='', show_legend=True, tag="Tag"):

    # Define your grid layout
    fig, axes = plt.subplots(1,2, figsize=(fig_size[0]*2, fig_size[1]))
    # # Adjust the horizontal space between subplots
    plt.subplots_adjust(wspace=0.4)  # Increase the value to add more space

    isolated_df =isolated_dfs[idx1]
    df = summary_dfs[idx1]
    df.loc[:, x_category] = df.loc[:, x_category] - 10
    df = df[df[x_category]<=x_range[1]]
    df = df[df[x_category]>=x_range[0]]
    ax0 = axes[0]
    line0, line1 = plot_one_fig(ax0, isolated_df, df, 
        show_left_y_axis=True, show_right_y_axis=False)



    isolated_df =isolated_dfs[idx2]
    df = summary_dfs[idx2]
    df.loc[:, x_category] = df.loc[:, x_category] - 10
    df = df[df[x_category]<=x_range[1]]
    df = df[df[x_category]>=x_range[0]]
    ax1 = axes[1]
    line3, line4 = plot_one_fig(ax1, isolated_df, df, 
        show_left_y_axis=False, show_right_y_axis=True)

    # Add shared legend
    fig.text(0.5, 0.05, x_label, ha='center', va='center', fontsize=12)
    if label1 !='':
        fig.text(0.28, -0.03, label1, 
            ha='center', va='center', 
            fontsize=12, fontweight='bold')
    if label2 !='':
        fig.text(0.72, -0.03, label2, 
            ha='center', va='center', 
            fontsize=12, fontweight='bold')
    # Add a single legend for all subplots
    if show_legend:
        lines = [line0, line1]
        labels = [l.get_label() for l in lines]
        fig.legend(
            lines, labels, loc='upper center', 
            bbox_to_anchor=(0.5, 1.1), ncol= 2, 
            fancybox=False, shadow=False, 
            prop={'size': 12, 'weight': 'normal'}, frameon=False,
            handletextpad=0.3, columnspacing=1.5)
                
    fig.tight_layout()
    # Save the figure
    with_legend = "-legend" if show_legend else ""
    output_file = f"{tag}-double-{idx1}-{idx2}{with_legend}.pdf"
    plt.savefig(
        f"{tiga_common.FIGS_PATH}/{output_file}", 
        bbox_inches='tight', dpi=1200)
    tiga_common.print_info(f"saved {tiga_common.FIGS_PATH}/{output_file}")


def plot_one_fig(ax0, isolated_df, df, 
    show_left_y_axis=True, show_right_y_axis=True):
    line0, = ax0.plot(
        df[x_category], 
        df[y_categories[0]], 
        marker=y_category_markers[0], 
        color=y_category_colors[0], 
        label=y_category_labels[0],
        linestyle = y_category_linestyles[1]
    )
    add_point = list(isolated_df[y_categories[0]])[0]
    # print(f"add point {add_point}")
    ax0.scatter(scatter_x_loc, [add_point],
        marker=y_category_markers[0], 
        color=y_category_colors[0], 
        facecolors='none',   
        label=y_category_labels[0],
        linestyle = y_category_linestyles[1]
    ) 

    ax0.set_xlim(x_range)

    ax0.set_xticks( [scatter_x_loc]+ x_ticks)
    ax0.set_xticklabels(['0-Hdrm']+x_ticks)
    ax0.tick_params(axis='y', labelcolor=y_category_colors[0])
    ax0.set_ylim(y_ranges[0])
    ymax = y_ranges[0][1]
    y_ticks = np.arange(
        0, ymax+y_tick_intervals[0]*0.5, y_tick_intervals[0])
    ax0.set_yticks(y_ticks)
    if show_left_y_axis:
        ax0.set_ylabel(y_category_labels[0], color=y_category_colors[0])
    else:
        ax0.set_yticklabels([])

    ax1 = ax0.twinx()
    line1, = ax1.plot(
        df[x_category], 
        df[y_categories[1]], 
        marker=y_category_markers[1], 
        color=y_category_colors[1], 
        label=y_category_labels[1],
        linestyle = y_category_linestyles[1]
    )
    # print("X:", list(df[x_category]))
    # print("Y:", list(df[y_categories[1]]) )

    add_point = list(isolated_df[y_categories[1]])[0]
    # print(f"add point {add_point}")
    ax1.scatter(scatter_x_loc, [add_point],
        facecolors='none',   
        marker=y_category_markers[1], 
        color=y_category_colors[1], 
        label=y_category_labels[1],
        linestyle = y_category_linestyles[1]
    ) 
    ymax = y_ranges[1][1]
    ax1.tick_params(axis='y', labelcolor=y_category_colors[1])
    ax1.set_ylim(y_ranges[1])
    y_ticks = np.arange(
        0, ymax+y_tick_intervals[1]*0.5, y_tick_intervals[1])
    ax1.set_yticks(y_ticks)
    if show_right_y_axis:
        ax1.set_ylabel(y_category_labels[1], color=y_category_colors[1])
    else:
        ax1.set_yticklabels([])

    return line0, line1

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
    isolated_dfs = []
    for df in dfs:
        df = df.drop('BenchType', axis=1)
        # df = df.drop('Prefix', axis=1)
        df = df.drop('TestType', axis=1)
        isolated_df = df[df['Prefix']=='origin-']
        df = df[df['Prefix']=='headroom-']
        df = df.drop('Prefix', axis=1)
        isolated_df = isolated_df.drop('Prefix', axis=1)
        summary_df = df.groupby(
            ['OWDDelta'],  
            as_index = False).median().round(2)
        summary_dfs.append(summary_df)
        isolated_df = isolated_df.groupby(
            ['OWDDelta'],  
            as_index = False).median().round(2)
        isolated_dfs.append(isolated_df)

    os.system(f"sudo mkdir -m777 -p {tiga_common.FIGS_PATH}")
    # for region_idx in range(len(summary_dfs)):
    #     # summary_dfs[region_idx].to_csv(f"HeadRoom-{region_idx}.csv")
    #     # isolated_dfs[region_idx].to_csv(f"Origin-{region_idx}.csv")
    #     MyPlotHeadRoom(
    #         isolated_dfs[region_idx],
    #         summary_dfs[region_idx], 
    #         region_idx, 
    #         show_legend=True, 
    #         tag=args.tag)
    
    MyPlotHeadRoomDouble(isolated_dfs, 
        summary_dfs, 0,3, label1 ='South Carolina',
        label2 = 'Hong Kong',
        show_legend=True, tag=args.tag)