from PIL import Image
import sys
import csv
import os
import matplotlib.pyplot as plt
import math
import numpy as np
from scipy import sqrt
import scipy as sp
import scipy.stats as st
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.ticker as plticker
import itertools

percentile_val = 95
plt.rcParams["xtick.major.pad"] = 10
plt.rcParams["ytick.major.pad"] = 10

# fp["xylabels_font_size"] = 26
# fp["ticks_font_size"] = 26
# fp["legend_size"] = 20


fp_default = {}

fp_default["figsize"] = (8, 6.5)
fp_default["dimensions"] = (0.15, 0.99, 0.93, 0.11)  # (0.17, 0.99, 0.97, 0.09)
fp_default["legend_position"] = (0.44, 1.1)
fp_default["xylabels_font_size"] = 16
fp_default["ticks_font_size"] = 14
fp_default["legend_size"] = 12
fp_default["hspace_subplots"] = 0.12
fp_default["wspace_subplots"] = 0.5
fp_default["legend_handle_length"] = 2
fp_default["legend_handle_height"] = 1.6
fp_default["legend_label_spacing"] = 0.5 #The vertical space between the legend entries
fp_default["legend_handle_textpad"] = 0.5
fp_default["legend_column_spacing"] = 2 #The spacing between columns
fp_default["marker_size"] = 6
fp_default["line_width"] = 1
fp_default["grid_color"] = "#D0D0D0"
fp_default["ticks_weight"] = "normal"  # "bold"
fp_default["numpoints"] = 1
fp_default["bars_width"] = 0.2
fp_default["bars_left_blank"] = 0.3
fp_default["legend_cols"] = 6
fp_default["xlabelpad"] = 10
fp_default["ylabelpad"] = 10
fp_default["titleFont"] = 8
fp_default["title_x"] = 0
fp_default["title_y"] = 1.0
fp_default["legendFrame"] = False
fp_default["colored"] = False
fp_default["axis_tick_pad"] = 7
fp_default["markers"] = True
fp_default["yAxesBothSides"] = False
fp_default["bar_edge_color"] = 'black'

comments = ""


# ===============================================================================
# cols = {"reqRate":"Requests per second",
#         "bucketSize":"Bucket size",
#         "avgLatency":"Average latency",
#         "buckets":"Buckets",
#         "counts":"Counts",
#         "sums":"Sums",
#         }
# ===============================================================================

# Borrowed from someone else
def lineSpecGenerator(lines=None, blank=False):
    """
        Return cycling list of lines
        [8, 4, 2, 4, 2, 4] means
        8 points on, (dash)
        4 points off,
        2 points on, (dot)
        4 points off,
        2 points on, (dot)
        4 points off.
    """

    if not lines:
        lines = [
            [10, 0.00001],  # solid
            [4, 2, 4, 2],  # dash dash
            [2, 2, 2, 2],  # dot dot
            [6, 4, 2, 4],  # ,4,8]
            [6, 4, 2, 4, 2, 4],  # dash dot dot
            [7, 5, 7, 5],  # dash dash
        ]
    index = 0
    while True:
        yield lines[index] if not blank else [0.00001, 1]
        index = (index + 1) % len(lines)


# Borrowed from someone else
def lineGenerator():
    "Return cycling list of lines"
    spec = ["-", "--", "-.", ":"]
    index = 0
    while True:
        yield spec[index]
        index = (index + 1) % len(spec)


def colorGenerator(colors=None):
    "Return cycling list of colors"
    if not colors:
        colors = ["#2b8cbe", "#7bccc4", "#b3cde3",
                  "#cc4c02", "#fe9929", "#fdcc8a",
                  "#052538", "#25536F", "#708FA1",
                  "#364031", "#6C7F63", "#A2BF94",
                  "#668F8C", "#07A5A8", "#B6AD91", "#71819A", "#4A97E8", "#947766", "orange",
                  "#ED3232", "#CC00CC", "#008837",
                  "#E08655", "#ca0020", "cyan", "#990033", "#00FF00", "#336699",
                  "purple", "red", "lightseagreen", "green", "blue", "#13A8A8",
                  "#D45622", "tan", "olive"]
    index = 0
    while True:
        yield colors[index]
        index = (index + 1) % len(colors)


def colorGeneratorGrey(colors=None):
    "Return cycling list of colors"
    if not colors:
        colors = ["#555555", "#777777", "#999999", "#aaaaaa", "#bbbbbb", "#cccccc"]
    index = 0
    while True:
        yield colors[index]
        index = (index + 1) % len(colors)
    # Borrowed from someone else


def markerGenerator(markers=None):
    "Return cycling list of colors"
    if not markers:
        markers = ["o", "s", "^", "*", "d", "v", "3", "4", "+"]
    index = 0
    while True:
        yield markers[index]
        index = (index + 1) % len(markers)


# Borrowed from someone else
def hatchGenerator():
    "Return cycling list of colors"
    hatches = ["/", "\\", "o", "-", "x", ".", "*"]
    index = 0
    while True:
        yield hatches[index]
        index = (index + 1) % len(hatches)


# Borrowed from someone else
def convertToStep(x, y):
    """Convert to a "stepped" data format by duplicating all but the last elt."""
    newx = []
    newy = []
    for i, d in enumerate(x):
        newx.append(d)
        newy.append(y[i])
        if i != len(x) - 1:
            newx.append(x[i + 1])
            newy.append(y[i])
    return newx, newy


# Borrowed from someone else
def convertToStepUpCDF(x, y):
    """Convert to a "stepped" data format by duplicating all but the last elt.

        Step goes up, rather than to the right.
        """
    newx = []
    newy = []
    for i, d in enumerate(x):
        newx.append(d)
        newy.append(y[i])
        if i != len(x) - 1:
            newx.append(x[i])
            newy.append(y[i + 1])
    newx.append(newx[-1])
    newy.append(1.0)
    return newx, newy


def xyMarkGenerator():
    markers = ["+", "o", "s", "^", "*", "d", "v", "3", "4"]
    colors = ["b", "g", "r", "c", "m", "y"]
    colors = ["orange", "#55C959", "#4A97E8", "#ED3232", "#CC00CC", "#008837",
              "#E08655", "#ca0020", "cyan", "#990033", "#00FF00", "#336699",
              "purple", "red", "lightseagreen", "green", "blue", "#13A8A8",
              "#D45622", "tan", "olive"]
    mIdx = 0
    cIdx = 0
    while True:
        yield "%s\"%s\"" % (markers[mIdx], colors[cIdx])
        mIdx = (mIdx + 1) % len(markers)
        cIdx = (cIdx + 1) % len(colors)


def plotTimeSeries(data, title, xlabel, ylabel, fp=fp_default, annotations=None, step=False, hline=False, yMin=None,
                   exact_ticks=False, show_legend=True, xticks=True, yticks=True, yMax=None, xMax=None, xMin=None,
                   subplot=None, first_plot=True, logx=False, logy=False, colors=None, sharey=None, lspec=None, markers=None):
    """Plot a time series.

    Input: data, a list of dicts.
    Each inner-most dict includes the following fields:
            x: array
            y: array
            label: string

    step: if True, display data in stepped form.
    """

    #            print "%s --- %s"%(title, data)

    if first_plot:
        plt.figure(figsize=fp["figsize"])

    if subplot:
        ax = plt.subplot(subplot, sharey=sharey)
    else:
        ax = plt.subplot(111)

    if fp["colored"]:
        cgen = colorGenerator(colors)
    else:
        cgen = colorGeneratorGrey(colors)

    lgen = lineSpecGenerator(lspec)
    mgen = markerGenerator(markers)

    if logx:
        plt.xscale("log")
    if logy:
        plt.yscale("log")

    # fig.canvas.set_window_title( title )
    if len(comments) > 0:
        title = title + "\n(" + comments + ")"

    plt.title(title, fontsize=fp["titleFont"], loc="left", x=fp["title_x"], y=fp["title_y"])

    for d in data:
        x = d["x"]
        y = d["y"]

        if step:
            x, y = convertToStep(x, y)

        if exact_ticks:
            x = np.arange(len(x))
            plt.xticks(x, d["x"])

        if fp["markers"]:
            plt.plot(x, y, label=d["label"], color=next(cgen), dashes=next(lgen), lw=fp["line_width"], marker=next(mgen), ms=fp["marker_size"])
        else:
            plt.plot(x, y, label=d["label"], color=next(cgen), dashes=next(lgen), lw=fp["line_width"])

        if "conf_ival" in d:
            plt.errorbar(x, y, yerr=d["conf_ival"], linestyle="None", marker="None", elinewidth=1, color="#9e9e9e", capsize=3)
        if annotations != None:
            for idx in range(len(annotations)):
                ax.annotate(annotations[idx], xy=(x[idx], y[idx]), xytext=(x[idx] + 0.1, y[idx] + 0.1))
        if "vline" in d:
            ax.plot((d["vline"], d["vline"]), (0, 1000000), 'k--')

        annotations = None
    if hline:
        plt.axhline(0.5, ls="--", c="silver")
        plt.axhline(percentile_val / 100.0, ls="--", c="silver")

    plt.xlabel(xlabel, fontsize=fp["xylabels_font_size"], multialignment="center", labelpad=fp["xlabelpad"])
    plt.ylabel(ylabel, fontsize=fp["xylabels_font_size"], multialignment="center", labelpad=fp["ylabelpad"])

    if yMin: plt.ylim(ymin=yMin)
    if yMax: plt.ylim(ymax=yMax)
    if xMax: plt.xlim(xmax=xMax)
    if xMin: plt.xlim(xmin=xMin)

    plt.grid(True, color=fp["grid_color"])
    if xticks:
        ax.tick_params(axis="both", which="major", pad=fp["axis_tick_pad"])
        for tick in ax.xaxis.get_major_ticks():
            tick.label1.set_fontsize(fp["ticks_font_size"])
    else:
        ax.set_xticks([])

    if yticks:
        for tick in ax.yaxis.get_major_ticks():
            tick.label1.set_fontsize(fp["ticks_font_size"])
    else:
        # [tk.set_visible(False) for tk in ax.get_yticklabels()]
        ax.label_outer()

    if fp["yAxesBothSides"]:
        ax.tick_params(labelright=True)

    if show_legend:
        handles, labels = ax.get_legend_handles_labels()
        ncols = fp["legend_cols"]
        #legend = plt.legend(_flip(handles, ncols), _flip(labels, ncols), loc=9,
        legend = plt.legend(handles, labels, loc=9,
                            prop={"size": fp["legend_size"]}, bbox_to_anchor=fp["legend_position"],
                            ncol=ncols, handlelength=fp["legend_handle_length"], numpoints=1,
                            handleheight=fp["legend_handle_height"], frameon=fp["legendFrame"],
                            labelspacing=fp["legend_label_spacing"], handletextpad=fp['legend_handle_textpad'],
                            columnspacing=fp['legend_column_spacing'],)
        if fp["legendFrame"]: legend.get_frame().set_color("white")
    else:
        plt.legend().set_visible(False)

    box = ax.get_position()
    ax.set_position([box.x0, box.y0 + box.height * 0.1, box.width, box.height * 0.8])

    left, right, top, bottom = fp["dimensions"]
    plt.subplots_adjust(left=left, right=right, top=top, bottom=bottom)

    return ax


def plotCDF(data, title, xlabel, ylabel, fp=fp_default, annotations=None, step=False,
            hline=True, N=0, yMin0=True, exact_ticks=False, xMax=None,
            show_legend=True, legend_loc=0, xticks=True, subplot=None,
            first_plot=True, logx=False, colors=None):
    data_mod = []
    xxMax = 0
    for index, line in enumerate(data):
        vals = sorted(line["y"])
        x = sorted(vals)
        #                    y = [(float(i) / len(x)) for i in range(len(x))]
        #                    x, y = convertToStepUpCDF(x, y)
        x1 = []
        y1 = []
        agg = 0.0
        for i in range(len(x)):
            v = x[i]
            while i < len(x) - 1 and x[i + 1] == v:
                i += 1

            x1.append(v)
            y1.append(float(i + 1) / len(x))

        mx = max(x1)
        if xxMax < mx: xxMax = mx

        entry = {}
        entry["x"] = x1
        entry["y"] = y1
        entry["label"] = line["label"]
        data_mod.append(entry)

    if xMax:
        xxMax = xMax
    else:
        xxMax += 0.5
    return plotTimeSeries(data_mod, title, xlabel, ylabel,
                          annotations=annotations, step=step, hline=hline, N=N,
                          yMin0=yMin0, exact_ticks=exact_ticks, show_legend=show_legend,
                          legend_loc=legend_loc, xticks=xticks, yMax=1.1, xMax=xxMax,
                          first_plot=first_plot, subplot=subplot, logx=logx, colors=colors, fp=fp)


def _flip(items, ncol):
    return itertools.chain(*[items[i::ncol] for i in range(ncol)])


def plotBars(res, title="", xlabel="", ylabel="", N=4, show_legend=True,
             show_height=False, subplot=None, first_plot=True, ystep_base=None,
             xticks=True, show_improvement=False, text_height_offset=0, show_text=False,
             xticks_text=None, yMax=None, xMin=None, xMax=None, yMin0=None, fp=fp_default, colors=None):
    if fp["colored"]:
        cgen = colorGenerator(colors)
    else:
        cgen = colorGeneratorGrey(colors)
    hgen = hatchGenerator()

    if first_plot:
        fig = plt.figure(figsize=fp["figsize"])

    if subplot:
        ax = plt.subplot(subplot)
    else:
        ax = plt.subplot(111)

    ind = np.arange(N)

    left_blank = fp["bars_left_blank"]
    width = fp["bars_width"]  # (1.0-left_blank)/len(res)

    base_line = res[0]["y"]
    for i in range(len(res)):
        r = res[i]
        color = next(cgen)

        hatch = None
        if fp["hatchgen"]:
            hatch = next(hgen)

        if "conf_ival" in r:
            # rects = ax.bar((i*width)+(i*left_blank), r["y"], width, zorder=3,
            #            ecolor="black", label=r["label"], color=color, hatch=hatch, align="edge")
            # ax.errorbar((i*width)+width/2+(i*left_blank),r["y"],yerr=r["conf_ival"],ecolor="black",
            #        barsabove=True,zorder=4)

            rects = ax.bar(left_blank + ind + (i * width), r["y"], width,
                           color=color, capsize=3, label=r["label"],edgecolor=fp['bar_edge_color'],
                           hatch=hatch, align="center", zorder=3)
            ax.errorbar(left_blank + ind + (i * width), r["y"], yerr=r["conf_ival"],
                        ecolor="#5c5c5c", zorder=4, barsabove=True, fmt="none", capsize=3)
        else:
            rects = ax.bar(i * left_blank + (i * width) + ind, r["y"], width, ecolor="black", label=r["label"],
                           color=color, hatch=hatch, align="center", zorder=3)

        for rectI, rect in enumerate(rects):
            height = rect.get_height()
            if show_height and not np.isnan(height):
                ax.text(rect.get_x() + rect.get_width(), height + text_height_offset, "%.3f" % (
                    float(height)), ha="right", va="bottom", fontsize=15, rotation="vertical",
                        weight=fp["ticks_weight"])
            elif show_improvement:
                improvement = float(height / base_line[rectI])
                ax.text(rect.get_x() + rect.get_width() / 3.0, height + text_height_offset,
                        "%.1gx" % improvement, ha="right", va="bottom", fontsize=14)
            elif show_text:
                text = r["text"][rectI]
                ax.text(rect.get_x() + rect.get_width() / 2.0, height + text_height_offset, "%s" % text,
                        ha="center", va="top", fontsize=fp["xylabels_font_size"], rotation="vertical")
            if xticks_text:
                text = r["x"][rectI]
                ax.text(rect.get_x() + rect.get_width() / 2, -1.15, "%s" % text, ha="center",
                        va="top", fontsize=fp["xylabels_font_size"], weight=fp["ticks_weight"])

    nTypes = len(res)
    plt.xticks(ind + nTypes * width / 2 + left_blank / 2, res[0]["x"])

    # plt.xticks(ind, res[0]["x"])

    plt.xlabel(xlabel, fontsize=fp["xylabels_font_size"], multialignment="center", labelpad=fp["xlabelpad"])
    plt.ylabel(ylabel, fontsize=fp["xylabels_font_size"], multialignment="center", labelpad=fp["ylabelpad"])

    if yMin0: plt.ylim(ymin=0)
    if yMax: plt.ylim(ymax=yMax)
    plt.xlim(xmax=xMax) if xMax else plt.xlim(xmax=N + 0.02)
    plt.xlim(xmin=xMin) if xMin else plt.xlim(xmin=-0.1)

    #     plt.ylim(0,max*1.58)
    plt.ylim(ymin=0, ymax=yMax)

    if len(comments) > 0:
        title = title + "\n(" + comments + ")"

    plt.title(title, fontsize=fp["titleFont"])

    #     plt.tick_params(labelright=True, labelsize=ticks_font_size)

    if xticks:
        ax.tick_params(axis="both", which="major", pad=fp["axis_tick_pad"])
        for tick in ax.xaxis.get_major_ticks():
            tick.label1.set_fontsize(fp["ticks_font_size"])
    else:
        ax.set_xticks([])

    for tick in ax.yaxis.get_major_ticks():
        tick.label1.set_fontsize(fp["ticks_font_size"])

    if ystep_base:
        loc = plticker.MultipleLocator(base=ystep_base)
        ax.yaxis.set_major_locator(loc)

    left, right, top, bottom = fp["dimensions"]
    plt.subplots_adjust(left=left, right=right, top=top, bottom=bottom,
                        wspace=fp["wspace_subplots"], hspace=fp["hspace_subplots"])

    if show_legend:
        ncols = fp["legend_cols"]
        handles, labels = ax.get_legend_handles_labels()
        legend = plt.legend(handles, labels, loc=9,
                            prop={"size": fp["legend_size"]}, bbox_to_anchor=fp["legend_position"],
                            ncol=ncols, frameon=fp["legendFrame"], handlelength=fp["legend_handle_length"],
                            handleheight=fp["legend_handle_height"], handletextpad=fp['legend_handle_textpad'],
                            labelspacing=fp["legend_label_spacing"], columnspacing=fp['legend_column_spacing'],)
        if fp["legendFrame"]: legend.get_frame().set_color("white")
    else:
        plt.legend().set_visible(False)

    plt.grid(axis="y", color=fp["grid_color"], zorder=2)

    return ax


def get_mean_and_conf(data, confidence=0.95):
    dataLen = len(data)
    if dataLen == 0:
        return 0, 0
    elif dataLen == 1:
        return data[0], 0

    # mean = np.mean(data)
    # se = st.sem(data)
    #
    # if se == 0:
    #     return mean, se
    #
    # # ival = se * st.t.ppf((1+confidence)/2., dataLen-1)
    # std = math.sqrt(se)
    # t_dist = st.t.ppf((1 + confidence) / 2., dataLen - 1)
    # ival = t_dist * std / math.sqrt(dataLen)
    #
    # return mean, ival

    # https://gist.github.com/gcardone/05276131b6dc7232dbaf
    mean = np.average(data)
    # evaluate sample variance by setting delta degrees of freedom (ddof) to
    # 1. The degree used in calculations is N - ddof
    stddev = np.std(data, ddof=1)
    # Get the endpoints of the range that contains 95% of the distribution
    t_bounds = st.t.interval(0.95, len(data) - 1)
    # sum mean to the confidence interval
    ci = [mean + critval * stddev / sqrt(len(data)) for critval in t_bounds]

    return mean, mean-ci[0]


def get_median_and_conf(values):
    values = np.sort(values)
    med = np.median(values)

    n = len(values)
    lower = n / 2 - 1.96 * np.sqrt(n) / 2
    upper = 1 + n / 2 + 1.96 * np.sqrt(n) / 2

    lower = int(math.ceil(lower))
    upper = int(upper)

    lower = values[lower]
    upper = values[upper]

    return med, [lower, upper]


def get_median_and_conf_weights(values, weights, cinterval=0.95):
    values = np.array(values)
    weights = np.array(weights)

    rv = st.rv_discrete(values=(values, weights / float(weights.sum())))
    med = rv.median()
    ci = rv.interval(cinterval)

    return med, ci


# Borrowed from someone else
def makefilmstrip(images, mode="RGB", color="white"):
    """Return a combined (filmstripped, each on top of the other) image of the images.
    """
    width = max(img.size[0] for img in images)
    height = sum(img.size[1] for img in images)

    image = Image.new(mode, (width, height), color)

    left, upper = 0, 0
    for img in images:
        image.paste(img, (left, upper))
        upper += img.size[1]

    return image


# Borrowed from someone else
def merge_plots(infiles, outfile):
    from glob import glob
    files = infiles
    images = map(Image.open, files)
    img = makefilmstrip(images)
    img.save(outfile)


# Borrowed from someone else
def save_figures(config, fname="figures.pdf"):
    pp = PdfPages("%s/%s" % (config["fig_dir"], fname))
    for i in plt.get_fignums():
        plt.figure(i)
        plt.savefig(pp, format="pdf")
    pp.close()


# Taken from wikipedia: http://en.wikipedia.org/wiki/Student%27s_t-distribution
# for degrees of freedom 1, 2, 3, ...
t_dist_95 = [-1,  # ignore this, just want to align indexes with the degree of freedom
             12.71, 4.303, 3.182, 2.776, 2.571,
             2.447, 2.365, 2.306, 2.262, 2.228,
             2.201, 2.179, 2.160, 2.145, 2.131,
             2.120, 2.110, 2.101, 2.093, 2.086,
             2.080, 2.074, 2.069, 2.064, 2.060]


def conf_ival_ratio(samples_a, samples_b):
    """ Ref: http://www.graphpad.com/FAQ/images/Ci%20of%20quotient.pdf
        For ratio Q = mean(samples_a) / mean(samples_b) 
    """
    a = np.mean(samples_a)
    b = np.mean(samples_b)
    q = a / b
    sem_a = st.sem(samples_a)
    sem_b = st.sem(samples_b)

    se_q = q * sqrt((sem_a ** 2 / a ** 2) + (sem_b ** 2 / b ** 2))
    ci = float(se_q * t_dist_95[len(samples_a) + len(samples_b) - 2])

    return (ci, ci)


def conf_ival_ratio_harvey(samples_a, samples_b):
    """ Ref: http://www.graphpad.com/FAQ/images/Ci%20of%20quotient.pdf
        For ratio Q = mean(samples_a) / mean(samples_b) 
    """
    a = np.mean(samples_a)
    b = np.mean(samples_b)
    q = a / b
    sem_b = st.sem(samples_b)
    sem_a = st.sem(samples_a)

    g = (t_dist_95[len(samples_a) + len(samples_b) - 2] * (sem_b / b)) ** 2

    if g >= 1:
        print("            =====> Error in confidence interval")
        return -1, -1

    se_q = (q / (1.0 - g)) * sqrt(((1 - g) * (sem_a ** 2 / a ** 2)) + (sem_b ** 2 / b ** 2))
    ci_l = float((q / (1.0 - g)) - (t_dist_95[len(samples_a) + len(samples_b) - 2] * se_q))
    ci_u = float((q / (1.0 - g)) + (t_dist_95[len(samples_a) + len(samples_b) - 2] * se_q))

    ci_l = q - ci_l  # it is because we want to make these absoulte numbers but pyplot will consider them as relative numbers. It will subtracti this number for q.
    ci_u = ci_u - q  # pyplot will add this number to q => ci_u = q+(ci_u-q)

    return (ci_l, ci_u)


def conf_ival_ratio_fieller(samples_a, samples_b):
    pass
