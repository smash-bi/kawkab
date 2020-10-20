#!/usr/bin/python

import json
import pprint as pp

from plotutils import plotBars, plt, plotTimeSeries
import pandas as pd

def _read_results(fname):
    with open(fname) as data_file:
        data = json.load(data_file)
    return data

def burst_handling_stable_results(conf, figp, infile, outprefix, expTime, ann=None):
    w = _read_results(infile)[0]["TputLog"]

    wx = w['TimeSec']
    wy = w['Counts']
    #wy[-1] = 0

    factor = 1000000
    wy = [x/factor for x in wy]

    wres = {}
    wres["x"] = wx
    wres["y"] = wy
    wres["label"] = 'Write Requests'

    figp['figsize'] = (8, 2.5)
    figp['dimensions'] = (0.08, 0.975, 0.85, 0.19)
    figp['legend_cols'] = 1
    figp['markers'] = False
    figp['legend_position'] = (0.47, 1.26)
    figp['markers'] = False


    res_bundle = [wres]

    title = ""
    xlabel = "Time (sec)"
    ylabel = "Records per second ($\\times 10^6$)"

    colors = ["#2b8cbe","#cc4c02", "#fe9929",]

    lspec = [[10, 0.00001],  # solid]
             ]

    plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=figp, xMin=0.05, xMax=expTime, yMax=6,
                   lspec=lspec, colors=colors, annotations=ann)

    plt.savefig("%s/%s.pdf" % (conf["fig_dir"], outprefix))
    plt.savefig("%s/%s.eps" % (conf["fig_dir"], outprefix))

def burst_handling_stable_cache_results(conf, figp, infile, outprefix, expTime, expStartDIff, ann=None):
    cols = ['cacheOcc', 'lsOcc', 'lsCanEvict', 'gsQlen', 'time']
    d = pd.read_csv(infile, names=cols)

    #import pdb; pdb.set_trace()

    #w = _read_results(file)[0]["TputLog"]

    # Backend full at 540 seconds absolute
    # 15 seconds difference when the clients started and when the server started
    wx = d.time.tolist()[expStartDIff:]
    cocc = d.cacheOcc.tolist()[expStartDIff:]
    locc = d.lsOcc.tolist()[expStartDIff:]
    lev = d.lsCanEvict.tolist()[expStartDIff:]
    gsq = d.gsQlen.tolist()[expStartDIff:]

    wx = [int(x)-expStartDIff for x in wx]
    cocc = [float(x) for x in cocc]
    locc = [float(x) for x in locc]
    lev = [float(x) for x in lev]
    gsq = [float(x) for x in gsq]


    cod = {}
    cod["x"] = wx
    cod["y"] = cocc
    cod["label"] = 'Cache occupancy'

    lod = {}
    lod["x"] = wx
    lod["y"] = locc
    lod["label"] = 'Local store occupancy'

    led = {}
    led["x"] = wx
    led["y"] = lev
    led["label"] = '% evictable from\nlocal store'

    gsd = {}
    gsd["x"] = wx
    gsd["y"] = gsq
    gsd["label"] = '% of local store chunk\nin global store queue'


    figp['figsize'] = (8, 2.5)
    figp['dimensions'] = (0.08, 0.975, 0.85, 0.19)
    figp['legend_cols'] = 4
    figp['markers'] = False
    figp['legend_position'] = (0.47, 1.26)
    figp['markers'] = False


    res_bundle = [cod, lod, led, gsd]

    title = ""
    xlabel = "Time (sec)"
    ylabel = "Percentage"

    colors = ["#2b8cbe","#cc4c02", "#fe9929", 'purple']

    lspec = [[10, 0.00001],  # solid]
             ]

    plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=figp, xMin=0.01, xMax=expTime, yMax=105,
                   lspec=lspec, colors=colors, annotations=ann)

    plt.savefig("%s/%s.pdf" % (conf["fig_dir"], outprefix))
    plt.savefig("%s/%s.eps" % (conf["fig_dir"], outprefix))

def burst_handling_results(conf, figParams):
    # sexpTime = 700
    # sexpStartDiff = 15
    # stableTput = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-stable13-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/write-results-hists.json'
    # stableCache = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-stable13-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/servers/server-0-cache.txt'
    # stfile = '06-burst_stable_tput'
    # scfile = '06-burst_stable_cache'
    #
    # uexpTime = 350
    # uexpStartDiff = 15
    # unstableTput = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-unstable13-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/write-results-hists.json'
    # unstableCache = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-unstable13-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/servers/server-0-cache.txt'
    # ustfile = '06-burst_unstable_tput'
    # uscfile = '06-burst_unstable_cache'
    #
    # sann = [{'text':'Local store\nis full', 'xy':(640, 1.8), 'xytext':(550, 1)}]
    # scann = [{'text':'Local store\nis full', 'xy':(650, 75), 'xytext':(650, 35)},
    #         {'text':'Global store is full', 'xy':(523, 28), 'xytext':(420, 42)}]
    # usann = [{'text':'Local store\nis full', 'xy':(285, 0.7), 'xytext':(310, 2.5)},]
    # uscann = [{'text':'Local store\nis full', 'xy':(283, 75), 'xytext':(283, 40)},]

    sexpTime = 700
    sexpStartDiff = 18
    stableTput = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-stable15-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/write-results-hists.json'
    stableCache = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-stable15-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/servers/server-0-cache.txt'
    stfile = '06-burst_stable_tput'
    scfile = '06-burst_stable_cache'

    uexpTime = 325
    uexpStartDiff = 18
    unstableTput = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-unstable15-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/write-results-hists.json'
    unstableCache = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-unstable15-nc200-bs100-rs64-nf1-wr100-iat2.2/run_1/servers/server-0-cache.txt'
    ustfile = '06-burst_unstable_tput'
    uscfile = '06-burst_unstable_cache'

    sann = [{'text':'Local store\nis full', 'xy':(650, 1.8), 'xytext':(575, 1)}]
    scann = [{'text':'Local store\nis full', 'xy':(660, 75), 'xytext':(660, 35)},
             {'text':'Global store is full', 'xy':(525, 30), 'xytext':(450, 42)}]
    usann = [{'text':'Local store\nis full', 'xy':(275, 0.7), 'xytext':(235, 1)},]
    uscann = [{'text':'Local store\nis full', 'xy':(276, 75), 'xytext':(276, 40)},]

    burst_handling_stable_results(conf, figParams, stableTput, stfile, sexpTime, sann)
    burst_handling_stable_cache_results(conf, figParams, stableCache, scfile, sexpTime, sexpStartDiff, scann)
    burst_handling_stable_results(conf, figParams, unstableTput, ustfile, uexpTime, usann)
    burst_handling_stable_cache_results(conf, figParams, unstableCache, uscfile, uexpTime, uexpStartDiff, uscann)