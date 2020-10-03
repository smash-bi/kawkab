#!/usr/bin/python

import json
import pprint as pp

from plotutils import plotBars, plt, plotTimeSeries

def _read_results(fname):
    with open(fname) as data_file:
        data = json.load(data_file)
    return data

def hist_read_results(conf, figparams):
    readsFile = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-hq13r-nc80-bs10000-rs16-nf1-wr0-iat1/run_1/read-results-hists.json'
    writesFile = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-hq13w-nc80-bs1000-rs16-nf1-wr100-iat5/run_1/write-results-hists.json'

    r = _read_results(readsFile)[0]["TputLog"]
    w = _read_results(writesFile)[0]["TputLog"]

    rstart = 188
    rx = [x+rstart for x in r['TimeSec']]
    ry = r['Counts']
    wx = w['TimeSec']
    wy = w['Counts']
    ry[0] = 0
    ry[-1] = 0
    wy[-1] = 0

    factor = 1000000
    ry = [x/factor for x in ry]
    wy = [x/factor for x in wy]

    rres = {}
    rres["x"] = rx
    rres["y"] = ry
    rres["label"] = 'Reads on non-primary'

    wres = {}
    wres["x"] = wx
    wres["y"] = wy
    wres["label"] = 'Writes on primary'

    figp = {}
    figp.update(figparams)
    figp['figsize'] = (6, 2.5)
    figp['dimensions'] = (0.08, 0.975, 0.85, 0.19)
    figp['legend_cols'] = 2
    figp['markers'] = False


    res_bundle = [wres, rres]

    title = ""
    xlabel = "Time (sec)"
    ylabel = "Records per second ($\\times 10^6$)"

    colors = ["#2b8cbe","#cc4c02", "#fe9929",]

    lspec = [[10, 0.00001],  # solid]
             ]

    plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=figp, xMin=0.05, xMax=500, lspec=lspec, colors=colors)

    plt.savefig("%s/hist-timeline.pdf" % (conf["fig_dir"]))
    plt.savefig("%s/hist-timeline.eps" % (conf["fig_dir"]))

def hist_read_results_hq16(conf, figp):
    npReadsFile = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-hq16r-nc80-bs10000-rs16-nf1-wr0-iat1/run_1/read-results-hists.json'
    pWritesFile = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-hq16w-nc80-bs1000-rs16-nf1-wr80-iat10/run_1/write-results-hists.json'
    pReadsFile = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-hq16w-nc80-bs1000-rs16-nf1-wr80-iat10/run_1/read-results-hists.json'

    npr = _read_results(npReadsFile)[0]["TputLog"]
    pw = _read_results(pWritesFile)[0]["TputLog"]
    pr = _read_results(pReadsFile)[0]["TputLog"]

    rstart = 109
    nprx = [x+rstart for x in npr['TimeSec']]
    npry = npr['Counts']
    pwx = pw['TimeSec']
    pwy = pw['Counts']
    prx = pr['TimeSec']
    pry = pr['Counts']
    npry[0] = 0
    npry[-1] = 0
    # pwy[-1] = 0
    # pry[-1] = 0

    factor = 1000000
    npry = [x/factor for x in npry]
    pwy = [x/factor for x in pwy]
    pry = [x/factor for x in pry]


    npres = {}
    npres["x"] = nprx
    npres["y"] = npry
    npres["label"] = 'Historical queries\non non-primary'

    pwres = {}
    pwres["x"] = pwx
    pwres["y"] = pwy
    pwres["label"] = 'Writes on primary'

    prres = {}
    prres["x"] = prx
    prres["y"] = pry
    prres["label"] = 'Realtime queries\non primary'

    figp['figsize'] = (6, 2.5)
    figp['dimensions'] = (0.08, 0.975, 0.85, 0.19)
    figp['legend_cols'] = 3
    figp['markers'] = False
    figp['legend_position'] = (0.47, 1.26)


    res_bundle = [pwres, prres, npres]

    title = ""
    xlabel = "Time (sec)"
    ylabel = "Records per second ($\\times 10^6$)"

    colors = ["#2b8cbe","#cc4c02", "#fe9929",]

    lspec = [[10, 0.00001],  # solid]
             ]

    plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=figp, xMin=-0.001, yMin=-0.001, xMax=300, lspec=lspec, colors=colors)

    plt.savefig("%s/hist-timeline.pdf" % (conf["fig_dir"]))
    plt.savefig("%s/hist-timeline.eps" % (conf["fig_dir"]))
