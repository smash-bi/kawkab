#!/usr/bin/python

import json
import pprint as pp
from results_parser import _dcdf, _expand

from plotutils import plotBars, plt, plotTimeSeries

def _read_results(fname):
    with open(fname) as data_file:
        data = json.load(data_file)
    return data

def hist_read_results_hq13(conf, figparams):
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
    plt.savefig("%s/hist-timeline.png" % (conf["fig_dir"]), dpi=300)

def hist_tput_timeline(conf, figp, nptid, npiat, npnc, npnf, npStartOffset, npbs, npwr,
              ptid, piat, pnc, pnf, pbs, pwr, testTime, figprefix):
    nprx, npry = hist_tput(nptid, npiat, npnc, npnf, npStartOffset, npbs, npwr, 'read')
    #npry[0] = 0
    #npry[-1] = 0
    npres = {}
    npres["x"] = nprx
    npres["y"] = npry
    npres["label"] = 'Historical queries\non non-primary'

    pwx, pwy = hist_tput(ptid, piat, pnc, pnf, 0, pbs, pwr, 'write')
    pwres = {}
    pwres["x"] = pwx
    pwres["y"] = pwy
    pwres["label"] = 'Writes on primary'

    prx, pry = hist_tput(ptid, piat, pnc, pnf, 0, pbs, pwr, 'read')
    prres = {}
    prres["x"] = prx
    prres["y"] = pry
    prres["label"] = 'Realtime queries\non primary'

    figp['figsize'] = (6, 2.5)
    figp['dimensions'] = (0.12, 0.975, 0.85, 0.19)
    figp['legend_cols'] = 3
    figp['markers'] = False
    figp['legend_position'] = (0.47, 1.26)

    res_bundle = [prres, pwres, npres]

    title = ""
    xlabel = "Time (sec)"
    ylabel = "Records per second ($\\times 10^6$)"

    colors = ["#2b8cbe","#cc4c02", "#515151",]

    lspec = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [10, 0.00001],  # solid]
        [5, 1.5, 1.5, 1.5],
    ]

    plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=figp, xMin=-0.001, yMin=-0.001, xMax=testTime, lspec=lspec, colors=colors)

    plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figprefix))
    plt.savefig("%s/%s.eps" % (conf["fig_dir"], figprefix))
    plt.savefig("%s/%s.png" % (conf["fig_dir"], figprefix), dpi=300)

def hist_tput(tid, iat, nc, nf, startOffset, bs, wr, reqType):
    f = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-%s-nc%d-bs%d-rs16-nf%d-wr%d-iat%g/run_1/%s-results-hists.json'%(tid, nc, bs, nf, wr, iat, reqType)
    dres = _read_results(f)[0]
    res = dres["TputLog"]
    thrX = [v+startOffset for v in res['TimeSec']]
    thrY = res['Counts']
    factor = 1000000
    thrY = [v/factor for v in thrY]

    return thrX, thrY

def hist_lat_cdf(tid, iat, nc, nf, bs, wr, reqType):
    f = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-%s-nc%d-bs%d-rs16-nf%d-wr%d-iat%g/run_1/%s-results-hists.json'%(tid, nc, bs, nf, wr, iat, reqType)
    dres = _read_results(f)[0]
    lh = dres['Latency Histogram']
    lats = lh['latency']
    cnts = lh['count']
    lf = _expand(lats, cnts)
    cdf = _dcdf(lf)
    cdfX = cdf[0]
    cdfY = cdf[1]

    return cdfX, cdfY

def hist_read_cdf(conf, figp, nptid, npiat, npnc, npnf, npbs, npwr,
                  ptid, piat, pnc, pnf, pbs, pwr, figprefix):
    colors = ["#2b8cbe", #"#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", #"#cc4c02", "#cc4c02", "#cc4c02",
              '#515151',
              ]

    markers = ["s", "o", "^", "v",
               "o", "s", "^", "v"
               ]

    fgp = {}
    fgp.update(figp)
    fgp.update({
        'figsize': (3.5, 2.5),
        'dimensions': (0.19, 0.975, 0.80, 0.19),
        'line_width': 1,
        'markers': False,
        'legend_position': (0.4, 1.39),
        'title_y': 1.1,
        'title_x': 0.25,
        'legend_cols': 3,
    })

    lines = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [10, 0.00001],  # solid]
        [5, 1.5, 1.5, 1.5],
    ]

    nprx, npry = hist_lat_cdf(nptid, npiat, npnc, npnf, npbs, npwr, 'read')
    npres = {}
    npres["x"] = nprx
    npres["y"] = npry
    npres["label"] = 'Historical\nqueries\non non-primary'

    pwx, pwy = hist_lat_cdf(ptid, piat, pnc, pnf, pbs, pwr, 'write')
    pwres = {}
    pwres["x"] = pwx
    pwres["y"] = pwy
    pwres["label"] = 'Writes on\nprimary'

    prx, pry = hist_lat_cdf(ptid, piat, pnc, pnf, pbs, pwr, 'read')
    prres = {}
    prres["x"] = prx
    prres["y"] = pry
    prres["label"] = 'Realtime \nqueries\non primary'

    title = ''
    xlabel = "Request completion time ($\mu$s)"
    ylabel = 'CDF'# "CDF $P(X \\leq x)$"

    res_bundle = [prres, pwres, npres]

    plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp, logx=True, hline=True, xMin=100, yMin=0.001,
                   colors=colors, lspec=lines)

    plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figprefix))
    plt.savefig("%s/%s.eps" % (conf["fig_dir"], figprefix))
    plt.savefig("%s/%s.png" % (conf["fig_dir"], figprefix), dpi=300)

def hist_read_results_aws(conf, figParams):
    # nptid = 'awshq5r'; npnc = 4; npiat = 8; npbs = 250000; npnf = 100; npwr = 0
    # ptid = 'awshq5w-clp30'; pnc = 600; piat = 15; pbs = 1000; pnf = 1; pwr = 80
    # npStartOffset = 60
    # testTime = 180

    # nptid = 'awshq4r'; npnc = 6; npiat = 4; npbs = 500000; npnf = 100; npwr = 0
    # ptid = 'awshq4w-clp30'; pnc = 600; piat = 15; pbs = 1000; pnf = 1; pwr = 80
    # npStartOffset = 60
    # testTime = 180

    #nptid = 'awshq3r'; npnc = 10; npiat = 1; npbs = 1000000; npnf = 60; npwr = 0
    #nptid = 'awshq3r'; npnc = 10; npiat = 0.1; npbs = 1000000; npnf = 60; npwr = 0 # ---------------------------
    #nptid = 'awshq3r'; npnc = 10; npiat = 5; npbs = 1000000; npnf = 60; npwr = 0
    #nptid = 'awshq3r'; npnc = 1; npiat = 5; npbs = 1000000; npnf = 600; npwr = 0
    #nptid = 'awshq3r'; npnc = 1; npiat = 1; npbs = 1000000; npnf = 600; npwr = 0
    #nptid = 'awshq3r'; npnc = 1; npiat = 0.1; npbs = 1000000; npnf = 600; npwr = 0 #-------------------------
    #nptid = 'awshq3r'; npnc = 2; npiat = 2; npbs = 1000000; npnf = 300; npwr = 0
    ptid = 'awshq3w-clp30'; pnc = 600; piat = 15; pbs = 1000; pnf = 1; pwr = 80
    npStartOffset = 120
    testTime = 200

    #nptid = 'awshq3r'; npnc = 10; npnf = 60; npiat = 10; npbs = 500000; npwr = 0
    #nptid = 'awshq3r'; npnc = 1; npnf = 60; npiat = 7.5; npbs = 500000; npwr = 0
    #nptid = 'awshq3r'; npnc = 20; npnf = 30; npiat = 10; npbs = 500000; npwr = 0
    #nptid = 'awshq3r'; npnc = 5; npnf = 120; npiat = 7.5; npbs = 500000; npwr = 0
    #nptid = 'awshq3r'; npnc = 5; npnf = 60; npiat = 1; npbs = 500000; npwr = 0 #----------------
    #nptid = 'awshq3r'; npnc = 5; npnf = 60; npiat = 7.5; npbs = 500000; npwr = 0
    #ptid = 'awshq3w-clp30'; pnc = 600; piat = 15; pbs = 1000; pnf = 1; pwr = 80
    #npStartOffset = 60
    #testTime = 240

    hist_tput_timeline(conf, figParams, nptid, npiat, npnc, npnf, npStartOffset, npbs, npwr,
                       ptid, piat, pnc, pnf, pbs, pwr, testTime, 'hq_timeline_aws')

    hist_read_cdf(conf, figParams, nptid, npiat, npnc, npnf, npbs, npwr, ptid, piat, pnc, pnf, pbs, pwr, 'hq_cdf_aws')

def hist_read_results(conf, figParams):
    #tid = 'hq26'
    #rnc = 5
    #wnc = 400
    #rstart = 300
    #testTime = 600

    #tid = 'hq27'
    #rnc = 10
    #wnc = 400
    #rstart = 300
    #testTime = 600

    #nptid = 'hq26r'; npnc = 5; npiat = 0.5; npbs = 1000000; npnf = 80; npwr = 0
    #ptid = 'hq26w'; pnc = 400; piat = 10; pbs = 1000; pnf = 1; pwr = 80
    #npStartOffset = 300
    #testTime = 600

    # ---------------------------------------
    # nptid = 'hq29r'; npnc = 10; npiat = 0.5; npbs = 1000000; npnf = 80; npwr = 0
    # ptid = 'hq29w'; pnc = 800; piat = 10; pbs = 1000; pnf = 1; pwr = 80
    # npStartOffset = 260
    # testTime = 600

    nptid = 'hq31r'; npnc = 10; npiat = 0.5; npbs = 1000000; npnf = 80; npwr = 0
    ptid = 'hq31w'; pnc = 800; piat = 10; pbs = 1000; pnf = 1; pwr = 80
    npStartOffset = 550
    testTime = 900

    #nptid = 'hq30r'; npnc = 10; npiat = 0.5; npbs = 1000000; npnf = 80; npwr = 0
    #ptid = 'hq30w'; pnc = 800; piat = 10; pbs = 1000; pnf = 1; pwr = 80
    #npStartOffset = 800
    #testTime = 1200

    # tid = 'hq30'
    # rnc = 10
    # wnc = 800
    # rstart = 900
    #rnf = 80
    #rbs = 1000000
    #wiat = 10
    #wbs = 1000

    hist_tput_timeline(conf, figParams, nptid, npiat, npnc, npnf, npStartOffset, npbs, npwr,
              ptid, piat, pnc, pnf, pbs, pwr, testTime, 'hq_timeline')

    hist_read_cdf(conf, figParams, nptid, npiat, npnc, npnf, npbs, npwr, ptid, piat, pnc, pnf, pbs, pwr, 'hq_cdf')