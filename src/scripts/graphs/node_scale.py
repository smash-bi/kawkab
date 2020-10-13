#!/usr/bin/python

from results_parser import get_test_id, cols
from plotutils import plotBars, plt, plotTimeSeries
import pprint as pp

def node_scale_bars(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, save_fig=False):
    colors = ["#2b8cbe", #"#2b8cbe", "#2b8cbe", #"#2b8cbe",
              #"#cc4c02", "#cc4c02", "#cc4c02", #"#cc4c02",
              ]


    res_bundle = []
    for metric in conf['metric']:
        metric_name = metric['name']
        metric_points = metric['points']
        typ = metric['type']
        xVals = []
        yVals = []
        yci = []
        for point in metric_points:
            metric_point = point['val']
            prefix = point['prefix']
            if 'num_clients' in point: clients = point['num_clients']
            iat = point['iat'][0]

            params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients}
            test_id = get_test_id(conf, params)
            print(test_id)
            x = metric_point
            y, y_ci = results[test_id]['agg_data']["rpsThr"]
            xVals.append(x)
            yVals.append(y)
            yci.append(y_ci)

        yVals = [val / 1000000.0 for val in yVals]
        yci = [val/1000000.0 for val in yci]

        label = "%s" % (metric['label'])

        N = len(yVals)
        res = {}
        res["x"] = xVals
        res["y"] = yVals
        res["conf_ival"] = yci
        res["label"] = label
        res_bundle.append(res)

    xlabel = "Batch size"
    ylabel = "Records per second(x$10^6$)"

    print(title)
    pp.pprint(res_bundle)

    plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params,
             yMax=yMax, xMax=xMax, show_improvement=False, show_height=False)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))

def node_scale_line(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, logY=False, save_fig=False, show_ci=True):
    colors = ["#2b8cbe", "#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", "#cc4c02", "#cc4c02", "#cc4c02",
              ]

    markers = ["s", "o", "^", "v",
               "o", "s", "^", "v"
               ]

    lines = [
        [10, 0.00001],  # solid
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
    ]

    res_bundle = []
    for metric in conf['metric']:
        metric_name = metric['name']
        metric_points = metric['points']
        typ = metric['type']
        xVals = []
        yVals = []
        yci = []
        for point in metric_points:
            metric_point = point['val']
            prefix = point['prefix']
            if 'num_clients' in point: clients = point['num_clients']
            for iat in point['iat']:
                params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients}
                test_id = get_test_id(conf, params)
                print(test_id)
                x = metric_point
                y, y_ci = results[test_id]['agg_data']["rpsThr"]
                xVals.append(x)
                yVals.append(y)
                yci.append(y_ci)

        yVals = [val / 1000000.0 for val in yVals]
        yci = [val/1000000.0 for val in yci]

        label = metric['label']

        N = len(yVals)
        res = {}
        res["x"] = xVals
        res["y"] = yVals
        if show_ci: res["conf_ival"] = yci
        res["label"] = label
        res_bundle.append(res)

    ylabel = "Records per second (x$10^6$)"
    xlabel = "Number of nodes"

    print(title)
    pp.pprint(res_bundle)

    plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fig_params,
                   yMax=yMax, xMax=xMax, logy=logY, yMin=0, xMin=-0.001,
                   colors=colors, markers=markers, lspec=lines)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))