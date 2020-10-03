#!/usr/bin/python

from results_parser import get_test_id, cols
from plotutils import plotBars, plt, plotTimeSeries
import pprint as pp


def stream_scale_lines(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, logY=False, save_fig=False, show_ci=True):
    lat_label = {
        'meanLat': "Average",
        'lat50': 'Median',
        'lat95': '95 percentile',
        'lat99': '99 percentile',
        'maxLat': 'Max'
    }

    colors = ["#2b8cbe",
              "#cc4c02",
              ]

    markers = ["s", "o", "^", "v", "o", "s", "^", "v"]

    lines = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        [10, 0.00001],  # solid
        #[6, 4, 2, 4, 2, 4],  # dash dot dot
        #[7, 5, 7, 5],  # dash dash
    ]

    fgp = {}
    fgp.update(fig_params)
    fgp.update({
        'figsize':      (6, 2.5),
        'dimensions':   (0.096, 0.975, 0.85, 0.19),
        'legend_cols':  4,
        'legend_position':  (0.5, 1.29),
        'markers': False,
    })

    for latType in ["meanLat", "lat50", "lat95", "lat99", "maxLat"]:
        res_bundle = []
        for metric in conf['metric']:
            metric_name = metric['name']
            metric_points = metric['points']
            typ = metric['type']
            for point in metric_points:
                xVals = []
                yVals = []
                yci = []
                metric_point = point['val']
                prefix = point['prefix']
                if 'num_clients' in point: clients = point['num_clients']
                for iat in point['iat']:
                    params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients}
                    test_id = get_test_id(conf, params)
                    print(test_id)
                    x, _ = results[test_id]['agg_data']["rpsThr"]
                    y, y_ci = results[test_id]['agg_data'][latType]
                    xVals.append(x)
                    yVals.append(y)
                    yci.append(y_ci)

                xVals = [(val / 1000000.0) for val in xVals]
                yVals = [val / 1000.0 for val in yVals]
                yci = [val/1000.0 for val in yci]

                label = "%s %d" % (metric['label'], (metric_point*conf['num_clients'][0]))

                N = len(yVals)
                res = {}
                res["x"] = xVals
                res["y"] = yVals
                if show_ci: res["conf_ival"] = yci
                res["label"] = label
                res_bundle.append(res)

        xlabel = "Records per second (x$10^6$)"
        ylabel = "%s response time (ms)" % (lat_label[latType])

        print(title)
        print(lat_label[latType])
        pp.pprint(res_bundle)

        plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp, yMax=yMax, xMax=xMax, logy=logY, yMin=0, xMin=-0.001,
                       colors=colors, markers=markers, lspec=lines)

        if save_fig:
            plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix,latType))
            plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix,latType))

def stream_scale_results_bars(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, save_fig=False, show_ci=True):
    lat_label = {
        'meanLat': "Average",
        'lat50': 'Median',
        'lat95': '95 percentile',
        'lat99': '99 percentile',
        'maxLat': 'Max'
    }

    fgp = {}
    fgp.update(fig_params)
    fgp.update({
        'figsize':      (6, 2.5),
        'dimensions':   (0.096, 0.975, 0.85, 0.19),
        'legend_cols':  4,
        'legend_position':  (0.5, 1.29),
        'markers': False,
    })

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
            params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':point['iat'][0], 'num_clients':clients}
            test_id = get_test_id(conf, params)
            print(test_id)
            x = metric_point*conf['num_clients'][0]
            y, y_ci = results[test_id]['agg_data']["rpsThr"]
            xVals.append(x)
            yVals.append(y)
            yci.append(y_ci)

        yVals = [val / 1000000 for val in yVals]
        yci = [val/1000000 for val in yci]

        label = "%s" % (metric['label'])

        N = len(yVals)
        res = {}
        res["x"] = xVals
        res["y"] = yVals
        if show_ci: res["conf_ival"] = yci
        res["label"] = label
        res_bundle.append(res)

    xlabel = "Number of concurrent streams"
    ylabel = "Records per second ($\\times 10^6$)"

    print(title)
    pp.pprint(res_bundle)

    plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params, yMax=yMax, xMax=xMax, show_improvement=False)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))
