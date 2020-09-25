#!/usr/bin/python

from results_parser import get_test_id, cols
from plotutils import plotBars, plt, plotTimeSeries
import pprint as pp


def batch_size_lat_thr(conf, results, fig_params, figPrefix="", title="", barGraph=False, xMax=None, yMax=None, logY=False, save_fig=False):
    lat_label = {
        'meanLat': "Average",
        'lat50': 'Median',
        'lat95': '95 perc.',
        'lat99': '99 perc.',
        'maxLat': 'Max'
    }

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

                label = "%s %d" % (metric['label'], metric_point)

                N = len(yVals)
                res = {}
                res["x"] = xVals
                res["y"] = yVals
                res["conf_ival"] = yci
                res["label"] = label
                res_bundle.append(res)

        xlabel = "Records per second (x$10^6$)"
        ylabel = "%s latency (ms)" % (lat_label[latType])

        print(title)
        print(lat_label[latType])
        pp.pprint(res_bundle)

        if barGraph:
            plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params, yMax=yMax, xMax=xMax, logy=logY)
        else:
            plotTimeSeries(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params, yMax=yMax, xMax=xMax, logy=logY, )

        if save_fig:
            plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix,latType))
            plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix,latType))

def batch_size_bars(conf, results, fig_params, figPrefix="", title="", barGraph=True, xMax=None, yMax=None, save_fig=False):
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

    xlabel = "Batch Size"
    ylabel = "Records per second(x$10^6$)"

    print(title)
    pp.pprint(res_bundle)

    if barGraph:
        plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params, yMax=yMax, xMax=xMax)
    else:
        plotTimeSeries(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params, yMax=yMax, xMax=xMax)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))
