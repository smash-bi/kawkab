#!/usr/bin/python

from results_parser import get_test_id, cols
from plotutils import plotBars, plt, plotTimeSeries, plotCDF
import pprint as pp


def thr_lat_iat(conf, results, metric, fig_params, figPrefix="", title="", barGraph=False, xMax=None, yMax=None, save_fig=False):
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
            xVals = []
            yVals = []
            yci = []
            metric_name = metric['name']
            metric_points = metric['points']
            typ = metric['type']
            prefix = metric['prefix']
            for point in metric_points:
                metric_point = point['val']
                for iat in point['iat']:
                    params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat}
                    test_id = get_test_id(conf, params)
                    x, _ = results[test_id]['agg_data']["rpsThr"]
                    y, y_ci = results[test_id]['agg_data'][latType]
                    xVals.append(x)
                    yVals.append(y)
                    yci.append(y_ci)

                xVals = [(val / 1000000.0) for val in xVals]
                yVals = [val / 1000.0 for val in yVals]
                yci = [val/1000.0 for val in yci]

                label = "%s %d" % (conf['labels'][prefix], metric_point)

                N = len(yVals)
                res = {}
                res["x"] = xVals
                res["y"] = yVals
                res["conf_ival"] = yci
                res["label"] = label
                res_bundle.append(res)

                [print('%s, %.2f, %.2f'%(x, y, z)) for x,y,z in zip(xVals, yVals, yci)]
        xlabel = "Records per second (x$10^6$)"
        ylabel = "%s request completion time (ms)" % (lat_label[latType])

        print(title)
        print(lat_label[latType])
        pp.pprint(res_bundle)

        if barGraph:
            plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fgp=fig_params, yMax=yMax, xMax=xMax)
        else:
            plotTimeSeries(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params, yMax=yMax, xMax=xMax)

        if save_fig:
            plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix,latType))
            plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix,latType))


def lat_cdf(conf, results, fig_params, figPrefix="", save_fig=False):
    colors = ["#2b8cbe", #"#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", #"#cc4c02", "#cc4c02", "#cc4c02",
              'purple',
              'green',
              ]

    markers = ["s", "o", "^", "v",
               "o", "s", "^", "v"
               ]

    fgp = {}
    fgp.update(fig_params)
    fgp.update({
        'figsize': (4, 2.5),
        'dimensions': (0.19, 0.975, 0.85, 0.19),
        'line_width': 0.8,
        'markers': False,
        'marker_size':0.5,
        'legend_position': (0.45, 1.18),
        'title_y': 1.1,
        'title_x': 0.25,
        'legend_cols': 4,
    })

    lines = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        [10, 0.00001],  # solid
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        [10, 0.00001],  # solid


        #[6, 4, 2, 4, 2, 4],  # dash dot dot
        #[7, 5, 7, 5],  # dash dash
    ]

    for metric in conf['metric']:
        metric_name = metric['name']
        metric_points = metric['points']
        typ = metric['type']
        res_bundle = []
        for point in metric_points:
            metric_point = point['val']
            prefix = point['prefix']
            if 'num_clients' in point: clients = point['num_clients']
            if 'batch_size' in point: batch_size = point['batch_size']
            for iat in point['iat']:
                params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients, 'batch_size':batch_size}
                test_id = get_test_id(conf, params)
                print(test_id)
                x, y = results[test_id]['agg_data']['cdf']
                #x, y = results[test_id]['agg_data']['pdf']
                thr, _ = results[test_id]['agg_data']["rpsThr"]
                thr = thr/1000000.0

                label = "%d%% writes\n(%.2gm)" % (metric_point, thr)

                res = {}
                res['x'] = x
                res["y"] = y
                res["label"] = label
                res_bundle.append(res)

        title = '%s (%s %d)'%(metric['label'], metric_name, metric_point)
        xlabel = "Request completion time ($\mu$s)"
        ylabel = "CDF ($p \\leq x$)"

        plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp, logx=True, hline=True, xMin=100, yMin=0.001,
                colors=colors, lspec=lines, noLine=False)

        if save_fig:
            plt.savefig("%s/%s-%s-%g.pdf" % (conf["fig_dir"], figPrefix, metric_name, metric_point))
            plt.savefig("%s/%s-%s-%g.eps" % (conf["fig_dir"], figPrefix, metric_name, metric_point))
            plt.savefig("%s/%s-%s-%g.png" % (conf["fig_dir"], figPrefix, metric_name, metric_point), dpi=300)

def rs_bars_datathr(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, save_fig=False):
    colors = ["#2b8cbe", #"#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", #"#cc4c02", "#cc4c02", "#cc4c02",
              'purple',
              'green',
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
            #y, y_ci = results[test_id]['agg_data']["rpsThr"]
            y, y_ci = results[test_id]['agg_data']["dataThr"]
            xVals.append(x)
            yVals.append(y)
            yci.append(y_ci)

        #yVals = [val / 1000000.0 for val in yVals]
        #yci = [val/1000000.0 for val in yci]

        label = "%s" % (metric['label'])

        N = len(yVals)
        res = {}
        res["x"] = xVals
        res["y"] = yVals
        res["conf_ival"] = yci
        res["label"] = label
        res_bundle.append(res)

    xlabel = "Batch size"
    ylabel = "Throughput (MB/s)"

    print(title)
    pp.pprint(res_bundle)

    plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params,
             yMax=yMax, xMax=xMax, show_improvement=False, show_height=False)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.png" % (conf["fig_dir"], figPrefix), dpi=300)

def rs_bars_rps(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, save_fig=False):
    colors = ["#2b8cbe", #"#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", #"#cc4c02", "#cc4c02", "#cc4c02",
              'purple',
              'green',
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

    xlabel = "Record size (bytes)"
    ylabel = "Records per second(x$10^6$)"

    print(title)
    pp.pprint(res_bundle)

    plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params,
             yMax=yMax, xMax=xMax, show_improvement=False, show_height=False)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.png" % (conf["fig_dir"], figPrefix), dpi=300)

def lat_rs_cdf(conf, results, fig_params, figPrefix="", save_fig=False):
    colors = ["#2b8cbe", #"#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", #"#cc4c02", "#cc4c02", "#cc4c02",
              'purple',
              'green',
              ]

    markers = ["s", "o", "^", "v",
               "o", "s", "^", "v"
               ]

    fgp = {}
    fgp.update(fig_params)
    fgp.update({
        'figsize': (3.5, 2.5),
        'dimensions': (0.19, 0.975, 0.85, 0.19),
        'line_width': 0.8,
        'markers': False,
        'legend_position': (0.45, 1.18),
        'title_y': 1.1,
        'title_x': 0.25,
        'legend_cols': 4,
    })

    lines = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        [10, 0.00001],  # solid
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        [10, 0.00001],  # solid


        #[6, 4, 2, 4, 2, 4],  # dash dot dot
        #[7, 5, 7, 5],  # dash dash
    ]

    for metric in conf['metric']:
        metric_name = metric['name']
        metric_points = metric['points']
        typ = metric['type']
        for point in metric_points:
            res_bundle = []
            metric_point = point['val']
            prefix = point['prefix']
            if 'num_clients' in point: clients = point['num_clients']
            for iat in point['iat']:
                params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients}
                test_id = get_test_id(conf, params)
                print(test_id)
                x, y = results[test_id]['agg_data']['cdf']
                thr, _ = results[test_id]['agg_data']["rpsThr"]
                thr = thr/1000000.0

                label = "Tput %.2gm" % (thr)

                res = {}
                res['x'] = x
                res["y"] = y
                res["label"] = label
                res_bundle.append(res)

            title = '%s (%s %d)'%(metric['label'], metric_name, metric_point)
            xlabel = "Request completion time ($\mu$s)"
            ylabel = "CDF ($p \\leq x$)"

            plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp, logx=True, hline=True, xMin=100, yMin=0.001,
                           colors=colors, lspec=lines)

            if save_fig:
                plt.savefig("%s/%s-%s-%g.pdf" % (conf["fig_dir"], figPrefix, metric_name, metric_point))
                plt.savefig("%s/%s-%s-%g.eps" % (conf["fig_dir"], figPrefix, metric_name, metric_point))
                plt.savefig("%s/%s-%s-%g.png" % (conf["fig_dir"], figPrefix, metric_name, metric_point), dpi=300)