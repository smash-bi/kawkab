#!/usr/bin/python

from results_parser import get_test_id, cols
from plotutils import plotBars, plt, plotTimeSeries, plotCDF
import pprint as pp


def record_size_lat_thr(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, logY=False, save_fig=False, show_ci=True):
    lat_label = {
        'meanLat': "Average",
        'lat50': 'Median',
        'lat95': '95 percentile',
        'lat99': '99 percentile',
        'maxLat': 'Max'
    }

    colors = ["#2b8cbe", "#2b8cbe", "#2b8cbe", #"#2b8cbe",
              "#cc4c02", "#cc4c02", "#cc4c02", #"#cc4c02",
              ]

    markers = ["s", "o", "^", #"v",
               "o", "s", "^", #"v"
               ]

    lines = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        #[10, 0.00001],  # solid
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
        'markers': True,
    })

    for latType in ["meanLat"]:#["meanLat", "lat50", "lat95", "lat99", "maxLat"]:
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

def record_size_bars(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, save_fig=False, dataThr=False):
    colors = ["#2b8cbe", "#2b8cbe", "#2b8cbe", #"#2b8cbe",
              "#cc4c02", "#cc4c02", "#cc4c02", #"#cc4c02",
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
            if dataThr:
                y, y_ci = results[test_id]['agg_data']["dataThr"]

            xVals.append(x)
            yVals.append(y)
            yci.append(y_ci)

        if not dataThr:
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
    ylabel = "Records per second (x$10^6$)"
    if dataThr:
        ylabel = "Data throughput (MB/s)"

    print(title)
    pp.pprint(res_bundle)

    plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params,
             yMax=yMax, xMax=xMax, show_improvement=False, show_height=True)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.png" % (conf["fig_dir"], figPrefix), dpi=300)

def record_size_bars_lat(conf, results, latType, fig_params, figPrefix="", title="", xMax=None, yMax=None, save_fig=False):
    colors = ["#2b8cbe", "#2b8cbe", "#2b8cbe", #"#2b8cbe",
              "#cc4c02", "#cc4c02", "#cc4c02", #"#cc4c02",
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
            y, y_ci = results[test_id]['agg_data'][latType]
            xVals.append(x)
            yVals.append(y)
            yci.append(y_ci)

        yVals = [val / 1000.0 for val in yVals]
        yci = [val/1000.0 for val in yci]

        label = "%s" % (metric['label'])

        N = len(yVals)
        res = {}
        res["x"] = xVals
        res["y"] = yVals
        res["conf_ival"] = yci
        res["label"] = label
        res_bundle.append(res)

    xlabel = "Record size"
    ylabel = "%s request\ncompletion time (ms)" % (conf['labels'][latType])

    print(title)
    pp.pprint(res_bundle)

    plotBars(res_bundle, title, xlabel, ylabel, N=N, show_legend=True, fp=fig_params, yMax=yMax, xMax=xMax,
                show_improvement=True, show_height=False)

    if save_fig:
        plt.savefig("%s/%s.pdf" % (conf["fig_dir"], figPrefix))
        plt.savefig("%s/%s.eps" % (conf["fig_dir"], figPrefix))

def record_size_results_lines(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, logY=False, save_fig=False, show_ci=True):
    colors = ["#2b8cbe", "#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", "#cc4c02", "#cc4c02", "#cc4c02",
              ]

    markers = ["s", "o", "^", "v",
               "o", "s", "^", "v"
               ]

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
                if show_ci: res["conf_ival"] = yci
                res["label"] = label
                res_bundle.append(res)

        xlabel = "Records per second (x$10^6$)"
        ylabel = "%s request\ncompletion time (ms)" % (conf['labels'][latType])

        print(title)
        print(conf['labels'][latType])
        pp.pprint(res_bundle)

        plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fig_params,
                       yMax=yMax, xMax=xMax, logy=logY, yMin=0, xMin=-0.001,
                       colors=colors, markers=markers, lspec=lines)

        if save_fig:
            plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix,latType))
            plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix,latType))

def rs_cdf_results(conf, results, fig_params, figPrefix="", save_fig=False):
    colors = ["#2b8cbe", #"#2b8cbe", "#2b8cbe", "#2b8cbe",
              "#cc4c02", #"#cc4c02", "#cc4c02", "#cc4c02",
              'purple',
              #'green',
              '#515151',
              ]

    markers = ["s", "o", "^", "v",
               "o", "s", "^", "v"
               ]

    fgp = {}
    fgp.update(fig_params)
    fgp.update({
        'figsize':      (3.5, 2.2),
        'dimensions':   (0.215, 0.975, 0.835, 0.2),
        "legend_cols": 4,
        'legend_position':  (0.4, 1.33),
        'line_width': 0.8,
        'markers': False,
        'title_y': 1.1,
        'title_x': 0.25,
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
            for iat in point['iat']:
                params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients}
                test_id = get_test_id(conf, params)
                print(test_id)

                d = results[test_id]['agg_data']

                x, y = d['cdf']

                x, y = d['cdf']
                thr, _ = d["rpsThr"]
                thr = thr/1000000.0

                print("%d%% writes: %d mrs" % (metric_point, thr))
                print("p50=%g, p95=%g, p99=%g, max=%g"%(d['lat50'][0], d['lat95'][0], d['lat99'][0], d['maxLat'][0]))

                label = "%d-byte\nrecords" % (metric_point)

                res = {}
                res['x'] = x
                res["y"] = y
                res["label"] = label
                res_bundle.append(res)

        title = ''
        xlabel = "Request completion time ($\mu$s)"
        ylabel = "Fraction of requests"

        print('Write Ratio CDF')
        print(metric_name)

        plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp, logx=True, hline=True, xMin=100, yMin=0.001,
                       colors=colors, lspec=lines)

        if save_fig:
            plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix, metric_name))
            plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix, metric_name))
            plt.savefig("%s/%s-%s.png" % (conf["fig_dir"], figPrefix, metric_name), dpi=300)