#!/usr/bin/python

from results_parser import get_test_id, cols
from plotutils import plotBars, plt, plotTimeSeries
import pprint as pp

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

def wr_cdf_results(conf, results, fig_params, figPrefix="", save_fig=False):
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

                label = "%d%%\nwrites" % (metric_point)

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
            plt.savefig("%s/%s-%s-%g.pdf" % (conf["fig_dir"], figPrefix, metric_name, metric_point))
            plt.savefig("%s/%s-%s-%g.eps" % (conf["fig_dir"], figPrefix, metric_name, metric_point))
            plt.savefig("%s/%s-%s-%g.png" % (conf["fig_dir"], figPrefix, metric_name, metric_point), dpi=300)

def write_ratio_results(conf, results, latTypes, fig_params, figPrefix="", title="", xMax=None, yMax=None, logY=False, save_fig=False, show_ci=True):
    #colors = ["#2b8cbe", "#cc4c02", "#E08655", "#ED3232", "#CC00CC", "#008837", ]

    markers = ["s", "o", "^", "v", "o", "s", "^", "v"]
    #For writes
    colors = ["#2b8cbe","#cc4c02", "#BF3B9E", "#515151",]
    lines = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        [10, 0.00001],  # solid
    ]

    fgp = {}
    fgp.update(fig_params)
    fgp.update({
        #'dimensions':   (0.21, 0.975, 0.85, 0.19),
        'markers': False,
        'figsize':      (3.5, 2.2),
        'dimensions':   (0.215, 0.975, 0.835, 0.2),
        "legend_cols": 4,
        'legend_position':  (0.4, 1.33),
    })

    #latTypes = ["meanLat", "lat50", "lat95", "lat99", "maxLat"]

    for metric in conf['metric']:
        metric_name = metric['name']
        metric_points = metric['points']
        mlabel = metric['label']
        typ = metric['type']
        for latType in latTypes:
            res_bundle = []
            for point in metric_points:
                xVals = []
                yVals = []
                yci = []
                metric_point = point['val']
                prefix = point['prefix']
                res_file = point['res_file']
                if 'num_clients' in point: clients = point['num_clients']
                if 'batch_size' in point: batch_size = point['batch_size']
                for iat in point['iat']:
                    params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients, 'batch_size':batch_size}
                    test_id = get_test_id(conf, params)
                    print(test_id)
                    x, _ = results[test_id+res_file]['agg_data']["rpsThr"]
                    y, y_ci = results[test_id+res_file]['agg_data'][latType]
                    xVals.append(x)
                    yVals.append(y)
                    yci.append(y_ci)

                xVals = [(val / 1000000.0) for val in xVals]
                #yVals = [val / 1000.0 for val in yVals]
                #yci = [val/1000.0 for val in yci]

                label = "%d%%\n%s" % (metric_point,mlabel)
                if mlabel is 'reads':
                    label = "%d%% %s" % (100-metric_point,mlabel)

                N = len(yVals)
                res = {}
                res["x"] = xVals
                res["y"] = yVals
                if show_ci: res["conf_ival"] = yci
                res["label"] = label
                res_bundle.append(res)

            xlabel = "Records per second (x$10^6$)"
            ylabel = "%s request\ncompletion time ($\\mu$s)" % (conf['labels'][latType])
            #ylabel = "%s request\ncompletion time (ms)" % (conf['labels'][latType])

            print(title)
            print(conf['labels'][latType])
            pp.pprint(res_bundle)

            plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp,
                           xMax=xMax, logy=logY, yMin=100, xMin=-0.001, yMax=1000000,
                           colors=colors, markers=markers, lspec=lines)

            if save_fig:
                plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix,latType))
                plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix,latType))

def write_ratio_thr_lat(conf, results, latTypes, fig_params, figPrefix="", title="", xMax=None, yMax=None, logY=False, save_fig=False, show_ci=True):
    #colors = ["#2b8cbe", "#cc4c02", "#E08655", "#ED3232", "#CC00CC", "#008837", ]

    markers = ["s", "o", "^", "v", "o", "s", "^", "v"]
    #For writes
    colors = ["#2b8cbe","#cc4c02", "#BF3B9E", "#3BBF9C",]
    lines = [
        [5, 2, 5, 2],  # dash dash
        [2, 2, 2, 2],  # dot dot
        [5, 1.5, 1.5, 1.5],
        [10, 0.00001],  # solid
    ]

    #For reads
    # colors = ["#BF3B9E", "#cc4c02", "#2b8cbe",]
    # lines = [
    #     [5, 1.5, 1.5, 1.5],
    #     [2, 2, 2, 2],  # dot dot
    #     [5, 2, 5, 2],  # dash dash
    # ]

    fgp = {}
    fgp.update(fig_params)
    fgp.update({
        #'dimensions':   (0.21, 0.975, 0.85, 0.19),
        'markers': False,
        'figsize':      (3.5, 2.2),
        'dimensions':   (0.215, 0.975, 0.835, 0.2),
        "legend_cols": 4,
        'legend_position':  (0.4, 1.33),
    })

    for metric in conf['metric']:
        metric_name = metric['name']
        metric_points = metric['points']
        mlabel = metric['label']
        typ = metric['type']
        for latType in latTypes:
            res_bundle = []
            for point in metric_points:
                xVals = []
                yVals = []
                yci = []
                metric_point = point['val']
                prefix = point['prefix']
                res_file = point['res_file']
                if 'num_clients' in point: clients = point['num_clients']
                if 'batch_size' in point: batch_size = point['batch_size']
                for iat in point['iat']:
                    params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients, 'batch_size':batch_size}
                    test_id = get_test_id(conf, params)
                    print(test_id)
                    x, _ = results[test_id+res_file]['agg_data']["rpsThr"]
                    y, y_ci = results[test_id+res_file]['agg_data'][latType]
                    xVals.append(x)
                    yVals.append(y)
                    yci.append(y_ci)

                xVals = [(val / 1000000.0) for val in xVals]
                yVals = [val / 1.0 for val in yVals]
                #yVals = [val / 1000.0 for val in yVals]
                #yci = [val/1000.0 for val in yci]

                label = "%d%%\n%s" % (metric_point,mlabel)
                if mlabel is 'reads':
                    label = "%d%% %s" % (100-metric_point,mlabel)

                N = len(yVals)
                res = {}
                res["x"] = xVals
                res["y"] = yVals
                if show_ci: res["conf_ival"] = yci
                res["label"] = label
                res_bundle.append(res)

            xlabel = "Records per second (x$10^6$)"
            ylabel = "%s request\ncompletion time ($\\mu$s)" % (conf['labels'][latType])

            print(title)
            print(conf['labels'][latType])
            pp.pprint(res_bundle)

            plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp,
                           xMax=xMax, logy=logY, xMin=-0.001, yMin=100,# yMin=0.9999, #yMax=10000,
                           colors=colors, markers=markers, lspec=lines)

            if save_fig:
                plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix,latType))
                plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix,latType))
