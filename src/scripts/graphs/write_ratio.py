#!/usr/bin/python

from results_parser import get_test_id, cols
from plotutils import plotBars, plt, plotTimeSeries
import pprint as pp


def write_ratio_results(conf, results, fig_params, figPrefix="", title="", xMax=None, yMax=None, logY=False, save_fig=False, show_ci=True):
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
        for latType in ["lat50", "lat95"]:#["meanLat", "lat50", "lat95", "lat99", "maxLat"]:
            res_bundle = []
            for point in metric_points:
                xVals = []
                yVals = []
                yci = []
                metric_point = point['val']
                prefix = point['prefix']
                res_file = point['res_file']
                if 'num_clients' in point: clients = point['num_clients']
                for iat in point['iat']:
                    params = {"test_type": typ, 'test_prefix':prefix, metric_name: metric_point, 'iat':iat, 'num_clients':clients}
                    test_id = get_test_id(conf, params)
                    print(test_id)
                    x, _ = results[test_id+res_file]['agg_data']["rpsThr"]
                    y, y_ci = results[test_id+res_file]['agg_data'][latType]
                    xVals.append(x)
                    yVals.append(y)
                    yci.append(y_ci)

                xVals = [(val / 1000000.0) for val in xVals]
                yVals = [val / 1000.0 for val in yVals]
                yci = [val/1000.0 for val in yci]

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
            ylabel = "%s request\ncompletion time (ms)" % (conf['labels'][latType])

            print(title)
            print(conf['labels'][latType])
            pp.pprint(res_bundle)

            plotTimeSeries(res_bundle, title, xlabel, ylabel, show_legend=True, fp=fgp, yMax=10000, xMax=xMax, logy=logY, yMin=0, xMin=-0.001,
                           colors=colors, markers=markers, lspec=lines)

            if save_fig:
                plt.savefig("%s/%s-%s.pdf" % (conf["fig_dir"], figPrefix,latType))
                plt.savefig("%s/%s-%s.eps" % (conf["fig_dir"], figPrefix,latType))
