#!/usr/bin/python
import matplotlib as plt
from thr_lat import thr_lat_iat, lat_cdf, rs_bars_datathr, rs_bars_rps, lat_rs_cdf
from results_parser import load_results
from plotutils import save_figures, fp_default
from write_ratio import write_ratio_results, write_ratio_thr_lat
from batch_size import batch_size_lat_thr, batch_size_bars, batch_size_results_lines, batch_size_bars_lat
from hist_reads import hist_read_results, hist_read_results_hq16
from stream_scale import stream_scale_results_bars, stream_scale_lines
from node_scale import node_scale_bars, node_scale_line
from burst_handling import burst_handling_results

# plt.use("Agg")


# import warnings
# warnings.filterwarnings("error")

def updateFigParams(fig_params):
    params = {}
    params['figsize'] = (3.5, 2.2)
    params['dimensions'] = (0.21, 0.975, 0.85, 0.22)
    params['legend_position'] = (0.5, 1.2)
    params['legend_size'] = 9
    params['legend_label_spacing'] = 0.3
    params['legend_handle_length'] = 2
    params['legend_column_spacing'] = 1
    params['legend_label_spacing'] = 0.3
    params['legend_cols'] = 3
    params['title_x'] = 0
    params['title_y'] = 1
    params['titleFont'] = 9
    params['bars_width'] = 0.25  # 0.1
    params['bars_left_blank'] = 0.25
    params['marker_size'] = 2
    params['xylabels_font_size'] = 9
    params['ticks_font_size'] = 9
    params['legendFrame'] = False
    params['colored'] = True
    params['xlabelpad'] = 6
    params['ylabelpad'] = 8
    params['hatchgen'] = False
    params['line_width'] = 1.2
    params['axis_tick_pad'] = 4
    params['markers'] = True
    params['yAxesBothSides'] = False
    params['bar_edge_color'] = 'black'

    fig_params.update(params)
    return fig_params


def configParams(params):
    conf = {}

    conf['exp_params'] = {
        'btrdb': ('btrdb', 'results.json'), #(results dir, results file)
        'kawkab': ('kawkab', 'all-results.json')
    }

    conf['results_dir'] = '/home/sm3rizvi/kawkab/experiments/results'
    conf['fig_dir'] = 'figures'

    conf['numTestsToInclude'] = 0  # 0 means all
    conf['filterThrDown'] = False
    conf['filter'] = False
    conf['cutoffLat'] = 3100  # in milliseconds
    conf['maxLatency'] = 3000000  # in microseconds
    conf['breakAtFirstCutoff'] = False
    conf['avgFuncText'] = "Median"  # Can be Mean or Median
    conf['lastCommon'] = False
    conf['resultsBase'] = "results"  # "results" or "histograms"
    # conf['fromHistograms'] = True
    # conf['histogramsFile'] = "aggResults.txt"

    # btrdb-btr3-nc120-cpm15-bs500-rs16-nf1-wr100-iat3.000

    conf['test_types'] = [  # (test type, prefix, results dir prefix)
        ('btrdb', 'btr18'),
        # ('kawkab','sep13'),
    ]
    conf['write_ratio'] = [100]
    conf['num_clients'] = [200]
    conf['clients_per_machine'] = [10]
    conf['cdfThrIndex'] = -1  # For what throughput we want the CDF of the read and write requests
    conf['batch_size'] = [500]
    conf['record_size'] = [16]
    conf['write_ratio'] = [100]
    conf['files_per_client'] = [1]
    conf['iat'] = [1]

    conf['test_runs'] = [0]

    params.update(conf)
    params['labels'] = getLabels()

    return params


def getLabels():
    return {
        # 'lotk-1crd-180c-20GAgg':'ZKCanopus',
        'btrdb-btr18': 'BTrDB', 'btrdb-btr19': 'BTrDB',
        'rw-kw6': 'Kawkab', 'rw-kw8': 'Kawkab', 'rw-kw11': 'Kawkab', 'rw-kw10': 'Kawkab', 'rw-kw13': 'Kawkab',
        'rw-kw14': 'Kawkab1',
        'rw-sep18': 'Kawkab',  'rw-kw12': 'Kawkab',
        'rw-exprw3':'Kawkab',
        'rw-sep11': 'Kawkab Base','rw-sep14': 'Kawkab Base', 'rw-sep12': 'Kawkab Base', 'rw-sep15': 'Kawkab Base',
        'meanLat': "Average",
        'lat50': 'Median',
        'lat95': '95$\mathregular{^{th}}$ percentile',
        'lat99': '99 percentile',
        'maxLat': 'Max'
    }

def rs_bars(conf, figParams):
    config = {}
    config.update(conf)

    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'record_size', 'points':[
            #------------------- Batch size -----------------------------------
            {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':64, 'num_clients':800, 'iat':[6]},
            {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':96, 'num_clients':800, 'iat':[5]},
            {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':128, 'num_clients':800, 'iat':[3]},
            {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':160, 'num_clients':800, 'iat':[2]},
            {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':192, 'num_clients':800, 'iat':[2]},
        ]},
    ]
    config['write_ratio'] = [100]
    config['num_clients'] = [800]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [100]
    config['record_size'] = [0]
    config['files_per_client'] = [1]
    #config['iat'] = [925, 875, 825, 775, 750]
    config['test_runs'] = [1,2,3]

    fgp = {}
    fgp.update(figParams)
    fgp.update({"legend_cols": 3, 'markers': True})

    #metric = ('write_ratio', config['write_ratio'])

    results = load_results(config)

    #title = "Throughput and latency (Batch Size)"
    title = ""
    fig_prefix = "rs-bars-data"
    rs_bars_datathr(config, results, fgp, fig_prefix, title, None, None, True)
    fig_prefix = "rs-bars-rps"
    rs_bars_rps(config, results, fgp, fig_prefix, title, None, None, True)

def thr_lat_bs(conf, figParams):
    config = {}
    config.update(conf)

    # Points for bar graph
    # Median Lat Points
    # Kawkab: 100:8.5, 500:8, 1000:8, 10000:7, 20000:7, 30000:7
    # BTrDB: 100:1, 500:2.5, 1000:3.75, 10000:6, 20000:7, 25000:7, 30000:7, 40000:7, 50000:

    # Peak throughput
    # Kawkab: 100:11.75, 500:10.75, 1000:10.25, 10000:10.2, 20000:10, 30000:10.75
    # BTrDB: 100:1.3, 500:3.75, 1000:5.5, 10000:8.85, 20000:9.3, 25000:9.5, 30000:9.2, 40000:9,

    config['write_ratio'] = [100]
    config['metric'] = [
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'batch_size', 'points':[
        #     #------------------- Batch size -----------------------------------
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7.5, 8.5,  9.25, 10, 11, 11.75, 12]},#, 12.25, 12.5, 12.75, 13]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':500, 'num_clients':200, 'iat':[8, 9, 9.5, 10, 10.5, 10.75, 10.85, 11, 11.25, ]}, #, 13]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':1000, 'num_clients':200, 'iat':[8, 9, 9.25, 9.5, 9.75, 10, 10.25, 10.5, 10.75]},#, 12, 12.25, 12.5, 12.75, 13]},#, 13.25]}, #, 13]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':10000, 'num_clients':200, 'iat':[7, 9, 9.5, 9.75,10, 10.2, 10.35, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':20000, 'num_clients':200, 'iat':[7,9, 9.5, 9.75, 10, 10.25, 10.5, 11]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':30000, 'num_clients':200, 'iat':[7,9, 9.5, 9.75, 10, 10.25, 10.75, 11]},
        #     #-------------------- Scale nodes ----------------------------------
        #     # Tput: 200:10.2, 400:10.2, 600:10.5
        #     # {'res_file':'all-results.json', 'prefix':'rw-kw41', 'val':10000, 'num_clients':200, 'iat':[7, 9, 9.5, 9.75,10, 10.2, 10.35, ]},
        #     # {'res_file':'all-results.json', 'prefix':'rw-kw43', 'val':10000, 'num_clients':400, 'iat':[7, 9, 9.5, 9.75, 10.2, 10.5]},
        #     # {'res_file':'all-results.json', 'prefix':'rw-kw43', 'val':10000, 'num_clients':600, 'iat':[7, 9, 9.5, 9.75, 10, 10.2, 10.5, 11]},
        #     # {'res_file':'all-results.json', 'prefix':'rw-kw43', 'val':10000, 'num_clients':800, 'iat':[9.5, 9.75, 10, 10.2, 10.5, 11]},
        #     #------------------------ Scale Nodes 1000 --------------------
        # ]},
        # { 'type':'btrdb', 'label':'BTrDB',
        #   'name':'batch_size', 'points':[
        #     #Results for batch size test #<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[0.4, 0.6, 1, 1.2, 1.3, 1.5, 1.6, 1.8]},
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':500, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.2, 4.3, 4.5, 4.75, 5]},
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1000, 'num_clients':200, 'iat':[1,2, 3, 3.5, 3.75, 4, 4.5, 4.75, 5, 5.25, 5.5, 6]},
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':10000, 'num_clients':200, 'iat':[6, 7, 8, 8.65, 8.85, 9, 9.15, 9.3, 9.5]},
        #     {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':20000, 'num_clients':200, 'iat':[7, 7.5, 8, 8.5, 9, 9.15, 9.3, 9.4, 9.5]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':25000, 'num_clients':200, 'iat':[7, 7.5, 8, 8.5, 9, 9.3, 9.4, 9.5, 9.6, 9.7]},#, 10]},#, 10.2, 10.4, 10.6]}
        #     {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':30000, 'num_clients':200, 'iat':[7, 7.5, 8, 8.5, 9, 9.2, 9.3, 9.37, 9.4, 9.46, 9.5]},#, 9.7]},#, 10]},#, 10.2]},#, 10.3, 10.4,]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':40000, 'num_clients':200, 'iat':[7, 7.5, 8, 8.5, 9, 9.3, 9.4, 9.5, 9.7]},#, 10, 10.2, 10.4, 10.6]}
        #     #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':50000, 'num_clients':200, 'iat':[7, 7.5, 8, 8.5, 9, 9.3, 9.5, 9.7]},#, 10, 10.2, 10.4, 10.6]}
        #     #--------------- Scale nodes ----
        #     # Tput: 200:8.85, 400:4
        #     #{'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs1','val':500, 'num_clients':200, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, 4.30, ]},
        #     # {'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs2','val':500, 'num_clients':400, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, ]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs3','val':500, 'num_clients':600, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, 4.30, ]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs4','val':500, 'num_clients':800, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, 4.30, ]},
        #     #--------------------------
        # ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'record_size', 'points':[
        #     #------------------- Batch size -----------------------------------
        #     {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':64, 'num_clients':800, 'iat':[1,2,3,4,5,6]},
        #     {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':96, 'num_clients':800, 'iat':[1,2,3,4,5]},
        #     {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':128, 'num_clients':800, 'iat':[1,2,3]},
        #     {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':160, 'num_clients':800, 'iat':[1,2]},
        #     {'res_file':'all-results.json', 'prefix':'rw-v2t1', 'val':192, 'num_clients':800, 'iat':[1,2]},
        # ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'batch_size', 'points':[
        #     #{'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':100, 'num_clients':800, 'iat':[2, 4, 6, 7, 7.5, 8, 8.5, 9, 9.5, 9.75, 10, 10.5, 11, 11.5, 12]},
        #     #{'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':500, 'num_clients':800, 'iat':[4, 6, 8, 9, 10, 10.2, 10.4, 10.6, 10.8, 11, 12, 13]},
        #     {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':1000, 'num_clients':800, 'iat':[6, 8, 8.25, 8.75, 9, 9.25, 9.5, 9.75, 10, 10.4, 11, 11.5, 12, 12.5, 12.75, 13]},
        #     # {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':10000, 'num_clients':800, 'iat':[6, 8, 9, 10, 11, 12, 13, 13.25, 13.5,]},
        #     # {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':20000, 'num_clients':800, 'iat':[6, 8, 9, 10, 11, 12, 13, 13.25, 13.5]},
        #     # {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':30000, 'num_clients':800, 'iat':[6, 8, 9, 10, 11, 12, 13, ]},
        # ]},
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'batch_size', 'points':[
            #{'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':100, 'num_clients':800, 'iat':[2, 4, 6, 7, 7.5, 8, 8.5, 9, 9.5, 9.75, 10, 10.5, 11, 11.5, 12]},
            #{'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':500, 'num_clients':800, 'iat':[4, 6, 8, 9, 10, 10.2, 10.4, 10.6, 10.8, 11, 11.5, 12, 12.25, 12.5, 12.75, 13]},
            #{'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':1000, 'num_clients':800, 'iat':[6, 8, 9, 10, 10.4, 11, 11.5]},
            #{'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':10000, 'num_clients':800, 'iat':[6, 8, 9, 10, 11, 12, 12.5, 13, 13.25, 13.5,]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':20000, 'num_clients':800, 'iat':[6, 8, 9, 10, 11, 12, 13, 13.25, 13.3, 13.4,]},
            # {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'val':30000, 'num_clients':800, 'iat':[6, 8, 9, 10, 11, 12, 13, ]},
        ]},

    ]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [100]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    #config['iat'] = [925, 875, 825, 775, 750]
    config['test_runs'] = [1,2,3,4,5]

    fgp = {}
    fgp.update(figParams)
    fgp.update({"legend_cols": 3, 'markers': True})

    #metric = ('write_ratio', config['write_ratio'])

    results = load_results(config)

    #title = "Throughput and latency (Batch Size)"
    title = ""
    fig_prefix = "temp-bs"

    batch_size_lat_thr(config, results, fgp, fig_prefix, title, None, 3000, True, True, True)

def results_bs_lines(conf, figParams):
    config = {}
    config.update(conf)

    #BTrDB: 1000:5.5, 10000:8.85, 20000:9.4, 25000:9.5, 30000:9.3, 40000:9.3
    #Kawkab: 1000:10.75, 10000:10, 20000:10, 25000:, 30000:9.75
    fgp = {}
    config['metric'] = [
        # { 'type':'btrdb', 'label':'BTrDB',
        #   'name':'batch_size', 'points':[
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[0.4, 0.6, 1, 1.2, 1.3, 1.5,]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs1','val':500, 'num_clients':200, 'iat':[2.5, 3, 3.75, 4, 4.15, 4.3]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1000, 'num_clients':200, 'iat':[1,2, 4.5, 5, 5.5, 6]},
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':10000, 'num_clients':200, 'iat':[6, 7, 8, 8.65, 8.85, 9]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':20000, 'num_clients':200, 'iat':[7, 8.5, 9, 9.15, 9.3, 9.4]},
        #     {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':25000, 'num_clients':200, 'iat':[7, 7.5, 8.5, 9, 9.4, 9.5, 9.6, ]},
        #     {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':30000, 'num_clients':200, 'iat':[7, 8, 8.5, 9, 9.2, 9.3, 9.37]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':40000, 'num_clients':200, 'iat':[7, 8, 9, 9.3, 9.4, 9.5, 9.7]},
        #     #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':50000, 'num_clients':200, 'iat':[7, 8, 9, 9.5, 9.7]},
        # ]},
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'batch_size', 'points':[
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7.5, 8.5,  9.25, 10, 11, 11.75, 12]},#, 12.25, 12.5, 12.75, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':500, 'num_clients':200, 'iat':[8, 9, 10, 10.75, 10.85,]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':1000, 'num_clients':200, 'iat':[8, 9, 10, 10.75]},#, 11, 11.75]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':10000, 'num_clients':200, 'iat':[7,9, 9.5, 10, 10.35]},#, 10.5]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':20000, 'num_clients':200, 'iat':[7,9, 9.5, 10, 10.25, 11]},#, 11.75, 12], 12.25, 12.5,12.75]},
            # {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':30000, 'num_clients':200, 'iat':[7,9, 9.5, 9.75, 10, 10.25]},#, 10.75, 11]},
        ]},
    ]
    fgp.update(figParams)
    fgp.update({
        'figsize':      (6, 2.5),
        'dimensions':   (0.125, 0.975, 0.85, 0.18),
        'legend_cols':  4,
        'legend_position':  (0.5, 1.29),
        'markers': False,
    })
    fig_prefix = "03-batch-lines"
    #-----------------------------------

    # ------------------------
    # config['metric'] = [
    #     { 'type':'btrdb', 'label':'BTrDB',
    #       'name':'batch_size', 'points':[
    #         {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[0.4, 0.6, 1, 1.2, 1.3, 1.5,]},
    #     ]},
    #     { 'type':'kawkab', 'label':'Kawkab',
    #       'name':'batch_size', 'points':[
    #         {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7.5, 8.5,  10, 11, 11.75, 12]},
    #     ]},
    # ]
    #
    # fgp = {}
    # fgp.update(figParams)
    # fgp.update({
    #     'figsize':      (3.5, 2.2),
    #     'dimensions':   (0.21, 0.975, 0.85, 0.22),
    #     'legend_cols':  3,
    #     'legend_position':  (0.5, 1.2),
    #     'markers': False,
    # })
    # fig_prefix = "01-batch-lines"
    #---------------------------

    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [0]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1,2,3,4,5]

    results = load_results(config)

    title = ""

    batch_size_results_lines(config, results, fgp, fig_prefix, title, None, 1000, True, True, True)

def results_bs_bars(conf, figParams):
    config = {}
    config.update(conf)
    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [0]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1, 2, 3, 4, 5]

    # Points for bar graph
    # Peak throughput
    # Kawkab: 100:11.75, 500:10.75, 1000:10.25, 10000:10.2, 20000:10, 30000:10.75
    # BTrDB: 100:1.3, 500:3.75, 1000:5.5, 10000:8.85, 20000:9.3, 25000:9.5, 30000:9.2, 40000:9,

    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'batch_size', 'points':[
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[9.25]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':500, 'num_clients':200, 'iat':[9]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':1000, 'num_clients':200, 'iat':[9.5]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':10000, 'num_clients':200, 'iat':[9.5]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':20000, 'num_clients':200, 'iat':[9.5]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':30000, 'num_clients':200, 'iat':[9.5]},
        ]},
        { 'type':'btrdb', 'label':'BTrDB',
          'name':'batch_size', 'points':[
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[1.3]},
            {'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs1','val':500, 'num_clients':200, 'iat':[3.75]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1000, 'num_clients':200, 'iat':[5.5]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':10000, 'num_clients':200, 'iat':[8.65]},
            {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':20000, 'num_clients':200, 'iat':[9.15]},
            #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':25000, 'num_clients':200, 'iat':[9.5]},
            {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':30000, 'num_clients':200, 'iat':[9]},
            #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':40000, 'num_clients':200, 'iat':[9]},
            #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':50000, 'num_clients':200, 'iat':[9]},
        ]},
    ]

    fgp = {}
    fgp.update(figParams)
    fgp.update({
        'figsize':      (3.5, 2.2),
        'dimensions':   (0.19, 0.975, 0.85, 0.2),
        "legend_cols": 3,
        'legend_position':  (0.5, 1.25),
                })

    results = load_results(config)

    title = ""#"Throughput with different batch sizes"
    fig_prefix = "02-batch-bars-tput"

    batch_size_bars(config, results, fgp, fig_prefix, title, None, None, True)

def plot_cdf(conf, figParams):
    config = {}
    config.update(conf)
    config['write_ratio'] = [0]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [10000]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1,2,3,4,5]

    config['metric'] = [
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'batch_size', 'points':[
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'from_hist':True, 'cdf':True, 'val':100, 'num_clients':200, 'iat':[6, 7, 9.25]},#, 10, 11, 11.75, 12]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'from_hist':True, 'cdf':True,  'val':500, 'num_clients':200, 'iat':[8, 9, 9.5]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'from_hist':True, 'cdf':True,  'val':1000, 'num_clients':200, 'iat':[8, 9, 9.5]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'from_hist':True, 'cdf':True,  'val':10000, 'num_clients':200, 'iat':[7, 9, 9.5]},
        # ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'write_ratio', 'points':[
        #     {'res_file':'all-results.json', 'prefix':'rw-kw41', 'from_hist':True, 'cdf':True, 'val':100, 'num_clients':200, 'iat':[7,9, 9.5]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'cdf':True, 'val':80, 'num_clients':200, 'iat':[10, 12, 13]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'cdf':True, 'val':50, 'num_clients':200, 'iat':[17, 19, 19.75]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'cdf':True, 'val':20, 'num_clients':200, 'iat':[36, 38, 40]},
        # ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'record_size', 'points':[
        #     {'res_file':'write-results.json', 'prefix':'rw-v2t1', 'from_hist':True, 'cdf':True, 'val':64, 'num_clients':800, 'iat':[1, 5, 6]},#, 10, 11, 11.75, 12]},
        #     {'res_file':'write-results.json', 'prefix':'rw-v2t1', 'from_hist':True, 'cdf':True,  'val':96, 'num_clients':800, 'iat':[1, 4, 5]},
        #     {'res_file':'write-results.json', 'prefix':'rw-v2t1', 'from_hist':True, 'cdf':True,  'val':128, 'num_clients':800, 'iat':[1, 2, 3]},
        #     {'res_file':'write-results.json', 'prefix':'rw-v2t1', 'from_hist':True, 'cdf':True,  'val':160, 'num_clients':800, 'iat':[1, 2]},
        #     {'res_file':'write-results.json', 'prefix':'rw-v2t1', 'from_hist':True, 'cdf':True,  'val':192, 'num_clients':800, 'iat':[1, 2]},
        # ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'record_size', 'points':[
        #     # {'res_file':'read-results.json',  'prefix':'rw-hq16r', 'write_ratio':0, 'batch_size':10000, 'from_hist':True, 'cdf':True, 'val':16, 'num_clients':80, 'iat':[1]},#, 10, 11, 11.75, 12]},
        #     # {'res_file':'write-results.json', 'prefix':'rw-hq16w', 'write_ratio':80, 'batch_size':1000, 'from_hist':True, 'cdf':True, 'val':16, 'num_clients':80, 'iat':[10]},#, 10, 11, 11.75, 12]},
        #     # {'res_file':'read-results.json',  'prefix':'rw-hq16w', 'write_ratio':80, 'batch_size':1000, 'from_hist':True, 'cdf':True, 'val':16, 'num_clients':80, 'iat':[10]},#, 10, 11, 11.75, 12]},
        #     {'res_file':'read-results.json',  'prefix':'rw-hq22r', 'write_ratio':0, 'batch_size':1000000, 'from_hist':True, 'cdf':True, 'val':16, 'num_clients':5, 'iat':[0.1]},#, 10, 11, 11.75, 12]},
        #     {'res_file':'write-results.json', 'prefix':'rw-hq22w', 'write_ratio':80, 'batch_size':1000, 'from_hist':True, 'cdf':True, 'val':16, 'num_clients':200, 'iat':[10]},#, 10, 11, 11.75, 12]},
        #     {'res_file':'read-results.json',  'prefix':'rw-hq22w', 'write_ratio':80, 'batch_size':1000, 'from_hist':True, 'cdf':True, 'val':16, 'num_clients':200, 'iat':[10]},#, 10, 11, 11.75, 12]},
        # ]},
        # { 'type':'kawkab', 'label':'writes',
        #   'name':'write_ratio', 'points':[
        #     {'res_file':'all-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[2, 7,9, 9.5, 9.75,10, 10.2, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':80, 'num_clients':200, 'iat':[2, 10, 11, 12, 13, 13.5]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':50, 'num_clients':200, 'iat':[10, 17, 18, 19, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':20, 'num_clients':200, 'iat':[30, 36, 38, 40, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        # ]},
        { 'type':'kawkab', 'label':'writes',
          'name':'write_ratio', 'points':[
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'cdf':True, 'batch_size':1000, 'val':100, 'num_clients':800, 'iat':[8.25]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'cdf':True, 'batch_size':1000, 'val':80, 'num_clients':800, 'iat':[12]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'cdf':True, 'batch_size':1000, 'val':50, 'num_clients':800, 'iat':[17]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'cdf':True, 'batch_size':1000, 'val':20, 'num_clients':800, 'iat':[32]},
        ]},
    ]

    fgp = {}
    fgp.update(figParams)
    fgp.update({
        'figsize':      (3.5, 2.2),
        'dimensions':   (0.19, 0.975, 0.85, 0.2),
        "legend_cols": 3,
        'legend_position':  (0.5, 1.25),
    })

    results = load_results(config)

    fig_prefix = "cdf"

    lat_cdf(config, results, fgp, fig_prefix, True)

def results_bs_lat50_bars(conf, figParams):
    config = {}
    config.update(conf)
    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [0]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1, 2, 3, 4, 5]

    # Median Lat Points
    # Kawkab: 100:8.5, 500:8, 1000:8, 10000:7, 20000:7, 30000:7
    # BTrDB: 100:1, 500:2.5, 1000:3.75, 10000:6, 20000:7, 25000:7, 30000:7, 40000:7, 50000:
    fgp = {}
    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'batch_size', 'points':[
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7.5]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':500, 'num_clients':200, 'iat':[8]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':1000, 'num_clients':200, 'iat':[8]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':10000, 'num_clients':200, 'iat':[7]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':20000, 'num_clients':200, 'iat':[7]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':30000, 'num_clients':200, 'iat':[7]},
        ]},
        { 'type':'btrdb', 'label':'BTrDB',
          'name':'batch_size', 'points':[
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[0.6]},
            {'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs1','val':500, 'num_clients':200, 'iat':[2.5]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1000, 'num_clients':200, 'iat':[3.75]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':10000, 'num_clients':200, 'iat':[6]},
            {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':20000, 'num_clients':200, 'iat':[7]},
            #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':25000, 'num_clients':200, 'iat':[7]},
            {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':30000, 'num_clients':200, 'iat':[7]},
            #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':40000, 'num_clients':200, 'iat':[7]},
            #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':50000, 'num_clients':200, 'iat':[9]},
        ]},
    ]

    fgp = {}
    fgp.update(figParams)
    fgp.update({
        'figsize':      (3.5, 2.2),
        'dimensions':   (0.19, 0.975, 0.85, 0.2),
        "legend_cols": 3,
        'legend_position':  (0.5, 1.25),
    })

    results = load_results(config)

    title = ""#"Throughput with different batch sizes"
    fig_prefix = "02-batch-bars-lat50"

    batch_size_bars_lat(config, results, fgp, fig_prefix, title, None, None, True)

def scale_nodes_bars(conf, figParams):
    config = {}
    config.update(conf)
    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [10000]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1, 2, 3, 4, 5]

    # Points for bar graph
    # Peak throughput
    # Kawkab: 100:11.75, 500:10.75, 1000:10.25, 10000:10.2, 20000:10, 30000:10.75
    # BTrDB: 100:1.3, 500:3.75, 1000:5.5, 10000:8.85, 20000:9.3, 25000:9.5, 30000:9.2, 40000:9,

    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'num_servers', 'points':[
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':1, 'num_clients':200, 'iat':[11.75]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':2, 'num_clients':400, 'iat':[10.75]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':3, 'num_clients':600, 'iat':[10.25]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':4, 'num_clients':800, 'iat':[10.75]},
        ]},
        { 'type':'btrdb', 'label':'BTrDB',
          'name':'batch_size', 'points':[
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1, 'num_clients':200, 'iat':[1.3]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':2, 'num_clients':400, 'iat':[1.3]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':3, 'num_clients':600, 'iat':[1.3]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':4, 'num_clients':800, 'iat':[1.3]},
        ]},
    ]

    fgp = {}
    fgp.update(figParams)
    fgp.update({
        'figsize':      (3.5, 2.2),
        'dimensions':   (0.19, 0.975, 0.85, 0.2),
        "legend_cols": 3,
        'legend_position':  (0.5, 1.25),
    })

    results = load_results(config)

    title = ""#"Throughput with different batch sizes"
    fig_prefix = "02-batch-bars-tput"

    batch_size_bars(config, results, fgp, fig_prefix, title, None, None, True)

def thr_lat_write_ratio(conf, figParams):
    config = {}
    config.update(conf)

    config['metric'] = [
        # { 'type':'kawkab', 'label':'writes',
        #   'name':'write_ratio', 'points':[
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw29', 'val':100, 'num_clients':200, 'iat':[8,10,11]},
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw30', 'val':90, 'num_clients':200, 'iat':[5, 9, 10.5, 11.25, 11.75, 12]},
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw30', 'val':50, 'num_clients':200, 'iat':[13, 14, 15, 16, 17, 18, 19, 20]},
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw30', 'val':10, 'num_clients':200, 'iat':[22, 24, 26, 28, 28.5, 33, 33.5, 34]},
        #     #----------------------
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7,9, 9.5, 9.75,10, 10.2, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[17, 18, 19, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[36, 38, 40, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        #     #-----------------------------
        # ]},
        # { 'type':'kawkab', 'label':'reads',
        #   'name':'write_ratio', 'points':[
        #     #{'res_file':'read-results.json', 'prefix':'rw-kw30', 'val':90, 'num_clients':200, 'iat':[5, 9, 10.5, 11.25, 11.75, 12]},
        #     #{'res_file':'read-results.json', 'prefix':'rw-kw30', 'val':50, 'num_clients':200, 'iat':[13, 14, 15, 16, 17, 18, 19, 20]},
        #     #{'res_file':'read-results.json', 'prefix':'rw-kw30', 'val':10, 'num_clients':200, 'iat':[22, 24, 26, 28, 28.5, 33, 33.5, 34]},
        #     #-------------------------
        #     {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5, 13.75]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
        #     {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[17, 18, 19, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
        #     {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[36, 38, 40, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        # ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'write_ratio', 'points':[
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw29', 'val':100, 'num_clients':200, 'iat':[8,10,11]},
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw30', 'val':90, 'num_clients':200, 'iat':[5, 9, 10.5, 11.25, 11.75, 12]},
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw30', 'val':50, 'num_clients':200, 'iat':[13, 14, 15, 16, 17, 18, 19, 20]},
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw30', 'val':10, 'num_clients':200, 'iat':[22, 24, 26, 28, 28.5, 33, 33.5, 34]},
        #     #-----------------------
        #     {'res_file':'all-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7, 9, 9.5, 9.75, 10, 10.2, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5, 13.75]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[10, 13, 14, 15, 17, 18, 18.5, 19, 19.5, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[30, 33, 36, 38, 40, 41, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        #     #------------
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'from_hist':True, 'val':100, 'num_clients':200, 'iat':[8, 9, 10, 10.75]},#, 11, 11.75]},
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw47', 'from_hist':True, 'val':80, 'num_clients':400, 'iat':[5, 8, 10, 13]},
        # ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'write_ratio', 'points':[
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[8, 9, 9.5, 10, 10.25, 10.5, 10.75]},
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw46', 'val':80, 'num_clients':200, 'iat':[8, 10, 11,]},
        #     #{'res_file':'write-results.json', 'prefix':'rw-kw46', 'val':50, 'num_clients':200, 'iat':[10, 14, 16, 18.5, 18.75, 19, 19.25, 19.5, 20]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw46', 'val':20, 'num_clients':200, 'iat':[20, 30, 33, 36, 38, 39, 40, 41, 41.25, 41.5, 42, 42.5]},
        # ]},
        # { 'type':'btrdb', 'label':'BTrDB',
        #   'name':'write_ratio', 'points':[
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[6, 7, 8, 8.65, 8.85, 9]},
        #     {'res_file':'results.json', 'prefix':'btrdb-btr30-clp20-svrs1', 'val':80, 'num_clients':200, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.25, 4.5]},
        #     {'res_file':'results.json', 'prefix':'btrdb-btr30-clp20-svrs1', 'val':50, 'num_clients':200, 'iat':[1, 2, 2.5, 2.75, ]},
        #     #{'res_file':'results.json', 'prefix':'btrdb-btr30-clp20-svrs1', 'val':20, 'num_clients':200, 'iat':[1, 2, 2.5, 2.75, ]},
        # ]},
        # { 'type':'kawkab', 'label':'writes',
        #   'name':'write_ratio', 'points':[
        #     {'res_file':'all-results.json', 'prefix':'rw-kw41', 'batch_size':1000, 'val':100, 'num_clients':200, 'iat':[8, 9, 9.25, 9.5, 9.75, 10, 10.25, 10.5, 10.75]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw47', 'batch_size':1000, 'val':80, 'num_clients':400, 'iat':[5, 8, 10, 13,]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw47', 'batch_size':1000, 'val':50, 'num_clients':400, 'iat':[5, 15, 16, 17]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw47', 'batch_size':1000, 'val':20, 'num_clients':400, 'iat':[25, 30, 32, ]},
        # ]},
        { 'type':'kawkab', 'label':'writes',
          'name':'write_ratio', 'points':[
            #{'res_file':'all-results.json', 'prefix':'rw-kw41', 'batch_size':1000, 'val':100, 'num_clients':200, 'iat':[8, 9, 9.25, 9.5, 9.75, 10, 10.25, 10.5, 10.75]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'batch_size':1000, 'val':100, 'num_clients':800, 'iat':[6, 8, 8.25, 8.75, 9, 9.25, 9.5, 9.75, 10, 10.4, 11, 11.5, 12, 12.5, 13]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'batch_size':1000, 'val':80, 'num_clients':800, 'iat':[8, 12, 13, 14, 15, 15.5, 16,]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'batch_size':1000, 'val':50, 'num_clients':800, 'iat':[8, 15, 17, 19, 21, 22, 22.5, 23, 23.5, 24, 24.25,]},
            {'res_file':'all-results.json', 'prefix':'rw-16kbs2', 'from_hist':True, 'batch_size':1000, 'val':20, 'num_clients':800, 'iat':[8, 16, 24, 32, 34, 35, 37, 38, 39, 40, 42, 44]},
        ]},
        # { 'type':'kawkab', 'label':'writes',
        #   'name':'write_ratio', 'points':[
        #     {'res_file':'all-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[2, 7,9, 9.5, 9.75,10, 10.2, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':80, 'num_clients':200, 'iat':[2, 10, 11, 12, 13, 13.5]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':50, 'num_clients':200, 'iat':[10, 17, 18, 19, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':20, 'num_clients':200, 'iat':[30, 36, 38, 40, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        # ]},
    ]
    config['write_ratio'] = [0]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [10000]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1,2,3,4,5]

    results = load_results(config)

    title = ""
    fig_prefix = "write-ratio"

    latTypes = ["meanLat", "lat50", "lat95", "lat99", "maxLat"]

    write_ratio_thr_lat(config, results, latTypes, figParams, fig_prefix, title, None, None, True, True, True)

def results_write_ratio(conf, figParams):
    config = {}
    config.update(conf)

    fig_prefix = "04-write-ratio-all"
    config['metric'] = [
        # { 'type':'kawkab', 'label':'writes',
        #   'name':'write_ratio', 'points':[
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7,9, 9.5, 9.75,10, 10.2, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
        #     {'res_file':'all-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[17, 18, 19, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
        #     #{'res_file':'all-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[36, 38, 40, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        # ]},
        { 'type':'kawkab', 'label':'writes',
          'name':'write_ratio', 'points':[
            {'res_file':'all-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7,9, 9.5, 9.75,10, 10.2, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
            {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
            {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':50, 'num_clients':200, 'iat':[17, 18, 19, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
            {'res_file':'all-results.json', 'prefix':'rw-kw42', 'from_hist':True, 'val':20, 'num_clients':200, 'iat':[36, 38, 40, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        ]},

        # { 'type':'kawkab', 'label':'reads',
        #   'name':'write_ratio', 'points':[
        #     {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5, 13.75]},#, 13.75, 14, 14.25, 14.5, 14.75, 15, 15.25]},
        #     {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[17, 18, 19, 19.75, 20]},#, 20.25, 20.5, 20.75, 21, 21.25, 21.5]},
        #     {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[36, 38, 40, 42]},#, 42.5, 43, 43.25, 43.5]},#, 43.75, 44, 44.25]},
        # ]},
        # { 'type':'kawkab', 'label':'writes',
        #   'name':'write_ratio', 'points':[
        #     {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7, 9, 9.75, 10.2, 10.5, 11.25]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[10, 13, 15, 17, 18, 18.5, 19.5, 20]},
        #     {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[30, 33, 36, 40, 41]},
        # ]},
    ]

    #--------------------------------------------

    # fig_prefix = "04-write-ratio-reads"
    # config['metric'] = [
    #     { 'type':'kawkab', 'label':'reads',
    #       'name':'write_ratio', 'points':[
    #         #{'res_file':'read-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7, 9, 9.75, 10.2, 10.5, 11.25]},
    #         {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 11, 12, 13, 13.5]},
    #         {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[10,  15, 17, 18, 19.5, 20]},
    #         {'res_file':'read-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[30, 33, 36, 40, 41]},
    #     ]},
    # ]

    #--------------------------------------------

    # fig_prefix = "04-write-ratio-writes"
    # config['metric'] = [
    #     { 'type':'kawkab', 'label':'writes',
    #       'name':'write_ratio', 'points':[
    #         {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':20, 'num_clients':200, 'iat':[30, 33, 36, 40, 41]},
    #         {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':50, 'num_clients':200, 'iat':[10, 13, 17, 18.5, 19.5, 20]},
    #         {'res_file':'write-results.json', 'prefix':'rw-kw42', 'val':80, 'num_clients':200, 'iat':[10, 12, 13, 13.5]},
    #         {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7, 9, 10.2, 10.5, 11.25]},
    #     ]},
    # ]

    config['write_ratio'] = [0]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [10000]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1,2,3,4,5]

    latTypes = ["meanLat", "lat50", "lat95", "lat99", "maxLat"]

    results = load_results(config)

    title = ""

    write_ratio_results(config, results, latTypes, figParams, fig_prefix, title, None, None, True, True, True)

def thr_lat_stream_scale(conf, figParams):
    config = {}
    config.update(conf)


    config['metric'] = [
        # { 'type':'btrdb', 'label':'BTrDB',
        #   'name':'files_per_client', 'points':[
        #     {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1, 'num_clients':200, 'iat':[2, 2.5, 3.25, 3.75, 4, 4.2, 4.5,]},
        #     {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':2, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.9, 4, 4.1, 4.2]},
        #     {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':4, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.9, 4, 4.1]},#, 4.2, 4.3, 4.5]},
        #     {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':12, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.9, 4, 4.1, 4.2]},#, 4.3, 4.5]},
        #     {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':24, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.9]},#, 4, 4.1, 4.2, 4.3, 4.5]},
        #     {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':28, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.9, 4]},#, 4.1, 4.2, 4.3, 4.5]},
        #     {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':36, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.9]},#, 4, 4.1, 4.2, 4.3, 4.5]},
        # ]},
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'files_per_client', 'points':[
            # {'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':1, 'num_clients':200, 'iat':[8, 10, 11, 11.5, 12, 12.5, 13]},
            {'res_file':'write-results.json', 'prefix':'rw-kw29', 'val':1, 'num_clients':200, 'iat':[8, 10, 11]},#, 11.5, 11.75, 12]},#, 12.25, 12.5, 12.75, 13]},
            {'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':2, 'num_clients':200, 'iat':[7, 9, 10, 10.5, 11, 11.5, 12, 12.5]}, #13, 14]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':4, 'num_clients':200, 'iat':[7, 9, 10, 10.5, 11, 11.5, 12, 12.5, 13, 14]},
            {'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':12, 'num_clients':200, 'iat':[7, 9, 10, 10.5, 11, 11.5, 12, 12.5]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':24, 'num_clients':200, 'iat':[7, 9, 10, 10.5, 11, 11.5, 12, 12.5]},#, 12.5, 13, 14]},
             #{'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':28, 'num_clients':200, 'iat':[7, 9, 10, 10.5, 11, 11.5, 12, 12.5, 13, 14]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':36, 'num_clients':200, 'iat':[7, 9, 10, 10.5, 11, 11.5, 12, 12.5, 13, 14]},
            # --------------------------------
        ]},

    ]
    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [500]
    config['record_size'] = [16]
    config['files_per_client'] = [0]#[1,2, 4, 12, 24, 36, 28]
    config['test_runs'] = [2,3,4,5]

    fgp = {}
    fgp.update(figParams)
    fgp.update({"legend_cols": 3, 'markers': True})

    #metric = ('write_ratio', config['write_ratio'])

    results = load_results(config)

    #title = "Throughput and latency (Batch Size)"
    title = ""
    fig_prefix = "temp-stream-scale"

    stream_scale_lines(config, results, fgp, fig_prefix, title, None, None, True, True, True)

def stream_scale_results(conf, figParams):
    config = {}
    config.update(conf)


    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'files_per_client', 'points':[
            #{'res_file':'all-results.json', 'prefix':'rw-kw22', 'val':1, 'num_clients':200, 'iat':[10]},
            {'res_file':'write-results.json', 'prefix':'rw-kw29', 'val':1, 'num_clients':200, 'iat':[10]},
            {'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':2, 'num_clients':200, 'iat':[11.5]},
            {'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':4, 'num_clients':200, 'iat':[13]},
            {'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':12, 'num_clients':200, 'iat':[12]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':24, 'num_clients':200, 'iat':[11, 11.5]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':28, 'num_clients':200, 'iat':[11]},
            {'res_file':'write-results.json', 'prefix':'rw-kw28', 'val':36, 'num_clients':200, 'iat':[ 10.5,]},
            # --------------------------------
        ]},
        { 'type':'btrdb', 'label':'BTrDB',
          'name':'files_per_client', 'points':[
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1, 'num_clients':200, 'iat':[3.75]},
            {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':2, 'num_clients':200, 'iat':[4]},
            {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':4, 'num_clients':200, 'iat':[3.9]},
            {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':12, 'num_clients':200, 'iat':[3.9]},
            #{'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':24, 'num_clients':200, 'iat':[3.5, 3.6, 3.65, 3.7]},
            #{'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':28, 'num_clients':200, 'iat':[3.5]},
            {'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':36, 'num_clients':200, 'iat':[3]},
        ]},
    ]
    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [500]
    config['record_size'] = [16]
    config['files_per_client'] = [0]#[1,2, 4, 12, 24, 36, 28]
    config['test_runs'] = [1,2,3,4,5]

    results = load_results(config)

    #title = "Throughput and latency (Batch Size)"
    title = ""
    fig_prefix = "stream-scale"

    stream_scale_results_bars(config, results, figParams, fig_prefix, title, None, None, True, True)

def results_node_scale(conf, figParams):
    config = {}
    config.update(conf)

    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'num_nodes', 'points':[
            {'res_file':'all-results.json', 'prefix':'rw-kw41', 'val':1, 'num_clients':200, 'iat':[10.2]},
            {'res_file':'all-results.json', 'prefix':'rw-kw43', 'val':2, 'num_clients':400, 'iat':[10.2]},
            {'res_file':'all-results.json', 'prefix':'rw-kw43', 'val':3, 'num_clients':600, 'iat':[10.5]},
            {'res_file':'all-results.json', 'prefix':'rw-kw43', 'val':4, 'num_clients':800, 'iat':[10.5]},
        ]},
    ]
    config['write_ratio'] = [100]
    config['num_clients'] = [0]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [10000]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1,2,3,4,5]

    results = load_results(config)

    title = ""
    fig_prefix = "05-node-scale"

    #node_scale_bars(config, results, figParams, fig_prefix, title, None, None, True)
    node_scale_line(config, results, figParams, fig_prefix, title, None, None, False, True, True)

def plot_graphs(conf, figParams):
    # results = load_results(conf)

    # figParams['figsize'] =(3.25, 2)
    # figParams['dimensions'] = (0.2, 0.99, 0.78, 0.18)
    # figParams['legend_position'] = (0.47, 1.45)
    # figParams['bars_width'] = 0.1

    #rs_bars(conf, figParams)
    #plot_cdf(conf, figParams)
    thr_lat_bs(conf, figParams)
    #thr_lat_write_ratio(conf, figParams)
    #thr_lat_iat(conf, figParams)
    #hist_read_results_hq13(conf, figParams) #<<< Historical read results (no realtime reads)
    #thr_lat_stream_scale(conf, figParams)

    # Results
    #--------
    #results_write_ratio(conf, figParams)
    #results_bs_bars(conf, figParams)
    #results_bs_lat50_bars(conf, figParams)
    #results_bs_lines(conf, figParams)
    #stream_scale_results(conf, figParams)
    #hist_read_results_hq16(conf, figParams) #<<< Historical read results
    #hist_read_results(conf, figParams)
    #scale_nodes_bars(conf, figParams)
    #results_node_scale(conf, figParams)
    #burst_handling_results(conf, figParams)

if __name__ == '__main__':
    conf = configParams({})
    figParams = updateFigParams(fp_default)

    plot_graphs(conf, figParams)

    save_figures(conf)
