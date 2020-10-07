#!/usr/bin/python
import matplotlib as plt
from thr_lat import thr_lat_iat
from results_parser import load_results
from plotutils import save_figures, fp_default
from write_ratio import write_ratio_results
from batch_size import batch_size_lat_thr, batch_size_bars, batch_size_results_lines
from hist_reads import hist_read_results, hist_read_results_hq16
from stream_scale import stream_scale_results_bars, stream_scale_lines

# plt.use("Agg")


# import warnings
# warnings.filterwarnings("error")

def updateFigParams(fig_params):
    params = {}
    params['figsize'] = (3.5, 2.5)
    params['dimensions'] = (0.17, 0.975, 0.85, 0.19)
    params['legend_position'] = (0.4, 1.26)
    params['legend_size'] = 9
    params['legend_label_spacing'] = 0.3
    params['legend_handle_length'] = 2
    params['legend_column_spacing'] = 1
    params['legend_label_spacing'] = 0.3
    params['legend_cols'] = 4
    params['title_x'] = 0
    params['title_y'] = 1
    params['titleFont'] = 9
    params['bars_width'] = 0.25  # 0.1
    params['bars_left_blank'] = 0.25
    params['marker_size'] = 3
    params['xylabels_font_size'] = 9
    params['ticks_font_size'] = 9
    params['legendFrame'] = False
    params['colored'] = True
    params['xlabelpad'] = 8
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

def thr_lat(conf, figParams):
    config = {}
    config.update(conf)
    config['test_types'] = [  # (test type, prefix, results dir prefix)
        ('btrdb', 'btrdb-btr18', 'btrdb'),
        #('kawkab','rw-kw6', 'kawkab'),
    ]
    config['metric'] = [
        { 'type':'btrdb','res_file':'results.json', 'prefix':'btrdb-btr18',
          'name':'batch_size', 'points':[
            {'val':10000, 'iat':[6, 7, 8, 9, 10]}]
          },
    ]
    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [0]
    config['record_size'] = [16]
    config['write_ratio'] = [100]
    config['files_per_client'] = [1]
    #config['iat'] = [925, 875, 825, 775, 750]
    config['test_runs'] = [1, 2, 3, 4, 5]

    config['exp_params']['kawkab'] = ('kawkab', 'write-results.json')

    fgp = {}
    fgp.update(figParams)
    fgp.update({"legend_cols": 4, 'markers': True})

    #metric = ('write_ratio', config['write_ratio'])

    results = load_results(config)

    title = "Throughput and latency (Batch Size)"
    fig_prefix = "batch-size"

    thr_lat_iat(config, results, fgp, fig_prefix, title, False, None, None, True)


def thr_lat_bs(conf, figParams):
    config = {}
    config.update(conf)

    # Points for bar graph
    #BTrDB: (100, 1.3), (500, 4.2), (1000, 5.6), (10000, 7)
    #Kawkab base: (100, 6.9), (500, 1000), (1000, ), (10000, )
    #Kawkab: (100, 6.7), (500, 10.1), (1000, 11), (10000, 11)
    #Kawkab 14: (500, 9.5), (1000, 10), (10000, 11)

    config['write_ratio'] = [100]
    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'batch_size', 'points':[
            # {'res_file':'all-results.json', 'prefix':'rw-kw11', 'val':100, 'num_clients':200, 'iat':[3, 5, 6, 6.2, 6.4, 6.6, 6.7, 6.8, 6.9, 7, 7.2]}, #<<<<<<<<<<<<

            # {'res_file':'write-results.json', 'prefix':'rw-kw18', 'val':500, 'num_clients':560,  'iat':[8, 10, 11, 11.5, 12, 12.5, 13, 13.4,13.7, 13.85,14, 14.5, 15,]},
            # {'res_file':'write-results.json', 'prefix':'rw-kw19', 'val':1000, 'num_clients':560, 'iat':[8, 11, 12, 12.75, 13, 13.4, 13.7, 14, 14.25, 14.5, 15, 16]},
            # {'res_file':'write-results.json', 'prefix':'rw-kw20', 'val':10000, 'num_clients':560, 'iat':[8, 10, 13, 14.5, 15, 15.25, 15.5, 15.75, 16, 16.5]},
            # -------------------------------------------------------------
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':500, 'num_clients':200, 'iat':[8, 10, 11, 11.5, 12, 12.5, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':1000, 'num_clients':200, 'iat':[8, 11, 12, 12.75, 13]},

            # --------------------------------
            #{'res_file':'all-results.json', 'prefix':'rw-kw11', 'val':100, 'num_clients':200, 'iat':[3, 5, 6, 6.2, 6.4, 6.6, 6.7, 6.8, 6.9, 7, 7.2]}
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':500, 'num_clients':200, 'iat':[8, 10, 11, 11.5, 12, 12.5, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':1000, 'num_clients':200, 'iat':[8, 9, 10, 11, 12, 12.75, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw14', 'val':10000, 'num_clients':200, 'iat':[7, 9, 9.5, 10, 10.5, 11, 11.5, 12, 12.5]},
            # {'res_file':'write-results.json', 'prefix':'rw-kw26', 'val':1000, 'num_clients':200, 'iat':[10, 10.5, 11, 12]},
            # --------------------------------
            #{'res_file':'all-results.json', 'prefix':'rw-kw11', 'val':100, 'num_clients':200, 'iat':[3, 5, 6, 6.2, 6.4, 6.6, 6.7, 6.8, 6.9, 7, 7.2]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':500, 'num_clients':200, 'iat':[8, 10, 11, 11.5, 12, 12.5, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':1000, 'num_clients':200, 'iat':[8, 9, 10, 11, 12, 12.75, 13]},
            # {'res_file':'write-results.json', 'prefix':'rw-kw14', 'val':10000, 'num_clients':200, 'iat':[7, 9, 9.5, 10, 10.5, 11, 11.5, 12, 12.5]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':10000, 'num_clients':200, 'iat':[11, 12, 13.5, 14]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw29', 'val':500, 'num_clients':200, 'iat':[8, 10, 11]}, #, 11.5, 11.75, 12]}, #12.25, 12.5, 12.75, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw32', 'val':500, 'num_clients':200, 'iat':[8, 9, 10.25, 10.5, 10.75, 11]}, #, 11.25, 11.5, 11.75]},
            #---------------------
            #{'res_file':'write-results.json', 'prefix':'rw-kw14', 'val':500, 'num_clients':200, 'iat':[6,7,8,9,9.5,9.85, 10,10.15,10.25,10.3,10.5,10.75,11,11.25]},#XXXX
            #{'res_file':'write-results.json', 'prefix':'rw-kw10', 'val':500, 'num_clients':200, 'iat':[9,9.5,10,10.1,10.25,10.3,10.5,11]},#,11.5]},#XXXX
            #{'res_file':'write-results.json', 'prefix':'rw-kw22', 'val':500, 'num_clients':200, 'iat':[8,10,11,11.5,12,12.5,13]},#,13.4]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw29', 'val':500, 'num_clients':200, 'iat':[8,10,11,]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw32', 'val':500, 'num_clients':200, 'iat':[8,9,10.25,10.5,10.75]},
            #---------------------------------------------------------------
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':100, 'num_clients':200, 'iat':[7.5, 8.5,  9.25, 10, 11, 11.75, 12]},#, 12.25, 12.5, 12.75, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':500, 'num_clients':200, 'iat':[8, 9, 9.5, 10, 10.5, 10.75, 10.85, 11, 11.25, ]}, #, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':1000, 'num_clients':200, 'iat':[8, 9, 9.5, 10, 10.25, 10.5, 10.75]},#, 12, 12.25, 12.5, 12.75, 13]},#, 13.25]}, #, 13]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':10000, 'num_clients':200, 'iat':[7,9, 9.5, 9.75,10, 10.2, 10.5, 11, 11.25]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':20000, 'num_clients':200, 'iat':[7,9, 9.5, 9.75, 10, 10.25, 10.5, 11]},#, 11.5, 11.75, 12], 12.25, 12.5,12.75]},
            {'res_file':'write-results.json', 'prefix':'rw-kw41', 'val':30000, 'num_clients':200, 'iat':[7,9, 9.5, 9.75, 10, 10.25, 10.75, 11]},
        ]},
        # { 'type':'kawkab', 'label':'Kawkab',
        #   'name':'batch_size', 'points':[
        #     #{'res_file':'write-results.json','prefix':'rw-sep12', 'val':100, 'num_clients':160, 'iat':[1200, 500, 300, 165, 160, 150, 135, 125]},
        #     #{'res_file':'write-results.json','prefix':'rw-sep11', 'val':500, 'num_clients':160, 'iat':[1400, 1200, 1100, 1000]}, #925, 900, 875]},
        #     #{'res_file':'write-results.json', 'prefix':'rw-sep14', 'val':1000, 'num_clients':160, 'iat':[2100, 2075, 2050]},
        #     {'res_file':'write-results.json','prefix':'rw-sep15', 'val':10000, 'num_clients':160, 'iat':[26000, 25000, 24000, 23000]},
        # ]},
        { 'type':'btrdb', 'label':'BTrDB',
          'name':'batch_size', 'points':[
            #{'res_file':'all-results.json','prefix':'btrdb-btr18','val':100, 'num_clients':200, 'iat':[0.5, 0.8, 1, 1.2, 1.3, 1.5, 1.6, 1.8, 2, 3]},
            #{'res_file':'all-results.json','prefix':'btrdb-btr18','val':500, 'num_clients':200, 'num_clients':200, 'iat':[1, 3, 3.25, 3.5, 3.75, 4, 4.2, 4.3, 4.4, 4.6, 5]},
            #{'res_file':'all-results.json','prefix':'btrdb-btr18','val':1000, 'num_clients':200, 'iat':[1, 4.5, 4.75, 5, 5.2, 5.4, 5.6, 5.8, 6, 7]},
            #{'res_file':'all-results.json','prefix':'btrdb-btr18','val':10000, 'num_clients':200, 'iat':[1,2,3,4,5,6, 7, 8, 9, 10, 11, 12]},

            #{'res_file':'results.json','prefix':'btrdb-btr22-clp4','val':10000, 'num_clients':40, 'iat':[6, 6.5, 7, 7.5]},
            #{'res_file':'results.json','prefix':'btrdb-btr22-clp8','val':10000, 'num_clients':80, 'iat':[6, 6.5, 7, 7.5]},
            #{'res_file':'results.json','prefix':'btrdb-btr22-clp12','val':10000, 'num_clients':120, 'iat':[6, 6.5, 7, 7.5]},
            #{'res_file':'results.json','prefix':'btrdb-btr22-clp24','val':10000, 'num_clients':240, 'iat':[6, 6.5, 7, 7.5]},
            #{'res_file':'results.json','prefix':'btrdb-btr18-clp20','val':10000, 'num_clients':200, 'iat':[1,3,4,5,6, 6.5, 7, 7.5, 8, 8.5, 9, 9.15, 9.3, 9.5]}, #<<<<<<<<
            # ------------------------
            #{'res_file':'results.json','prefix':'btrdb-btr18-clp20','val':100, 'num_clients':200, 'iat':[0.4, 0.6, 0.8, 1, 1.2, 1.3, 1.5, 1.6, 1.8]},
            #{'res_file':'results.json','prefix':'btrdb-btr18-clp20','val':500, 'num_clients':200, 'iat':[1, 2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.2, 4.3, 4.5, 4.75, 5]},
            #{'res_file':'results.json','prefix':'btrdb-btr18-clp20','val':1000, 'num_clients':200, 'iat':[1,2, 3, 3.5, 3.75, 4, 4.5, 4.75, 5, 5.25, 5.5, 6]},
            #{'res_file':'results.json','prefix':'btrdb-btr18-clp20','val':10000, 'num_clients':200, 'iat':[1,3,4,5,6, 6.5, 7, 7.5, 8, 8.5, 8.65, 8.85, 9, 9.15, 9.3, 9.5]},
            #--------------------------
            #{'res_file':'results.json','prefix':'btrdb-btr24-clp10','val':100, 'num_clients':100, 'iat':[0.4, 0.6, 0.8, 1, 1.2, 1.3, 1.5, 1.6, 1.8]},
            #{'res_file':'results.json','prefix':'btrdb-btr24-clp10','val':500, 'num_clients':100, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.2, 4.3, 4.5, 4.75, 5]},
            #{'res_file':'results.json','prefix':'btrdb-btr24-clp10','val':1000, 'num_clients':100, 'iat':[1,2, 3, 3.5, 3.75, 4, 4.5, 4.75, 5, 5.25, 5.5, 6]},
            #{'res_file':'results.json','prefix':'btrdb-btr24-clp10','val':10000, 'num_clients':100, 'iat':[1,3,4,5,6, 6.5, 7, 7.5, 8, 8.5, 8.65, 8.85, 9, 9.15, 9.3, 9.5]},
            #--------------------------
            # Results for batch size test #<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
            #{'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[0.4, 0.6, 1, 1.2, 1.3, 1.5, 1.6, 1.8]},
            #{'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':500, 'num_clients':200, 'iat':[2, 2.5, 2.75, 3, 3.25, 3.5, 3.75, 4, 4.2, 4.3, 4.5, 4.75, 5]},
            #{'res_file':'results.json','prefix':'btrdb-btr26-clp20','val':500, 'num_clients':200, 'iat':[3.6, 3.65, 3.7]},
            #{'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1000, 'num_clients':200, 'iat':[1,2, 3, 3.5, 3.75, 4, 4.5, 4.75, 5, 5.25, 5.5, 6]},
            #{'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':10000, 'num_clients':200, 'iat':[6, 7, 8, 8.65, 8.85, 9, 9.15, 9.3, 9.5]},
            #{'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':20000, 'num_clients':200, 'iat':[7, 7.5, 8, 8.5, 9, 9.15, 9.3, 9.4, 9.5]},
            {'res_file':'results.json','prefix':'btrdb-btr29-clp20-svrs1','val':30000, 'num_clients':200, 'iat':[7, 7.5, 8, 8.5, 9, 9.3, 9.5, 9.7, 10, 10.2, 10.4,]},
            #-------------------
            # {'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs1','val':500, 'num_clients':200, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, 4.30, ]},
            # {'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs2','val':500, 'num_clients':400, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, 4.30, ]},
            # {'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs3','val':500, 'num_clients':600, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, 4.30, ]},
            # {'res_file':'results.json','prefix':'btrdb-btr27-clp20-svrs4','val':500, 'num_clients':800, 'iat':[2.5, 2.75, 3, 3.25, 3.5, 3.75, 3.85, 4, 4.15, 4.30, ]},
        ]},
    ]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [0]
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

    #BTrDB: 100:1.3, 500:3.75, 1000:5.5, 10000:8.85
    #Kawkab: 100:7, 500:10, 1000:12.75, 10000:12
    config['metric'] = [
        { 'type':'btrdb', 'label':'BTrDB',
          'name':'batch_size', 'points':[
            #{'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[0.4, 0.6,  1.2, 1.3, 1.5, 1.6, 1.8]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':500, 'num_clients':200, 'iat':[2, 2.5, 3.25, 3.75, 4, 4.2, 4.5,]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1000, 'num_clients':200, 'iat':[1,2, 3, 4, 4.5, 4.75, 5.25, 5.5, 6]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':10000, 'num_clients':200, 'iat':[ 6, 7, 7.5, 8, 8.5, 8.65, 8.85, 9, 9.3]},
        ]},
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'batch_size', 'points':[
            #{'res_file':'all-results.json', 'prefix':'rw-kw11', 'val':100, 'num_clients':200, 'iat':[6, 6.2, 6.4, 6.6, 6.7, 6.8, 6.9, 7, 7.2]},
            {'res_file':'all-results.json', 'prefix':'rw-kw29', 'val':500, 'num_clients':200, 'iat':[8, 10, 11]},
            {'res_file':'all-results.json', 'prefix':'rw-kw22', 'val':1000, 'num_clients':200, 'iat':[8, 9, 10, 11, 12, 12.75, 13]},
            {'res_file':'all-results.json', 'prefix':'rw-kw14', 'val':10000, 'num_clients':200, 'iat':[7, 9, 9.5, 10, 10.5, 11, 11.5, 12, 12.5]},
        ]},
    ]
    config['write_ratio'] = [100]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [0]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    #config['iat'] = [925, 875, 825, 775, 750]
    config['test_runs'] = [1,2,3]

    #metric = ('write_ratio', config['write_ratio'])

    results = load_results(config)

    #title = "Throughput and latency (Batch Size)"
    title = ""
    fig_prefix = "batch-size-lines"

    batch_size_results_lines(config, results, figParams, fig_prefix, title, None, 1000, True, True, False)

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
    #BTrDB: (100, 1.6), (500, 4.2), (1000, 5.8), (10000, 7)
    #Kawkab base: (100, ), (500, 1000), (1000, ), (10000, )
    #Kawkab: (100, 6.7), (500, 10.1), (1000, 11), (10000, 11)
    #Kawkab 14: (500, 9.5), (1000, 10), (10000, 11)

    #BTrDB: 100:1.3, 500:3.75, 1000:5.5, 10000:8.85
    #Kawkab: 100:7, 500:10, 1000:12.75, 10000:12

    config['metric'] = [
        { 'type':'kawkab', 'label':'Kawkab',
          'name':'batch_size', 'points':[
            #{'res_file':'all-results.json', 'prefix':'rw-kw11', 'val':100, 'num_clients':200, 'iat':[7]},
            {'res_file':'all-results.json', 'prefix':'rw-kw29', 'val':500, 'num_clients':200, 'iat':[10]},
            {'res_file':'all-results.json', 'prefix':'rw-kw22', 'val':1000, 'num_clients':200, 'iat':[12.75]},
            {'res_file':'all-results.json', 'prefix':'rw-kw14', 'val':10000, 'num_clients':200, 'iat':[12]},
        ]},
        # { 'type':'kawkab', 'label':'Kawkab base',
        #   'name':'batch_size', 'points':[
        #     {'res_file':'write-results.json', 'prefix':'rw-sep12', 'val':100, 'num_clients':160, 'iat':[160]},
        #     {'res_file':'write-results.json', 'prefix':'rw-sep11', 'val':500, 'num_clients':160, 'iat':[1000]},
        #     {'res_file':'write-results.json', 'prefix':'rw-sep14', 'val':1000, 'num_clients':160, 'iat':[2075]},
        #     {'res_file':'write-results.json', 'prefix':'rw-sep15', 'val':10000, 'num_clients':160, 'iat':[24000]},
        # ]},
        { 'type':'btrdb', 'label':'BTrDB',
          'name':'batch_size', 'points':[
            #{'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':100, 'num_clients':200, 'iat':[1.3]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':500, 'num_clients':200, 'iat':[3.75]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':1000, 'num_clients':200, 'iat':[5.5]},
            {'res_file':'results.json','prefix':'btrdb-btr25-clp20','val':10000, 'num_clients':200, 'iat':[8.85]},
        ]},
    ]

    fgp = {}
    fgp.update(figParams)
    fgp.update({"legend_cols": 3, 'markers': False})

    results = load_results(config)

    title = ""#"Throughput with different batch sizes"
    fig_prefix = "bs-bars"

    batch_size_bars(config, results, fgp, fig_prefix, title, True, None, None, True)

def results_write_ratio(conf, figParams):
    config = {}
    config.update(conf)

    config['metric'] = [
        { 'type':'kawkab', 'label':'writes',
          'name':'write_ratio', 'points':[
            {'res_file':'write-results.json', 'prefix':'rw-kw29', 'val':100, 'num_clients':200, 'iat':[8,10,11]},
            {'res_file':'write-results.json', 'prefix':'rw-kw30', 'val':90, 'num_clients':200, 'iat':[5, 9, 10.5, 11.25, 11.75, 12]},
            {'res_file':'write-results.json', 'prefix':'rw-kw30', 'val':50, 'num_clients':200, 'iat':[13, 14, 15, 16, 17, 18, 19, 20]},
            #{'res_file':'write-results.json', 'prefix':'rw-kw30', 'val':10, 'num_clients':200, 'iat':[22, 24, 26, 28, 28.5, 33, 33.5, 34]},

        ]},
        { 'type':'kawkab', 'label':'reads',
          'name':'write_ratio', 'points':[
            {'res_file':'read-results.json', 'prefix':'rw-kw30', 'val':90, 'num_clients':200, 'iat':[5, 9, 10.5, 11.25, 11.75, 12]},
            {'res_file':'read-results.json', 'prefix':'rw-kw30', 'val':50, 'num_clients':200, 'iat':[13, 14, 15, 16, 17, 18, 19, 20]},
            #{'res_file':'all-results.json', 'prefix':'rw-kw30', 'val':10, 'num_clients':200, 'iat':[22, 24, 26, 28, 28.5, 33, 33.5, 34]},
        ]},
        { 'type':'kawkab', 'label':'writes  ',
          'name':'write_ratio', 'points':[
            {'res_file':'all-results.json', 'prefix':'rw-kw29', 'val':100, 'num_clients':200, 'iat':[8,10,11]},
            {'res_file':'all-results.json', 'prefix':'rw-kw30', 'val':90, 'num_clients':200, 'iat':[5, 9, 10.5, 11.25, 11.75, 12]},
            {'res_file':'all-results.json', 'prefix':'rw-kw30', 'val':50, 'num_clients':200, 'iat':[13, 14, 15, 16, 17, 18, 19, 20]},
            #{'res_file':'all-results.json', 'prefix':'rw-kw30', 'val':10, 'num_clients':200, 'iat':[22, 24, 26, 28, 28.5, 33, 33.5, 34]},

        ]},
    ]
    config['write_ratio'] = [0]
    config['num_clients'] = [200]
    config['clients_per_machine'] = [10]
    config['batch_size'] = [500]
    config['record_size'] = [16]
    config['files_per_client'] = [1]
    config['test_runs'] = [1,2,3,4,5]


    results = load_results(config)

    title = ""
    fig_prefix = "write-ratio"

    write_ratio_results(config, results, figParams, fig_prefix, title, None, None, True, True, True)


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


def plot_graphs(conf, figParams):
    # results = load_results(conf)

    # figParams['figsize'] =(3.25, 2)
    # figParams['dimensions'] = (0.2, 0.99, 0.78, 0.18)
    # figParams['legend_position'] = (0.47, 1.45)
    # figParams['bars_width'] = 0.1

    thr_lat_bs(conf, figParams)

    #thr_lat_iat(conf, figParams)

    #hist_read_results(conf, figParams) #<<< Historical read results (no realtime reads)

    #thr_lat_stream_scale(conf, figParams)

    # Results
    #--------
    #results_write_ratio(conf, figParams)
    #results_bs_bars(conf, figParams)
    #results_bs_lines(conf, figParams)
    #stream_scale_results(conf, figParams)
    #hist_read_results_hq16(conf, figParams) #<<< Historical read results

if __name__ == '__main__':
    conf = configParams({})
    figParams = updateFigParams(fp_default)

    plot_graphs(conf, figParams)

    save_figures(conf)
