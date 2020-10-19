#!/usr/bin/python

import json
from pprint import pprint

import numpy as np
from plotutils import get_mean_and_conf

cols = {'opsThr': 'OpsTput', 'dataThr':'DataTput', 'meanLat':'Mean latency', 'lat50': 'Median latency',
        'lat95':'95% latency', 'lat99':'99% latency', 'minLat':'Min latency', 'maxLat':'Max latency',
        'rpsThr':'Records per second', 'latCDF':'Latency CDF'}


def config_params():
    conf = {}

    conf['root_dir'] = '/home/sm3rizvi/culor'
    conf['test_id'] = 'test0'
    # conf['exp_dir'] = '%s/experiments'%(conf['root_dir'])
    # conf['test_dir'] = '%s/%s'%(conf['exp_dir'],conf['test_id'])
    conf['fig_dir'] = 'figures'

    conf['test_types'] = ['zk']
    conf['test_duration'] = [281]
    conf['threads_per_client'] = [1]

    conf['test_runs'] = [1]  # , 6, 7, 8, 9, 10]
    conf['num_servers'] = [3]
    conf['num_clients'] = [3]
    conf['write_ratio'] = [80]

    conf['results_dir'] = '../experiments/zk-lot'

    conf['start_cutoff_vals'] = 60
    conf['end_cutoff_vals'] = 10

    return conf


def get_test_id(conf, params):
    # btrdb-btr3-nc120-cpm15-bs500-rs16-nf1-wr100-iat3.000

    test_type = conf['metric'][0]['type']
    test_prefix = conf['metric'][0]['points'][0]['prefix']
    clients = conf['num_clients'][0]
    write_ratio = conf['write_ratio'][0]
    clients_per_machine = conf['clients_per_machine'][0]
    batch_size = conf['batch_size'][0]
    record_size = conf['record_size'][0]
    files_per_client = conf['files_per_client'][0]
    iat = conf['iat'][0]

    if 'test_type' in params: test_type = params['test_type']
    if 'test_prefix' in params: test_prefix = params['test_prefix']
    if 'num_clients' in params: clients = params['num_clients']
    if 'write_ratio' in params: write_ratio = params['write_ratio']
    if 'clients_per_machine' in params: clients_per_machine = params['clients_per_machine']
    if 'batch_size' in params: batch_size = params['batch_size']
    if 'record_size' in params: record_size = params['record_size']
    if 'files_per_client' in params: files_per_client = params['files_per_client']
    if 'iat' in params: iat = params['iat']

    if test_type == 'btrdb':
        id = '%s-nc%d-cpm%d-bs%d-rs%d-nf%d-wr%d-iat%.2f' % (
            test_prefix,
            clients,
            clients_per_machine,
            batch_size,
            record_size,
            files_per_client,
            write_ratio,
            iat
        )
    else:
        id = '%s-nc%d-bs%d-rs%d-nf%d-wr%d-iat%g' % (
            test_prefix,
            clients,
            batch_size,
            record_size,
            files_per_client,
            write_ratio,
            iat
        )

    return id


# ------------------------------------------------------------------------------

def _get_results(conf, num_clients, results_dir):
    # cols = {'client':0,'thr':1,'avg_lat':2,'med_lat':3,'99lat':4,}
    results = []
    for i in range(num_clients):
        results_file = '%s/results/client_%02d.txt' % (results_dir, i + 1)
        results.append(_read_results(results_file))

    return results


def _read_results(fname):
    with open(fname) as data_file:
        data = json.load(data_file)
    return data


def get_avg_clients_results(conf, num_clients, results_dir):
    rps = 0.0
    lat = []
    for i in range(num_clients):
        results_file = '%s/results/client_%02d.txt' % (results_dir, i + 1)
        first_line, thrs, lats = _read_results(results_file)

        first_line = first_line.split()
        rps += float(first_line[0])
        lat.append(float(first_line[1]))

    if num_clients > 1:
        print('''WARNING: the list of throughput and 
            latencies returns the results from the last client''')

    lat = np.mean(lat)
    return rps, lat, thrs, lats


def get_runs_data(res_dir, file_prefix):
    fname = '%s/%s'%(res_dir,file_prefix)
    print ("Loading", fname)

    with open(fname) as data_file:
        #data = json.load(data_file)
        data = data_file.read().splitlines()

        lines = ""
        for i,line in enumerate(data):
            if line.startswith("\"Median") and not line.endswith(","):
                line = '%s,'%(line)

            lines += line + '\n'

    data = json.loads(lines)

    return data


def _num(s):
    try:
        return int(s)
    except ValueError:
        return float(s)


def get_and_parse_runs_data(res_dir, file_prefix, lat_from_hists=False):
    fname = '%s/%s'%(res_dir,file_prefix)
    print ("Loading", fname)
    res = {}

    with open(fname) as data_file:
        data = data_file.read().splitlines()
        for line in data:
            try:
                for kv in line.split():
                    try:
                        k, v = kv.split(':')
                        k = k.replace('"', '')
                        v = _num(v.replace(',',''))

                        if k == 'opsPs': k = 'OpsTput'
                        elif k == 'thrMBps': k = 'DataTput'
                        elif k == '50%Lat': k = 'Median latency'
                        elif k == '95%Lat': k = '95% latency'
                        elif k == '99%Lat': k = '99% latency'
                        elif k == 'recsPs': k = 'Records per second'
                        elif k == 'meanLat': k = 'Mean latency'
                        elif k == 'minLat' : k = 'Min latency'
                        elif k == 'maxLat' : k = 'Max latency'

                        res[k] = v
                    except ValueError:
                        continue
            except ValueError:
                continue

    if lat_from_hists:
        mn, mx, avg, p25, p50, p75, p95, p99 = _lats_from_hists(res_dir)

        print ('1 p50=%g, p95=%g, p99=%g'%(res['Median latency'], res['95% latency'], res['99% latency']))

        res['Min latency'] = mn
        res['Max latency'] = mx
        res['Mean latency'] = avg
        res['Median latency'] = p50
        res['95% latency'] = p95
        res['99% latency'] = p99

        print ('2 p50=%g, p95=%g, p99=%g'%(res['Median latency'], res['95% latency'], res['99% latency']))

        #import pdb; pdb.set_trace()

    #pprint(res)
    return res

def summarize_data(runs_data):
    opsThr = []
    dataThr = []
    rpsThr = []
    meanLat = []
    lat50 = []
    lat95 = []
    lat99 = []
    minLat = 9223372036854775807 # INT_MAX
    maxLat = 0

    for data in runs_data:

        opsThr.append(data[cols['opsThr']])
        dataThr.append(data[cols['dataThr']])
        rpsThr.append(data[cols['rpsThr']])

        meanLat.append(data[cols['meanLat']])
        lat50.append(data[cols['lat50']])
        lat95.append(data[cols['lat95']])
        lat99.append(data[cols['lat99']])

        if minLat > data[cols['minLat']]: minLat = data[cols['minLat']]
        if maxLat < data[cols['maxLat']]: maxLat = data[cols['maxLat']]

    res = {
        'meanLat': get_mean_and_conf(meanLat),
        'opsThr': get_mean_and_conf(opsThr),
        'dataThr': get_mean_and_conf(dataThr),
        'rpsThr': get_mean_and_conf(rpsThr),
        'lat50': get_mean_and_conf(lat50),
        'lat95': get_mean_and_conf(lat95),
        'lat99': get_mean_and_conf(lat99),
        'minLat': (minLat, 0),
        'maxLat': (maxLat, 0)
    }

    #print('Lat50: ', res['lat50'], lat50)
    #print('Lat95: ', res['lat95'], lat95)

    #import pdb; pdb.set_trace()
    return res

def _expand(lats, counts):
    vals = []
    l = len(lats)
    for i in range(l):
        for j in range(counts[i]):
            vals.append(lats[i])

    return np.sort(vals)

def _lats_from_hists(res_dir):
    f = '%s/read-results-hists.json'%(res_dir)
    print('Reading ',f)
    data = _read_results(f)
    rl = data[0]['Latency Histogram']['latency']
    rc = data[0]['Latency Histogram']['count']

    f = '%s/write-results-hists.json'%(res_dir)
    print('Reading ',f)
    data = _read_results(f)
    wl = data[0]['Latency Histogram']['latency']
    wc = data[0]['Latency Histogram']['count']

    rlf = _expand(rl, rc)
    wlf = _expand(wl, wc)

    rwlf = np.concatenate((rlf, wlf))

    p25, p50, p75, p95, p99 = np.percentile(rwlf, [25, 50, 75, 95, 99])

    mn = np.min(rwlf)
    mx = np.max(rwlf)
    avg = np.mean(rwlf)

    return mn, mx, avg, p25, p50, p75, p95, p99

def load_results(conf):
    # btrdb-btr3-nc120-cpm15-bs500-rs16-nf1-wr100-iat3.000
    all_results = {}
    for batch_size in conf['batch_size']:
        for record_size in conf['record_size']:
            for clients in conf['num_clients']:
                for cpm in conf['clients_per_machine']:
                    for write_ratio in conf['write_ratio']:
                        for files_per_client in conf['files_per_client']:
                            #for iat in conf['iat']:
                            for metric in conf['metric']:
                                metric_name = metric['name']
                                metric_points = metric['points']
                                test_type = metric['type']
                                for point in metric_points:
                                    metric_point = point['val']
                                    test_prefix = point['prefix']
                                    res_file = point['res_file']
                                    from_hists = False
                                    if 'from_hist' in point and point['from_hist']: from_hists = True
                                    if 'num_clients' in point: clients = point['num_clients']
                                    for iat in point['iat']:
                                        params = {'test_type': test_type, 'test_prefix':test_prefix, 'batch_size': batch_size,
                                                  'record_size': record_size, 'num_clients': clients,
                                                  'files_per_client': files_per_client, 'write_ratio': write_ratio,
                                                  'clients_per_machine': cpm, 'iat': iat, metric_name:metric_point
                                                  }
                                        test_id = get_test_id(conf, params)
                                        print (test_id)
                                        run_data = []

                                        for run_i in conf['test_runs']:
                                            results_dir = '%s/%s/%s/run_%d' % (conf['results_dir'], test_type, test_id, run_i)

                                            if test_type == 'kawkab':
                                                data = get_and_parse_runs_data(results_dir, res_file, from_hists)
                                            else:
                                                data = get_runs_data(results_dir, res_file)
                                            run_data.append(data)

                                        all_results[test_id] = {'runs_data': run_data,
                                                            'agg_data': summarize_data(run_data),
                                                            }
                                        all_results[test_id+res_file] = {'runs_data': run_data,
                                                                'agg_data': summarize_data(run_data),
                                                                }
    return all_results


if __name__ == '__main__':
    conf = config_params()
    results = load_results(conf)

    print(results)
