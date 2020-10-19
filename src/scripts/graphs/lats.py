#!/usr/bin/python

import numpy as np
import pprint as pp
import json
from plotutils import plotCDF, save_figures

rf = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-kw42-nc200-bs10000-rs16-nf1-wr20-iat40/run_5/read-results-hists.json'
wf = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-kw42-nc200-bs10000-rs16-nf1-wr20-iat40/run_5/write-results-hists.json'
af = '/home/sm3rizvi/kawkab/experiments/results/kawkab/rw-kw42-nc200-bs10000-rs16-nf1-wr20-iat40/run_5/all-results-hists.json'

def _read_results(fname):
    with open(fname) as data_file:
        data = json.load(data_file)
    return data[0]

def flatten(lats, counts):
    vals = []
    l = len(lats)
    for i in range(l):
        for j in range(counts[i]):
            vals.append(lats[i])

    return np.sort(vals)

rd = _read_results(rf)
wd = _read_results(wf)
ad = _read_results(af)

print(rd.keys())

rl = rd['Latency Histogram']['latency']
rc = rd['Latency Histogram']['count']
wl = wd['Latency Histogram']['latency']
wc = wd['Latency Histogram']['count']
al = ad['Latency Histogram']['latency']
ac = ad['Latency Histogram']['count']

#rlcs = np.sort(np.array(zip(rl,rc)), 0)
#wlcs = np.sort(np.array(zip(wl,wc)), 0)
#alcs = np.sort(np.array(zip(al,ac)), 0)


#import pdb; pdb.set_trace()

rl = [x/1000 for x in rl]
wl = [x/1000 for x in wl]
al = [x/1000 for x in al]

rlf = flatten(rl, rc)
wlf = flatten(wl, wc)
alf = flatten(al, ac)

#comb = list(rlf) + list(wlf)

# plotCDF([
#         #{'y':rl, 'label':'Read'},
#         {'y':rlf, 'label':'Read Fl'},
#         #{'y':wlf, 'label':'Write'},
#         {'y':alf, 'label':'All'},
#         #{'y':comb, 'label':'Combined'}
#          ], '', '', '')

#save_figures({'fig_dir':'figures'})

print('a', ad['50%Lat'], ad['95%Lat'], ad['99%Lat'])
print('r', rd['50%Lat'], rd['95%Lat'], rd['99%Lat'])
print('w', wd['50%Lat'], wd['95%Lat'], wd['99%Lat'])
print('r', np.percentile(rlf, 50), np.percentile(rlf, 95), np.percentile(rlf, 99))
print('w', np.percentile(wlf, 50), np.percentile(wlf, 95), np.percentile(wlf, 99))
print('a', np.percentile(alf, 50), np.percentile(alf, 95), np.percentile(alf, 99))

a,b,c = np.percentile(alf, [50, 95, 99])
print (a, b, c)
