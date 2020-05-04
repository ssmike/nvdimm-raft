#!/bin/env python

import json
import os
from copy import deepcopy as copy

quorum = int(os.getenv('QUORUM'))


def port(i):
    return 9000 + i


confs = [
    {
        'max_batch': 1,
        'max_delay': 0,
        'id': i,
        'port': port(i),
        'pool_size': 3,
        'max_message': 8192,
        'members': [
            {'host': 'localhost', 'port': port(i)}
            for i in range(quorum)
        ],
        'heartbeat_interval': 0.2,
        'heartbeat_timeout': 0.01,
        'election_timeout': 0.8,
        'applied_backlog': 10000,
        'log': '%d.dir' % (i,)
    }
    for i in range(quorum)
]

for i in range(quorum):
    with open('%d.json' % (i,), 'w') as fout:
        json.dump(confs[i], fout, indent=4)


client_conf = copy(confs[0])
client_conf['port'] = port(quorum)
with open('client.json', 'w') as fout:
    json.dump(client_conf, fout, indent=4)
