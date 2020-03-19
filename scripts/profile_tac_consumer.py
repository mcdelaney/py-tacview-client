#!/usr/bin/env python
import argparse
from functools import partial
from multiprocessing import Process
import sys
from pathlib import Path
import yappi # type: ignore

sys.path.append(str(Path('.').parent.absolute()))
sys.path.append(str(Path('.').parent.absolute().joinpath('tests')))
from tacview_client import client, db # type: ignore
import serve_test_data # type: ignore


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--iters", type=int, default=50000,
                        help='Number of lines to read')
    parser.add_argument('--profile', action='store_true',
                        help='Set this flag to run yappi profiler')
    parser.add_argument('--filename', type=str, required=True,
                        help='Filename to process')
    parser.add_argument('--bulk', action='store_true',
                        help='Should the program run in bulk mode?')
    args = parser.parse_args()

    server_proc = Process(target=partial(
        serve_test_data.main, filename=args.filename))
    server_proc.start()

    db.drop_and_recreate_tables()
    if args.profile:
        yappi.set_clock_type('cpu')
        yappi.start(builtins=True)

    client.main(host='127.0.0.1',
                port=5555,
                debug=False,
                max_iters=args.iters,
                bulk=args.bulk)

    if not args.profile:
        client.check_results()

    server_proc.terminate()

    if args.profile:
        prof_filename = 'callgrind.tacview.prof'
        stats = yappi.get_func_stats()
        stats.sort('ttot', 'asc')
        stats.save(prof_filename, type='callgrind') # type: ignore
