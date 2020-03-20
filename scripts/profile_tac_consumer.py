#!/usr/bin/env python
import argparse
import asyncio
from functools import partial
from multiprocessing import Process
import sys
from pathlib import Path

import yappi # type: ignore

sys.path.append(str(Path('.').parent.absolute().joinpath('tacview_client')))
sys.path.append(str(Path('.').parent.absolute().joinpath('tests')))
import client, db, config, serve_file # type: ignore


if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--iters", type=int, default=50000,
                        help='Number of lines to read')
    parser.add_argument('--profile', action='store_true',
                        help='Set this flag to run yappi profiler')
    parser.add_argument('--filename', type=Path, required=True,
                        help='Filename to process')
    parser.add_argument('--batch_size', required=False,
                        type=int, default=500000,
                        help='Number of records to be combined in write batches')
    args = parser.parse_args()

    server_proc = Process(target=partial(
        serve_file.main, filename=args.filename, port=5555))
    server_proc.start()

    db.drop_and_recreate_tables()

    if args.profile:
        yappi.set_clock_type('cpu')
        yappi.start(builtins=True)

    client.main(host='127.0.0.1',
                port=5555,
                debug=False,
                max_iters=args.iters,
                batch_size=args.batch_size,
                dsn=config.DB_URL)

    if not args.profile:
        asyncio.run(client.check_results())

    server_proc.terminate() # type: ignore

    if args.profile:
        prof_filename = 'callgrind.tacview.prof'
        stats = yappi.get_func_stats()
        stats.sort('ttot', 'asc')
        stats.save(prof_filename, type='callgrind') # type: ignore
