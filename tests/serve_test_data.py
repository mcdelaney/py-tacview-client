#!/usr/bin/env python3
"""Socket Perf Test.
"""
import argparse
import asyncio
from asyncio.log import logging
import gzip
from functools import partial
import sys
from pathlib import Path

from google.cloud import storage

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger('test_server')
logFormatter = logging.Formatter(
        "%(asctime)s [%(name)s] [%(levelname)-5.5s]  %(message)s")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
LOG.addHandler(consoleHandler)
LOG.propagate = False
LOG.setLevel(logging.INFO)


async def handle_req(reader, writer, filename: str) -> None:
    """Send data."""
    try:
        LOG.info('Connection started...')
        if filename.endswith('.gz'):
            fp_ = gzip.open(filename, 'rb')
        else:
            fp_ = open(filename, 'rb')
                # lines = fp_.readlines()
        await reader.read(4026)

        while True:
            line = fp_.read()
            if line:
                writer.write(line)
            else:
                break

        await writer.drain()
        writer.close()
        LOG.info("All lines sent...closing...")
    except (ConnectionResetError, BrokenPipeError):
        writer.close()


def run_server(filename: str, port: str) -> None:
    """Read from Tacview socket."""
    LOG.info(f'Serving tests data at 127.0.0.1:{port}. ..')
    loop = asyncio.get_event_loop()
    loop.create_task(asyncio.start_server(partial(handle_req, filename=filename),
                                          "127.0.0.1", port))
    loop.run_forever()


def main(filename: str, port: str) -> None:
    try:
        run_server(filename, port)
    except KeyboardInterrupt:
        LOG.info("Keyboard interupt!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', required=True, type=str,
                        help='Path to a taview acmi file that should be served locally.')
    parser.add_argument('--port', required=False, type=str, default='5555',
                        help='Port on which the data should be served.')
    args = parser.parse_args()
    main(filename=args.filename, port=args.port)

