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
LOG.propagate=False
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


def run_server(filename: str) -> None:
    """Read from Tacview socket."""
    LOG.info('Serving tests data at 127.0.0.1:5555...')
    loop = asyncio.get_event_loop()
    loop.create_task(asyncio.start_server(partial(handle_req, filename=filename),
                                          "127.0.0.1", "5555"))
    loop.run_forever()


def main(filename: str) -> None:
    try:
        run_server(filename)
    except KeyboardInterrupt:
        LOG.info("Keyboard interupt!")


if __name__ == "__main__":
    main(filename='tests/data/tacview-test2.txt')

