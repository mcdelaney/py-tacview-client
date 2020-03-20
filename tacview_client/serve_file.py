#!/usr/bin/env python3
"""Serve a local tacview file via sockets.
"""
import asyncio
from asyncio.log import logging
import gzip
from pathlib import Path
from functools import partial

import uvloop

logging.basicConfig(level=logging.INFO)
LOG = logging.getLogger('test_server')
logFormatter = logging.Formatter(
        "%(asctime)s [%(name)s] [%(levelname)-5.5s]  %(message)s")
consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
LOG.addHandler(consoleHandler)
LOG.propagate = False
LOG.setLevel(logging.INFO)


async def handle_req(reader, writer, filename: Path) -> None:
    """Send data."""
    try:
        LOG.info('Connection started...')
        if filename.suffix =='.gz':
            fp_ = gzip.open(filename, 'rb')
        else:
            fp_ = filename.open('rb')
        await reader.read(4026)

        while True:
            line = fp_.read()
            if not line:
                break
            writer.write(line)

        await writer.drain()
        writer.close()
        LOG.info("All lines sent...closing...")
    except (ConnectionResetError, BrokenPipeError):
        writer.close()


def run_server(filename: Path, port: str) -> None:
    """Read from Tacview socket."""
    LOG.info(f'Serving Tacview file {filename} 127.0.0.1:{port}. ..')
    uvloop.install()
    loop = asyncio.get_event_loop()
    loop.create_task(asyncio.start_server(partial(handle_req, filename=filename),
                                          "127.0.0.1", port))
    loop.run_forever()


def main(filename: Path, port: str) -> None:
    try:
        run_server(filename, port)
    except KeyboardInterrupt:
        LOG.info("Keyboard interupt!")
