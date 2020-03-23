#!/usr/bin/env python3
"""Serve a local tacview file via sockets.
"""
import asyncio
from asyncio import CancelledError
from asyncio.log import logging
import gzip
from pathlib import Path
from functools import partial

import uvloop

from tacview_client.config import get_logger
LOG = get_logger()


async def handle_req(reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                     filename: Path) -> None:
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
            check = await reader.read()
            if check and check == -1:
                break
        await writer.drain()
        writer.close()
        LOG.info("All lines sent...closing...")
    except (ConnectionResetError, BrokenPipeError, CancelledError):
        LOG.info("Cancel received..shutting down...")
        writer.close()
    LOG.info('Exiting...')


async def serve_file(filename: Path, port: int) -> None:
    server = await asyncio.start_server(
                    partial(handle_req, filename=filename),
                    "127.0.0.1", port)
    try:
        await server.serve_forever()
    except CancelledError:
        LOG.error("Cancel error caught!")
        server.close()


def main(filename: Path, port: int) -> None:
    """Read from Tacview socket."""
    uvloop.install()
    LOG.info(f'Serving Tacview file {filename} 127.0.0.1:{port}. ..')
    loop = asyncio.get_event_loop()
    task = loop.create_task(serve_file(filename, port))
    try:
        loop.run_until_complete(task)
    except KeyboardInterrupt:
        LOG.info("Keyboard interupt!")
        task.cancel()
    except Exception as err:
        task.cancel()
        raise err


if __name__=='__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', type=Path, help='Filename for server')
    parser.add_argument('--port', type=int, help='Port for server')
    args = parser.parse_args()
    main(args.filename, args.port)
