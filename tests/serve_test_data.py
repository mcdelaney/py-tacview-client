#!/usr/bin/env python3
"""Socket Perf Test.
"""
import argparse
import sys
from pathlib import Path

sys.path.append(str(Path('.').parent.absolute().joinpath('tacview_client')))
from tacview_client import serve_file # type: ignore


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--filename', required=True, type=Path,
                        help='Path to a taview acmi file that should be served locally.')
    parser.add_argument('--port', required=False, type=int, default=5555,
                        help='Port on which the data should be served.')
    args = parser.parse_args()
    serve_file.main(filename=args.filename, port=args.port)

