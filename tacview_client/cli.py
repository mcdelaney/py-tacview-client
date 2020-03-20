import argparse
from functools import partial
from multiprocessing import Process
from pathlib import Path
import sys

from tacview_client import config, client, serve_file


def main():
    """Entrypoint to create tacview file readers."""
    parser = argparse.ArgumentParser(
        description="""Tacview client allows for both batch and real-time processing
        of a tacview stream or file.

        Example usage:
            tacview
               --host 127.0.0.1
               --filename C:/Users/mcdel/Documents/Tacview/important-mission.txt
               --postgres_dsn postgresql://0.0.0.0:5432/dcs?user=prod&password=pwd
        """)
    parser.add_argument('--host', type=str, required=False,
                        help='Tacview host ip to connect to. '
                        'If set to set either "127.0.0.1", "localhost" or"0.0.0.0" '
                        'and the filename argument is set a second process will be '
                        'created serve the file.')
    parser.add_argument('--filename', type=Path, required=False,
                        help='Path to valid tacview acmi file that should be read.'
                             ' This is only used if host = 127.0.0.1 or localhost.')
    parser.add_argument('--port', type=int, required=False,
                        default=42674, help='Port to connect on.')
    parser.add_argument('--batch_size', required=False,
                        type=int, default=500000,
                        help='Number of records to be combined in write batches')
    parser.add_argument('--debug', action='store_true',
                        help='Should the program run in debug mode?')
    parser.add_argument('--postgres_dsn', type=str,
                        default=config.DB_URL,
                        help='DSN for connection to the postgres server. Format should be:'
                        ' postgresql://{ip}:{port}/{dbname}?user={username}&password={password}')
    args = parser.parse_args()

    if not args.filename.exists():
        print("File does not exist at location: %s" % args.filename)
        sys.exit(1)

    if args.host in ['localhost', '127.0.0.1', '0.0.0.0'] and args.filename:
        print("Localhost and filename configured...will start server to host file...")
        server_proc = Process(target=partial(
            serve_file.main, filename=args.filename, port=args.port))
        server_proc.start()

    try:
        client.main(host=args.host,
                    port=args.port,
                    debug=args.debug,
                    max_iters=None,
                    batch_size=args.batch_size,
                    dsn=args.postgres_dsn)
    except KeyboardInterrupt:
        print("tacview-client shutting down...")
    except Exception as err:
        print(err)
        raise err
    finally:
        try:
            server_proc.terminate() # type: ignore
        except Exception:
            pass


