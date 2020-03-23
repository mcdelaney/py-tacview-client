import argparse
from functools import partial
from multiprocessing import Process
from pathlib import Path
import sys

from . import config, client, serve_file, db


class TacviewCli:
    def __init__(self):
        parser = argparse.ArgumentParser(
            description='Tacview-Client CLI',
            usage="Run one of: [client, createdb dropdb]")
        parser.add_argument('command', choices= ['client', 'dropdb', 'createdb'],
                            help='Subcommand to invoke')
        args = parser.parse_args(sys.argv[1:2])
        if not hasattr(self, args.command):
            print('Unrecognized command')
            parser.print_help()
            sys.exit(1)
        getattr(self, args.command)()

    def client(self):
        """CLI for tacview client reader."""
        parser = argparse.ArgumentParser(prog="tacview",
            description="""Tacview client allows for both batch and real-time processing
                        of a tacview stream or file.
                        Example usage:
                            tacview client
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
        args = parser.parse_args(sys.argv[2:])
        if not args.filename.exists():
            print("File does not exist at location: %s" % args.filename)
            sys.exit(1)

        try:
            if args.host in ['localhost', '127.0.0.1', '0.0.0.0'] and args.filename:
                print("Localhost and filename configured...will start server to host file...")
                server_proc = Process(target=partial(
                    serve_file.main, filename=args.filename, port=args.port))
                server_proc.start()
                client.main(host=args.host,
                            port=args.port,
                            debug=args.debug,
                            max_iters=None,
                            batch_size=args.batch_size,
                            dsn=args.postgres_dsn)
                server_proc.join()
        except KeyboardInterrupt:
            print("tacview-client shutting down...")
        except Exception as err:
            print(err)
        finally:
            try:
                server_proc.terminate() # type: ignore
            except Exception:
                pass

    def dropdb(self):
        """Drop database tables."""
        db.drop_tables()

    def createdb(self):
        """Create database tables."""
        db.create_tables()



def main():
    """Entrypoint to interact with tacview-client."""
    TacviewCli()



