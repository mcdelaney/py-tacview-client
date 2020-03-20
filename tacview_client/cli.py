import argparse
from tacview_client import config, client

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, required=True,
                        help='Tacview host ip to connect to.')
    parser.add_argument('--port', type=int, required=False,
                        default=42674, help='Port to connect on.')
    parser.add_argument('--bulk', action='store_true',
                        help='Should the program run in bulk mode?')
    parser.add_argument('--debug', action='store_true',
                        help='Should the program run in debug mode?')
    parser.add_argument('--postgres_dsn', type=str,
                        default=config.DB_URL,
                        help='DSN for connection to the postgres server.')
    args = parser.parse_args()

    try:
        client.main(host=args.host,
             port=args.port,
             debug=args.debug,
             max_iters=None,
             bulk=args.bulk,
             dsn=args.postgres_dsn)
    except KeyboardInterrupt:
        print("tacview-client shutting down...")
    except Exception as err:
        raise err

