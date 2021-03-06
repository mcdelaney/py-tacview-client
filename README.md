# py-tacview-client
A python tacview consumer.
Install from pypy with `pip install tacview-client`.

## Overview
`py-tacview-client` processes either a realtime tacview steam, or a an existing tacview acmi
file, enriches the data to detect collisions between objects, parent relationships to child objects (projectiles), and streams the resulting data into a postgres database.

`tacview_client` assumes that you have set an environment variable, `TACVIEW_DATABASE_URL`, equal to the
connection DSN of a postgressql database where tacview output should be stored.
The format for the DSN should match:
```postgresql://{ip}:{port}/{dbname}?user={username}&password={password}```

After installation, you'll need to initialize that tables with:
```tacview createdb```.
To nuke everything and start from scratch, run
```tacview dropdb```.

To process a single acmi file, run:
``` tacview process_file --filename {Path to the File}```.

To process a real-time event stream from a running tacview server, run:
```
tacview process_stream
    --host {ip-of-tacview-server}
    --port {default:42674}
    --client_name {auth username}
    --client_password {auth-password}
```

## Developing
### Style:
The `py-tacview-client` codebase utilizes the `black` formatting standard.
