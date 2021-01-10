# py-tacview-client
A python tacview consumer.

## Overview
`py-tacview-client` processes either a realtime tacview steam, or a an existing tacview acmi
file, enriches the data to detect collisions between objects, parent relationships to child objects (projectiles), and streams the resulting data into a postgres database.

`tacview_client` assumes that you have set an environment variable, `TACVIEW_DATABASE_URL`, equal to the
connection DSN of a postgressql database where tacview output should be stored.
The format for the DSN should match:
```postgresql://{ip}:{port}/{dbname}?user={username}&password={password}```

## Developing
### Style:
The `py-tacview-client` codebase utilizes the `black` formatting standard.
