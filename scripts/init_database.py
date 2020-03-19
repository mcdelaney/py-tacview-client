#!/usr/bin/env python
import sys
from pathlib import Path
sys.path.append(str(Path('.').parent.absolute()))
from tacview_client.db import drop_and_recreate_tables

if __name__ == "__main__":
    drop_and_recreate_tables()

