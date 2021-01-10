import sys

sys.path.append("/Users/mdelaney/projects/dcs-wp-magic/")
from dcs.tacview import client

client.main(
    host="127.0.0.1", port=5555, debug=False, max_iters=50, only_proc=False, bulk=True
)
