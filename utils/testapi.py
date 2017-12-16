#!/usr/bin/env python
from cm_api.api_client import ApiResource
def main(argv):
  test()
def test():
  cm_host='xxx'
  api = ApiResource(cm_host, username="xxx", password="xxx", use_tls=False, version="12")

# Get a list of all clusters
  cdh4 = None
  for c in api.get_all_clusters():
    print c.name
    if c.version == "CDH5":
      cdh4 = c
## -- Output --
# Cluster 1 - CDH4
# Cluster 2 - CDH3