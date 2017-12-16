#!/usr/bin/env python
from utils.getConfig import get_conf
from cm_api.api_client import ApiResource, ApiException
from cm_api.http_client import HttpClient
from cm_api.resource import Resource

# CM_HOST = get_conf("clouderaTest", "CM_HOST")
# CM_USER = get_conf("clouderaTest", "CM_USER")
# CM_PASSWD = get_conf("clouderaTest", "CM_PASSWD")
# VERSION = get_conf("clouderaTest","VERSION")

CM_HOST = get_conf("clouderaProd", "CM_HOST")
CM_USER = get_conf("clouderaProd", "CM_USER")
CM_PASSWD = get_conf("clouderaProd", "CM_PASSWD")
VERSION = get_conf("clouderaProd","VERSION")

class ApiClient(object):
    """

    """

    def __init__(self):
        self._api = ApiResource(CM_HOST, username=CM_USER, password=CM_PASSWD, use_tls=False, version=VERSION)

