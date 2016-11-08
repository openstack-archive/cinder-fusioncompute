# Copyright 2016 Huawei Technologies Co.,LTD.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

"""
[VRM DRIVER] VRM CLIENT.

"""
import urlparse

from oslo_config import cfg
from oslo_log import log as logging

from cinder.volume.drivers.huawei.vrm.conf import FC_DRIVER_CONF
from cinder.volume.drivers.huawei.vrm.http_client import VRMHTTPClient


TASK_WAITING = 'waiting'
TASK_RUNNING = 'running'
TASK_SUCCESS = 'success'
TASK_FAILED = 'failed'
TASK_CANCELLING = 'cancelling'
TASK_UNKNOWN = 'unknown'

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class BaseProxy(object):
    '''BaseProxy

    BaseProxy
    '''
    def __init__(self):

        self.vrmhttpclient = VRMHTTPClient()
        self.site_uri = self.vrmhttpclient.get_siteuri()
        self.site_urn = self.vrmhttpclient.get_siteurn()
        self.limit = 100
        self.BASIC_URI = '/service'

    def _joined_params(self, params):
        '''_joined_params

        :param params:
        :return:
        '''
        param_str = []
        for k, v in params.items():
            if (k is None) or (v is None) or len(k) == 0:
                continue
            if k == 'scope' and v == self.site_urn:
                continue
            param_str.append("%s=%s" % (k, str(v)))
        return '&'.join(param_str)

    def _generate_url(self, path, query=None, frag=None):
        '''_generate_url

        :param path:
        :param query:
        :param frag:
        :return:url
        '''
        if CONF.vrm_ssl:
            scheme = 'https'
        else:
            scheme = 'http'
        fc_ip = FC_DRIVER_CONF.fc_ip

        netloc = str(fc_ip) + ':' + str(CONF.vrm_port)
        if path.startswith(self.BASIC_URI):
            url = urlparse.urlunsplit((scheme, netloc, path, query, frag))
        else:
            url = urlparse.urlunsplit(
                (scheme, netloc, self.BASIC_URI + str(path), query, frag))
        return url



