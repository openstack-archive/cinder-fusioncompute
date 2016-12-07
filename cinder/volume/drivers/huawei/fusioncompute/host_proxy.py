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

from oslo_log import log as logging

from cinder.volume.drivers.huawei.fusioncompute.base_proxy import BaseProxy


TASK_WAITING = 'waiting'
TASK_RUNNING = 'running'
TASK_SUCCESS = 'success'
TASK_FAILED = 'failed'
TASK_CANCELLING = 'cancelling'
TASK_UNKNOWN = 'unknown'

LOG = logging.getLogger(__name__)


class HostProxy(BaseProxy):
    '''HostProxy

    '''
    def __init__(self):
        super(HostProxy, self).__init__()

    def list_host(self, **kwargs):
        '''list_host

        :param kwargs:
        :return:
        '''
        # LOG.info(_("[VRM-CINDER] start list_host()"))
        uri = '/hosts'
        method = 'GET'
        path = self.site_uri + uri

        offset = 0
        hosts = []
        while True:
            parameters = {'limit': self.limit,
                          'offset': offset,
                          'scope': kwargs.get('scope')}

            appendix = self._joined_params(parameters)
            new_url = self._generate_url(path, appendix)
            resp, body = self.vrmhttpclient.request(new_url, method)
            total = int(body.get('total') or 0)
            if total > 0:
                res = body.get('hosts')
                hosts += res
                offset += len(res)
                if offset >= total or len(hosts) >= total or len(
                        res) < self.limit:
                    break
            else:
                break

        return hosts


