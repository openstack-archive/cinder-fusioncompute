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

from cinder.i18n import _
from cinder.volume.drivers.huawei.fusioncompute.base_proxy import BaseProxy


TASK_WAITING = 'waiting'
TASK_RUNNING = 'running'
TASK_SUCCESS = 'success'
TASK_FAILED = 'failed'
TASK_CANCELLING = 'cancelling'
TASK_UNKNOWN = 'unknown'

LOG = logging.getLogger(__name__)


class ClusterProxy(BaseProxy):
    '''ClusterProxy

    '''
    def __init__(self):
        super(ClusterProxy, self).__init__()

    def list_cluster(self):
        '''list_cluster

        Get <cluster_uri>?tag=xxx&clusterUrns=urn1&clusterUrns=urn2 HTTP/1.1
        Host: https://<ip>:<port>
        Accept: application/json;version=<version>; charset=UTF-8
        X-Auth-Token: <Authen_TOKEN>

        :param:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start list_cluster()"))
        uri = '/clusters'
        method = 'GET'
        path = self.site_uri + uri

        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method)
        clusters = body.get('clusters')

        return clusters

    def list_hosts(self, **kwargs):
        '''list_hosts

        Get <host_uri>?limit=20&offset=0&scope=xxx HTTP/1.1
        Host: https://<ip>:<port>
        Accept: application/json;version=<version>; charset=UTF-8
        X-Auth-Token: <Authen_TOKEN>

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start list_host()"))
        uri = '/hosts'
        method = 'GET'
        path = self.site_uri + uri
        params = {
            'scope': kwargs.get('clusterUrn'),
        }
        appendix = self._joined_params(params)
        new_url = self._generate_url(path, appendix)
        resp, body = self.vrmhttpclient.request(new_url, method)
        hosts = body.get('hosts')
        return hosts
