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
import json

from oslo_log import log as logging

from cinder.i18n import _
from cinder.volume.drivers.huawei.vrm.base_proxy import BaseProxy
from cinder.volume.drivers.huawei.vrm import exception as driver_exception

TASK_WAITING = 'waiting'
TASK_RUNNING = 'running'
TASK_SUCCESS = 'success'
TASK_FAILED = 'failed'
TASK_CANCELLING = 'cancelling'
TASK_UNKNOWN = 'unknown'

LOG = logging.getLogger(__name__)


class DatastoreProxy(BaseProxy):
    '''DatastoreProxy

    DatastoreProxy
    '''
    def __init__(self):
        super(DatastoreProxy, self).__init__()

    def list_datastore(self, **kwargs):
        '''list_datastore

        :param kwargs:
        :return:
        '''

        # LOG.info(_("[VRM-CINDER] start list_datastore()"))
        uri = '/datastores'
        method = 'GET'
        path = self.site_uri + uri

        offset = 0
        datastores = []
        while True:
            parames = {'limit': self.limit,
                       'offset': offset,
                       'scope': kwargs.get('scope')}

            appendix = self._joined_params(parames)
            new_url = self._generate_url(path, appendix)
            resp, body = self.vrmhttpclient.request(new_url, method)
            total = int(body.get('total') or 0)
            if total > 0:
                res = body.get('datastores')
                datastores += res
                offset += len(res)
                if offset >= total or len(datastores) >= total or len(
                        res) < self.limit:
                    break
            else:
                break

        return datastores

    def query_datastore(self, **kwargs):
        '''Query DataStore

        :param kwargs:
        :return:
        '''
        # LOG.info(_("[VRM-CINDER] start list_datastore()"))
        uri = '/datastores' + '/' + kwargs.get('id')
        method = 'GET'
        path = self.site_uri + uri
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method)
        return body

    def check_ds_connect_cluster(self, **kwargs):
        '''Check cluster connected to the datastore

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start _check_ds_connect_cluster()"))
        uri = '/datastores'
        method = 'POST'
        body = {'clusterUrn': kwargs.get('cluster_urn')}
        path = self.site_uri + uri + '/' + kwargs.get(
            'datastore_id') + '/action/ifconcluster'
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        if_connect_flag = body.get('ifConnectFlag')
        if if_connect_flag and if_connect_flag is True:
            return
        else:
            raise driver_exception.NoNeededData()
