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
from oslo_config import cfg
from oslo_log import log as logging

from cinder.i18n import _
from cinder.volume.drivers.huawei.vrm.base_proxy import BaseProxy
from cinder.volume.drivers.huawei.vrm import exception as driver_exception
from cinder.volume.drivers.huawei.vrm.utils import Delete_Snapshot_Code

try:
    from eventlet import sleep
except ImportError:
    from time import sleep

TASK_WAITING = 'waiting'
TASK_RUNNING = 'running'
TASK_SUCCESS = 'success'
TASK_FAILED = 'failed'
TASK_CANCELLING = 'cancelling'
TASK_UNKNOWN = 'unknown'

CONF = cfg.CONF

LOG = logging.getLogger(__name__)


class TaskProxy(BaseProxy):
    '''TaskProxy

    TaskProxy
    '''
    def __init__(self):
        super(TaskProxy, self).__init__()

    def wait_task(self, isShortQuery=0, **kwargs):
        '''wait_task

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start wait_task()"))

        task_uri = kwargs.get('task_uri')
        method = 'GET'
        if task_uri is None:
            LOG.info(_("[VRM-CINDER] task_uri is none."))
            raise driver_exception.ClientException(101)
        else:
            new_url = self._generate_url(task_uri)
            retry = 0
            error_num = 0
            while retry < int(CONF.vrm_timeout):
                if isShortQuery == 0:
                    if retry > 10:
                        retry += 10
                        sleep(10)
                    else:
                        retry += 3
                        sleep(3)
                else:
                    if retry > 10:
                        retry += 10
                        sleep(10)
                    else:
                        retry += 1
                        sleep(1)
                try:
                    resp, body = self.vrmhttpclient.request(new_url, method)
                except Exception as ex:
                    LOG.info(_("[VRM-CINDER] querytask request exception."))
                    error_num += 1
                    if 30 < error_num:
                        LOG.info(
                            _("[VRM-CINDER] querytask request exception."))
                        raise ex
                    else:
                        continue

                if body:
                    status = body.get('status')
                    if status in [TASK_WAITING, TASK_RUNNING]:

                        continue
                    elif status in [TASK_SUCCESS]:
                        LOG.info(
                            _("[VRM-CINDER] return TASK_SUCCESS wait_task()"))
                        return status
                    elif status in [TASK_FAILED, TASK_CANCELLING]:
                        LOG.info(
                            _("[VRM-CINDER] return TASK_FAILED wait_task()"))
                        if body.get('reason') in Delete_Snapshot_Code:
                            raise driver_exception.ClientException(
                                code=101,
                                message=body.get('reasonDes'),
                                errorCode=body.get('reason'))
                        raise driver_exception.ClientException(101)
                    else:
                        LOG.info(_("[VRM-CINDER] pass wait_task()"))
                        error_num += 1
                        if 30 < error_num:
                            raise driver_exception.ClientException(101)
                        else:
                            continue
                else:
                    LOG.info(_("[VRM-CINDER] body is none."))
                    error_num += 1
                    if 30 < error_num:
                        raise driver_exception.ClientException(101)
                    else:
                        continue


