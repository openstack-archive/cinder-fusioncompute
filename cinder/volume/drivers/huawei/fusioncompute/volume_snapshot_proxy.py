# vim: tabstop=4 shiftwidth=4 softtabstop=4

"""
[VRM DRIVER] VRM CLIENT.

"""
import json

from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception as cinder_exception
from cinder.i18n import _
from cinder.volume.drivers.huawei.vrm.base_proxy import BaseProxy
from cinder.volume.drivers.huawei.vrm import exception as driver_exception
from cinder.volume.drivers.huawei.vrm.task_proxy import TaskProxy
from cinder.volume.drivers.huawei.vrm.utils import Delete_Snapshot_Code


CONF = cfg.CONF
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

LOG = logging.getLogger(__name__)


class VolumeSnapshotProxy(BaseProxy):
    def __init__(self, *args, **kwargs):
        super(VolumeSnapshotProxy, self).__init__()
        LOG.info(_("[VRM-CINDER] start __init__()"))
        self.task_proxy = TaskProxy()
        self.create_retry = ['10000009', '10430056', '10430050', '10300421', '10430057', '10300318', '10300162', '10300026']
        self.delete_retry = Delete_Snapshot_Code
        self.number = CONF.vrm_vol_snapshot_retries
        self.sleep_time = CONF.vrm_snapshot_sleeptime
        if not self.number:
            LOG.error('conf number is None, use 3')
            self.number = 3

        if not self.sleep_time:
            LOG.error('conf sleeptime is None, use 300')
            self.sleep_time = 300

    def query_volumesnapshot(self, **kwargs):
        '''query_volumesnapshot

            'list_volumesnapshot': ('GET',
                                ('/volumesnapshots', None, kwargs.get('uuid'), None),
                                {'limit': kwargs.get('limit'),
                                 'offset': kwargs.get('offset'),
                                 'scope': kwargs.get('scope')
                                },
                                {},
                                False),
        '''
        LOG.info(_("[VRM-CINDER] start query_volumesnapshot()"))
        uri = '/volumesnapshots'
        method = 'GET'
        path = self.site_uri + uri + '/' + kwargs.get('uuid')
        body = None
        offset = 0
        datastores = []
        new_url = self._generate_url(path)
        try:
            resp, body = self.vrmhttpclient.request(new_url, method)
        except driver_exception.ClientException as ex:
            LOG.info(_("[VRM-CINDER] query snapshot (%s)"), ex.errorCode)
            if ex.errorCode == "10430051":
                return None
            else:
                raise ex

        '''
        error_code = body.get('errorCode')
        if error_code != None:
            if '10430010' == error_code:
                LOG.info(_("[VRM-CINDER] snapshot not exist"))
                return None
        '''

        return body

    def list_snapshot(self, **kwargs):
        '''list_snapshot

            'list_volumesnapshot': ('GET',
                                ('/volumesnapshots', None, kwargs.get('uuid'), None),
                                {'limit': kwargs.get('limit'),
                                 'offset': kwargs.get('offset'),
                                 'scope': kwargs.get('scope')
                                },
                                {},
                                False),
        '''
        LOG.info(_("[VRM-CINDER] start list_snapshot()"))
        uri = '/volumesnapshots/queryVolumeSnapshots'
        method = 'GET'
        path = self.site_uri + uri
        body = None
        offset = 0

        snapshots = []
        while True:
            parames = {
                'limit': self.limit,
               'offset': offset
               }
            appendix = self._joined_params(parames)
            new_url = self._generate_url(path, appendix)
            resp, body = self.vrmhttpclient.request(new_url, method)
            total = int(body.get('total') or 0)
            if total > 0:
                res = body.get('snapshots')
                snapshots += res
                offset += len(res)
                if offset >= total or len(snapshots) >= total or len(res) < self.limit:
                    break
            else:
                break
       
        return snapshots
        
    def create_volumesnapshot(self, **kwargs):
        '''create_volumesnapshot

            'create_volumesnapshot': ('POST',
                                  ('/volumesnapshots', None, None, None),
                                  {},
                                  {'volumeUrn': kwargs.get('vol_urn'), 'snapshotUuid': kwargs.get('uuid'),
                                  },
                                  False),
        '''
        LOG.info(_("[VRM-CINDER] start create_volumesnapshot()"))
        uri = '/volumesnapshots'
        method = 'POST'
        path = self.site_uri + uri
        body = {
            'volumeUrn': kwargs.get('vol_urn'),
            'snapshotUuid': kwargs.get('snapshot_uuid')
        }
        enable_active = kwargs.get('enable_active', None)
        if enable_active is not None:
            body.update({'enableActive': enable_active})
        new_url = self._generate_url(path)

        number = self.number
        sleep_time = self.sleep_time
        while(number >= 0):
            try:
                resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
                task_uri = body.get('taskUri')
                if task_uri is not None:
                    self.task_proxy.wait_task(task_uri=task_uri)
                return body
            except driver_exception.ClientException as ex:
                LOG.info(_("[VRM-CINDER] create volumesnapshot (%s)"), ex.errorCode)
                if ex.errorCode not in self.create_retry:
                    raise ex
                LOG.debug('[VRM-CINDER] The errorcode is in retry list:%d' % number)
                number -= 1
                if number < 0:
                    raise ex
                sleep(sleep_time)


    def active_snapshots(self, **kwargs):
        '''active_snapshots

            'active_snapshots': ('POST',
                                  ('/volumesnapshots/enableSnapshots', None, None, None),
                                  {},
                                  {'snapshotList': kwargs.get('snapshot_urns')},
                                  False),
        '''
        LOG.info(_("[VRM-CINDER] start active_snapshots()"))
        uri = '/volumesnapshots/enableSnapshots'
        method = 'POST'
        path = self.site_uri + uri
        body = {
            'snapshotUuidList': kwargs.get('snapshot_uuids')
        }
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        task_uri = body.get('taskUri')
        if task_uri is not None:
            self.task_proxy.wait_task(task_uri=task_uri)
        return body

    def delete_volumesnapshot(self, **kwargs):
        '''delete_volumesnapshot

                    'delete_volumesnapshot': ('DELETE',
                                          ('/volumesnapshots', None, None, kwargs.get('id')),
                                          {},
                                          {
                                              'snapshotUuid': kwargs.get('snapshotUuid'),
                                          },
                                          True),
        '''
        LOG.info(_("[VRM-CINDER] start delete_volumesnapshot()"))
        uri = '/volumesnapshots'
        method = 'DELETE'
        path = self.site_uri + uri + '/' + kwargs.get('id')
        body = {
            'volumeUrn': kwargs.get('vol_urn'),
            'snapshotUuid': kwargs.get('snapshot_uuid')}
        new_url = self._generate_url(path)

        number = self.number
        sleep_time = self.sleep_time
        while(number >= 0):
            try:
                resp, body = self.vrmhttpclient.request(new_url, method)
                task_urn_ = body.get('taskUrn')
                task_uri = body.get('taskUri')
                self.task_proxy.wait_task(task_uri=task_uri)
                break
            except driver_exception.ClientException as ex:
                LOG.info(_("[VRM-CINDER] delete volumesnapshot (%s)"), ex.errorCode)

                normal_errorcode = ['10300026', '10300057', '10430052', '10430054', '10430056', '10300421', '10300318', '10300162', '10300153', '10300170', '10000009', '10300421', '10420138']
                if ex.errorCode in normal_errorcode:
                    LOG.error(_("[VRM-CINDER] snapshot status conflicts, try delete later."))
                    raise cinder_exception.SnapshotIsBusy(snapshot_name=kwargs.get('snapshot_uuid'))

                if ex.errorCode not in self.delete_retry:
                    raise ex
                LOG.info('[VRM-CINDER] The errorcode is in retry list, number:%d' % number)
                number -= 1
                if number < 0:
                    raise ex
                sleep(sleep_time)

    def create_volume_from_snapshot(self, **kwargs):
        '''create_volume_from_snapshot

            'createvolumefromsnapshot': ('POST',
                                         ('/volumesnapshots', None, "createvol", None),
                                         {},
                                         {'snapshotUuid': kwargs.get('uuid'),
                                          'volumeName': kwargs.get('name'),
                                          'volumeType': normal/share
                                          'volumeUuid': uuid
                                          'snapshotVolumeType': 0/1
                                         },
                                         True),
        '''
        LOG.info(_("[VRM-CINDER] start createvolumefromsnapshot()"))
        uri = '/volumesnapshots/createvol'
        method = 'POST'
        path = self.site_uri + uri
        snapshotVolumeType = 0
        if str(kwargs.get('full_clone')) == '0':
            snapshotVolumeType = 1
        body = {
            'snapshotUuid': kwargs.get('snapshot_uuid'),
            'volumeName': kwargs.get('volume_name'),
            'volumeType': kwargs.get('type'),
            'volumeUuid': kwargs.get('volume_uuid'),
            'snapshotVolumeType': snapshotVolumeType,
        }
        if kwargs.get('volume_size') is not None:
            body.update({'volumeSize': kwargs.get('volume_size')})
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))

        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        return body
