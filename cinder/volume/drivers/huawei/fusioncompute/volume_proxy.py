# vim: tabstop=4 shiftwidth=4 softtabstop=4

"""
[VRM DRIVER] VRM CLIENT.

"""
import json
from cinder import exception as cinder_exception
from cinder.i18n import _

from cinder.volume.drivers.huawei.vrm.base_proxy import BaseProxy
from cinder.volume.drivers.huawei.vrm import exception as driver_exception
from cinder.volume.drivers.huawei.vrm.task_proxy import TaskProxy

from oslo_log import log as logging

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


class VolumeProxy(BaseProxy):
    def __init__(self):
        super(VolumeProxy, self).__init__()
        self.task_proxy = TaskProxy()

    def filter_not_none_dict(self, src_dic):
        obj_dic = {}
        for index_dic in src_dic:
            if src_dic.get(index_dic) is not None:
                obj_dic.update({index_dic: src_dic.get(index_dic)})
        return obj_dic

    def query_volume(self, **kwargs):
        '''query_volume

                'query_volume': ('GET',
                                 ('/volumes', kwargs.get(self.RESOURCE_URI), None, kwargs.get('id')),
                                 {'limit': kwargs.get('limit'),
                                  'offset': kwargs.get('offset'),
                                  'scope': kwargs.get('scope')
                                 },
                                 {},
                                 False),
        '''
        LOG.info(_("[VRM-CINDER] start query_volume()"))
        uri = '/volumes'
        method = 'GET'
        path = self.site_uri + uri + '/' + kwargs.get('id')

        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method)
        return body

    def query_volume_replications(self, **kwargs):
        '''query_volume_replications

                'query_volume_replications': ('GET',
                                 ('/volumes/{volume_id}/action/replications', kwargs.get(self.RESOURCE_URI), None,
                                 kwargs.get('volume_id')),
                                 False),
        '''
        LOG.info(_("[VRM-CINDER] start query_volume_replications()"))
        uri = '/volumes'
        method = 'GET'
        path = self.site_uri + uri + '/' + kwargs.get('volume_id') + '/action/replications'

        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method)
        return body

    def list_volumes(self, **kwargs):
        '''list_volumes

                'list_volumes': ('GET',
                                 ('/volumes', kwargs.get(self.RESOURCE_URI), None, kwargs.get('id')),
                                 {'limit': kwargs.get('limit'),
                                  'offset': kwargs.get('offset'),
                                  'scope': kwargs.get('scope')
                                 },
                                 {},
                                 False),
        '''
        LOG.info(_("[VRM-CINDER] start query_volumesnapshot()"))
        uri = '/volumes/compatibility/discovery '
        method = 'GET'
        path = self.site_uri + uri

        offset = 0
        volumes = []
        volumes_map = {}  # use map to clean repeat
        while True:
            parames = {
                'limit': self.limit,
                'offset': offset,
                'scope': kwargs.get('scope'),
                'uuid': kwargs.get('uuid')
            }
            appendix = self._joined_params(parames)
            new_url = self._generate_url(path, appendix)
            resp, body = self.vrmhttpclient.request(new_url, method)
            total = int(body.get('total') or 0)

            if total > 0:
                res = body.get('volumes')
                for volume in res:
                    volumes_map[volume.get('uuid')] = volume
                volumes = volumes_map.values()
                volumes += res
                offset += len(res)
                if offset >= total or len(volumes) >= total or len(res) < self.limit:
                    break
                if offset > 5:
                    offset -= 5  # repeat query more 5 to ensure all data be collect when audit.
            else:
                break

        for index_volume in volumes:
            if index_volume.get('customProperties') is not None and index_volume.get('customProperties').get('external_uuid') is not None:
                index_volume["uuid"] = index_volume["customProperties"]["external_uuid"]
        return volumes

    def list_volumes_extend(self, **kwargs):
        '''list_volumes_extend

                'list_volumes_extend': ('GET',
                                 ('/volumes', kwargs.get(self.RESOURCE_URI), None, kwargs.get('id')),
                                 {'limit': kwargs.get('limit'),
                                  'offset': kwargs.get('offset'),
                                  'scope': kwargs.get('scope')
                                 },
                                 {},
                                 False),
        '''
        LOG.info(_("[VRM-CINDER] start query_volumesnapshot()"))
        uri = '/volumes/extend'
        method = 'GET'
        path = self.site_uri + uri

        offset = 0
        volumes = []
        volumes_map = {}  # use map to clean repeat
        while True:
            parames = {
                'limit': self.limit,
                'offset': offset,
                'scope': kwargs.get('scope'),
                'uuid':kwargs.get('uuid')
            }
            appendix = self._joined_params(parames)
            new_url = self._generate_url(path, appendix)
            resp, body = self.vrmhttpclient.request(new_url, method)
            total = int(body.get('total') or 0)
            if total > 0:
                res = body.get('volumes')
                for volume in res:
                    volumes_map[volume.get('uuid')] = volume
                volumes = volumes_map.values()
                volumes += res
                offset += len(res)
                if offset >= total or len(volumes) >= total or len(res) < self.limit:
                    break
                if offset > 5:
                    offset -= 5  # repeat query more 5 to ensure all data be collect when audit.
            else:
                break

        for index_volume in volumes:
            if index_volume.get('customProperties') is not None and index_volume.get('customProperties').get('external_uuid') is not None:
                index_volume["uuid"] = index_volume["customProperties"]["external_uuid"]

        return volumes

    def create_volume(self, **kwargs):
        '''create_volume

                'create_volume': ('POST',
                                  ('/volumes', None, None, None),
                                  {},
                                  {'name': kwargs.get('name'),
                                   'quantityGB': kwargs.get('quantityGB'),
                                   'datastoreUrn': kwargs.get('datastoreUrn'),
                                   'uuid': kwargs.get('uuid'),
                                   'isThin': kwargs.get('isThin'),
                                   'type': kwargs.get('type'),
                                   'indepDisk': kwargs.get('indepDisk'),
                                   'persistentDisk': kwargs.get('persistentDisk'),
                                   'volumeId': kwargs.get('volumeId'),
                                    'snapshotUuid': kwargs.get('snapshotUuid'),
                                   'imageUrl': kwargs.get('imageUrl'),
                                  },
                                  True),
        '''
        LOG.info(_("[VRM-CINDER] start create_volume()"))
        uri = '/volumes'
        method = 'POST'
        path = self.site_uri + uri
        new_url = self._generate_url(path)
        body = {
            'name': kwargs.get('name'),
            'quantityGB': kwargs.get('size'),
            'datastoreUrn': kwargs.get('ds_urn'),
            'uuid': kwargs.get('uuid'),
            'isThin': kwargs.get('is_thin'),
            'type': kwargs.get('type'),
            'indepDisk': kwargs.get('independent'),
            'customProperties': {"external_uuid": kwargs.get('uuid')}
        }
        support_pvscsi = kwargs.get('support_pvscsi', None)
        if support_pvscsi is not None:
            body.update({'pvscsiSupport': support_pvscsi})

        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        return body

    def delete_volume(self, **kwargs):
        '''delete_volume

                'delete_volume': ('DELETE',
                                  ('/volumes', kwargs.get(self.RESOURCE_URI), None, None),
                                  {},
                                  {},
                                  True),
        '''
        LOG.info(_("[VRM-CINDER] start delete_volume()"))
        uri = '/volumes'
        method = 'DELETE'
        path = kwargs.get('volume_uri')
        new_url = self._generate_url(path)
        error_busy = ['10300057', '10420005', '10420009', '10410154', '10420137', '10420138', '10430058']
        try:
            resp, body = self.vrmhttpclient.request(new_url, method)
            task_uri = body.get('taskUri')
            self.task_proxy.wait_task(task_uri=task_uri)
        except driver_exception.ClientException as ex:
            LOG.info(_("[VRM-CINDER] delete volume (%s)"), ex.errorCode)
            if ex.errorCode == "10420004":
                return
            elif ex.errorCode in error_busy:
                LOG.info(_("[VRM-CINDER] volume status conflicts, try delete later."))
                raise cinder_exception.VolumeIsBusy(message=ex.errorCode)
            else:
                raise ex

    def clone_volume(self, **kwargs):
        '''clone_volume

                        'clone_volume': ('POST',
                                 ('/volumes', None, kwargs.get('src_name'), 'action/copyVol'),
                                 {},
                                 {'destinationVolumeID': kwargs.get('dest_name')
                                 },
                                 True),
        '''
        LOG.info(_("[VRM-CINDER] start clone_volume()"))
        uri = '/volumes'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get('src_volume_id') + '/action/copyVol'
        body = {'dstVolUrn': kwargs.get('dest_volume_urn')}
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)

    def _copy_nfs_image_to_volume(self, **kwargs):
        '''_copy_nfs_image_to_volume

                'copy_image_to_volume': ('POST',
                                         ('/volumes/imagetovolume', None, None, None),
                                         {},
                                         {
                                             'volumePara': {
                                                 'quantityGB': kwargs.get('volume_size'),
                                                 'urn': kwargs.get('volume_urn')
                                             },
                                             'imagePara': {
                                                 'id': kwargs.get('image_id'),
                                                 'url': kwargs.get('image_location')
                                             },
                                             'location': kwargs.get('cluster_urn'),
                                             'needCreateVolume': False
                                         },
                                         True),
        '''
        LOG.info(_("[VRM-CINDER] start copy_image_to_volume()"))
        uri = '/volumes/imagetovolume'
        method = 'POST'
        path = self.site_uri + uri
        new_url = self._generate_url(path)
        body = {
            'volumePara': {
                'quantityGB': kwargs.get('volume_size'),
                'urn': kwargs.get('volume_urn')
            },
            'imagePara': {
                'id': kwargs.get('image_id'),
                'url': kwargs.get('image_location')
            },
            'location': kwargs.get('cluster_urn'),
            'needCreateVolume': False
        }
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)

    def _copy_volume_to_image(self, **kwargs):
        '''_copy_volume_to_image

                'copy_volume_to_image': ('POST',
                                         ('/volumes/volumetoimage', None, None, None),
                                         {},
                                         {
                                             'volumePara': {'urn': kwargs.get('volume_urn'),
                                                            'quantityGB': kwargs.get('volume_size')},
                                             'imagePara': {'id': kwargs.get('image_id'), 'url': kwargs.get('image_url')}
                                         },
                                         True),
        '''

        LOG.info(_("[VRM-CINDER] start stop_vm()"))
        uri = '/volumes/volumetoimage'
        method = 'POST'
        path = self.site_uri + uri
        new_url = self._generate_url(path)

        body = {
            'volumePara': {
                'urn': kwargs.get('volume_urn'),
                'quantityGB': kwargs.get('volume_size')},
            'imagePara': {
                'id': kwargs.get('image_id'),
                'url': kwargs.get('image_url')}
        }
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)

    def manage_existing(self, **kwargs):
        '''manage_existing

                'manage_existing': ('POST',
                                  ('/volumes', None, None, None),
                                  {},
                                  {'name': kwargs.get('name'),
                                   'quantityGB': kwargs.get('quantityGB'),
                                   'datastoreUrn': kwargs.get('datastoreUrn'),
                                   'uuid': kwargs.get('uuid'),
                                   'type': kwargs.get('type'),
                                   'indepDisk': kwargs.get('indepDisk'),
                                   'persistentDisk': kwargs.get('persistentDisk'),
                                   'volumeId': kwargs.get('volumeId'),
                                   'snapshotUuid': kwargs.get('snapshotUuid'),
                                   'imageUrl': kwargs.get('imageUrl'),
                                  },
                                  True),
        '''
        LOG.info(_("[VRM-CINDER] start create_volume()"))
        uri = '/volumes/registevol'
        method = 'POST'
        path = self.site_uri + uri
        new_url = self._generate_url(path)
        body = {
            'name': kwargs.get('name'),
            'quantityGB': kwargs.get('quantityGB'),
            'volInfoUrl': kwargs.get('volInfoUrl'),
            'uuid': kwargs.get('uuid'),
            'customProperties': {"external_uuid": kwargs.get('uuid')},
            'type': kwargs.get('type'),
            'maxReadBytes': kwargs.get('maxReadBytes'),
            'maxWriteBytes': kwargs.get('maxWriteBytes'),
            'maxReadRequest': kwargs.get('maxReadRequest'),
            'maxWriteRequest': kwargs.get('maxWriteRequest')}

        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        return body

    def unmanage(self, **kwargs):
        '''unmanage

                'unmanage': ('DELETE',
                                  ('/volumes?isOnlyDelDB=1', kwargs.get(self.RESOURCE_URI), None, None),
                                  {},
                                  {},
                                  True),
        '''
        LOG.info(_("[VRM-CINDER] start unmanage()"))
        method = 'DELETE'
        path = kwargs.get('volume_uri') + '?isOnlyDelDB=1'
        new_url = self._generate_url(path)
        try:
            resp, body = self.vrmhttpclient.request(new_url, method)
            task_uri = body.get('taskUri')
            self.task_proxy.wait_task(task_uri=task_uri)
        except driver_exception.ClientException as ex:
            LOG.info(_("[VRM-CINDER] unmanage volume (%s)"), ex.errorCode)
            if ex.errorCode == "10420004":
                return
            else:
                raise ex

    def migrate_volume(self, **kwargs):
        '''migrate_volume

            Post <vol_uri>/<volid>/action/migratevol HTTP/1.1
            Host https://<ip>:<port>
            Accept application/json;version=<version>; charset=UTF-8
            X-Auth-Token: <Authen_TOKEN>
            {
            'datastoreUrn':string,
            'speed': integer
            }
        '''
        LOG.info(_("[VRM-CINDER] start migrate_volume()"))
        uri = '/volumes'
        method = 'POST'
        mig_type = 1

        path = self.site_uri + uri + '/' + kwargs.get('volume_id') + '/action/migratevol'
        body = {'datastoreUrn': kwargs.get('dest_ds_urn'),
                'speed': kwargs.get('speed'),
                'migrateType': kwargs.get('migrate_type')}
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)

    def modify_volume(self, **kwargs):
        LOG.info(_("[VRM-CINDER] start modify_volume()"))
        uri = '/volumes'
        method = 'PUT'
        path = self.site_uri + uri + '/' + kwargs.get('volume_id')
        body = {}
        if kwargs.get('type') is not None:
            body.update({'type': kwargs.get('type')})
        if kwargs.get('name') is not None:
            body.update({'name': kwargs.get('name')})
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        if resp.status_code not in (200, 204):
            raise driver_exception.ClientException(101)

    def extend_volume(self, **kwargs):
        '''extend_volume

                'extend_volume': ('POST',
                                  (kwargs.get('volume_uri'),'/action/expandVol',  None, None),
                                  {},
                                  {},
                                  True),
        '''
        LOG.info(_("[VRM-CINDER] start extend_volume()"))
        method = 'POST'
        body = {'size': kwargs.get('size')}
        volume_uri = kwargs.get('volume_uri')

        path = volume_uri + '/action/expandVol'
        new_url = self._generate_url(path)

        try:
            resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))

            task_uri = body.get('taskUri')

            self.task_proxy.wait_task(task_uri=task_uri)
        except driver_exception.ClientException as ex:
            LOG.info(_("[VRM-CINDER] extend volume (%s)"), ex.errorCode)
            raise ex
        except Exception as ex:
            LOG.info(_("[VRM-CINDER] extend volume (%s)"), ex)
            raise ex

    def wait_task(self, **kwargs):
        task_uri = kwargs.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)

    def query_volume_by_custom_properties(self, **kwargs):
        '''query_volume_by_custom_properties

                'query_volume_by_custom_properties': ('POST',
                                  ('/volumes/queryby/custom-properties', None, None, None),
                                  {},
                                  {"condition": {"name": "external_uuid",
                                  "value": "93DF26A9-2D3B-43A6-A7D8-CF5E9D0DC17C"
                                  },
                                  "offset": 0,
                                  limit": 10
                                  },
                                  True),
        '''
        LOG.info(_("[VRM-CINDER] start query_volume_by_custom_properties()"))
        uri = '/volumes/queryby/custom-properties'
        method = 'POST'
        path = self.site_uri + uri
        new_url = self._generate_url(path)
        body = {
            'condition': {
                'name': 'external_uuid',
                'value': kwargs.get('external_uuid')
            }
        }
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        volumes = []
        total = int(body.get('total') or 0)
        if total > 0:
            res = body.get('volumes')
            volumes += res
            return volumes[0]
        return None

    def update_custom_properties(self, **kwargs):
        '''update_custom_properties

                'update_by_custom_properties': ('POST',
                                  ('/volumes/properties/custom', None, None, None),
                                  {},
                                  {"volumes": [
                                  'volumeUrn': kwargs.get('volumeUrn'),
                                  'customProperties': {
                                  'external_uuid': kwargs.get('external_uuid')
                                  }]
                                  },
                                  True),
        '''
        LOG.info(_("[VRM-CINDER] start modify_custom_properties()"))
        uri = '/volumes/properties/custom'
        method = 'POST'
        path = self.site_uri + uri
        new_url = self._generate_url(path)
        body = {
            'volumes': [
                {
                    'volumeUrn': kwargs.get('volume_urn'),
                    'customProperties': {
                        'external_uuid': kwargs.get('external_uuid')
                    }
                }
            ]
        }
        resp, body = self.vrmhttpclient.request(new_url, method, body=json.dumps(body))
        return body