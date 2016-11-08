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
import threading

from oslo_config import cfg
from oslo_log import log as logging

from cinder import exception
from cinder.i18n import _
from cinder.volume.drivers.huawei.vrm.base_proxy import BaseProxy
from cinder.volume.drivers.huawei.vrm.conf import FC_DRIVER_CONF
from cinder.volume.drivers.huawei.vrm.task_proxy import TaskProxy

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
CONF = cfg.CONF
vrm_template_lock = threading.Lock()


class VmProxy(BaseProxy):
    '''VmProxy

    VmProxy
    '''
    def __init__(self, *args, **kwargs):
        super(VmProxy, self).__init__()
        self.task_proxy = TaskProxy()

    def query_vm(self, **kwargs):
        '''Query VM

        'query_vm': ('GET',
                     ('/vms', None, kwargs.get('vm_id'), None),
                     {},
                     {},
                     False)
        '''
        LOG.info(_("[VRM-CINDER] start query_vm()"))
        uri = '/vms'
        method = 'GET'
        path = self.site_uri + uri + '/' + kwargs.get('vm_id')
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method)
        LOG.info(_("[VRM-CINDER] end ()"))
        return body

    def query_vm_by_uri(self, **kwargs):
        '''Query VM by Uri

        'query_vm': ('GET',
                     ('/vms', None, kwargs.get('vm_id'), None),
                     {},
                     {},
                     False)
        '''
        LOG.info(_("[VRM-CINDER] start query_vm()"))
        method = 'GET'
        path = kwargs.get('vm_uri')
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method)
        LOG.info(_("[VRM-CINDER] end ()"))
        return body

    def delete_vm(self, **kwargs):
        '''Delete VM

                'delete_vm': ('DELETE',
                              ('/vms', None, kwargs.get('vm_id'), None),
                              {},
                              {},
                              True),
        '''
        LOG.info(_("[VRM-CINDER] start delete_vm()"))
        uri = '/vms'
        method = 'DELETE'
        path = self.site_uri + uri + '/' + kwargs.get('vm_id')
        if kwargs.get('isReserveDisks') is not None and kwargs.get(
                'isReserveDisks') == 1:
            path = path + '?isReserveDisks=1'
        new_url = self._generate_url(path)
        resp, body = self.vrmhttpclient.request(new_url, method)
        task_uri = body.get('taskUri')
        if kwargs.get('isReserveDisks') is not None and kwargs.get(
                'isReserveDisks') == 1:
            self.task_proxy.wait_task(task_uri=task_uri, isShortQuery=1)
        else:
            self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end ()"))

    def detach_vol_from_vm(self, **kwargs):
        '''detach_vol_from_vm

         'detach_vol_from_vm': ('POST',
                               ('/vms', None, kwargs.get('vm_id'),
                               'action/detachvol'),
                               {},
                               {'volUrn': kwargs.get('volUrn')},
                               True),
        '''
        LOG.info(_("[VRM-CINDER] start detach_vol_from_vm()"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get(
            'vm_id') + '/action/detachvol'
        new_url = self._generate_url(path)
        body = {'volUrn': kwargs.get('volume_urn')}
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end ()"))

    def attach_vol_to_vm(self, **kwargs):
        '''attach_vol_to_vm

                'attach_vol_to_vm': ('POST',
                                       ('/vms', None, kwargs.get('vm_id'),
                                       'action/attachvol'),
                                       {},
                                       {'volUrn': kwargs.get('volUrn')},
                                       True),
        '''
        LOG.info(_("[VRM-CINDER] start attach_vol_to_vm()"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get(
            'vm_id') + '/action/attachvol'
        new_url = self._generate_url(path)
        body = {'volUrn': kwargs.get('volume_urn')}
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end attach_vol_to_vm()"))

    def stop_vm(self, **kwargs):
        '''stop_vm

                'stop_vm': ('POST',
                            ('/vms', None, kwargs.get('vm_id'), 'action/stop'),
                            {},
                            {'mode': kwargs.get('mode')},
                            True),
        '''
        LOG.info(_("[VRM-CINDER] start stop_vm()"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get('vm_id') + '/action/stop'
        new_url = self._generate_url(path)
        body = {'mode': kwargs.get('mode')}
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end ()"))

    def _combine_empty_vmConfig(self, **kwargs):
        '''_combine_empty_vmConfig

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start _combine_empty_vmConfig ()"))
        cpu_quantity = 2
        mem_quantityMB = 1024

        cpu = {'quantity': cpu_quantity}
        memory = {'quantityMB': mem_quantityMB}
        disks = []
        vmConfigBody = {
            'cpu': cpu,
            'memory': memory,
            'disks': disks,
        }
        LOG.info(_("[VRM-CINDER] _combine_empty_vmConfig end ()"))
        return vmConfigBody

    def _combine_vmConfig_4_import(self, **kwargs):
        '''_combine_vmConfig_4_import

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start _combine_vmConfig ()"))
        cpu_quantity = 2
        mem_quantityMB = 1024
        datastoreUrn = kwargs.get('ds_urn')
        if kwargs.get('volume_sequence_num') is None:
            kwargs['volume_sequence_num'] = 1
        link = kwargs.get('linkClone')
        disk_quantityGB = kwargs.get('volume_size')
        if link:
            uuid = ""
            if kwargs.get('quick_start') is True:
                disk_quantityGB = kwargs.get('image_size')
        else:
            uuid = kwargs.get("volume_id")

        thin = kwargs.get('is_thin')
        volumeUrn = kwargs.get('volume_urn')
        cpu = {'quantity': cpu_quantity}
        memory = {'quantityMB': mem_quantityMB}
        disks = [
            {
                'volumeUrn': volumeUrn,
                'datastoreUrn': datastoreUrn,
                'quantityGB': disk_quantityGB,
                'volType': 0,
                'sequenceNum': kwargs.get('volume_sequence_num'),
                'pciType': 'IDE',
                'volumeUuid': uuid,
                'isThin': thin,
                'isCoverData': True
            }
        ]
        vmConfigBody = {
            'cpu': cpu,
            'memory': memory,
            'disks': disks,
        }
        LOG.info(_("[VRM-CINDER] end ()"))
        return vmConfigBody

    def _combine_vmConfig_4_clone(self, **kwargs):
        '''_combine_vmConfig_4_clone

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start _combine_vmConfig ()"))
        cpu_quantity = 2
        mem_quantityMB = 1024
        datastoreUrn = kwargs.get('ds_urn')
        if kwargs.get('volume_sequence_num') is None:
            kwargs['volume_sequence_num'] = 1
        disk_quantityGB = kwargs.get('volume_size')
        link = kwargs.get('linkClone')
        if link:
            uuid = ""
        else:
            uuid = kwargs.get("volume_id")

        thin = kwargs.get('is_thin')
        cpu = {'quantity': cpu_quantity}
        memory = {'quantityMB': mem_quantityMB}
        disks = [
            {
                'datastoreUrn': datastoreUrn,
                'quantityGB': disk_quantityGB,
                'volType': 0,
                'sequenceNum': 1,
                'pciType': 'IDE',
                'volumeUuid': uuid,
                'isThin': thin
            }
        ]
        vmConfigBody = {
            'cpu': cpu,
            'memory': memory,
            'disks': disks,
        }
        LOG.info(_("[VRM-CINDER] end ()"))
        return vmConfigBody

    def _combine_vmConfig_4_export(self, **kwargs):
        '''_combine_vmConfig_4_export

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start _combine_vmConfig ()"))
        cpu_quantity = 2
        mem_quantityMB = 1024
        if kwargs.get('volume_sequence_num') is None:
            kwargs['volume_sequence_num'] = 1
        cpu = {'quantity': cpu_quantity}
        memory = {'quantityMB': mem_quantityMB}
        disks = [
            {
                'sequenceNum': kwargs.get('volume_sequence_num'),
            }
        ]
        vmConfigBody = {
            'cpu': cpu,
            'memory': memory,
            'disks': disks,
        }
        LOG.info(_("[VRM-CINDER] end ()"))
        return vmConfigBody

    def _combine_os_options(self, **kwargs):
        '''_combine_os_options

        :param kwargs:
        :return:
        '''
        osOptions = {
            'osType': 'Windows',
            'osVersion': 26
        }
        LOG.info(_("[VRM-CINDER] end ()"))
        return osOptions

    def clone_vm(self, **kwargs):
        '''clone_vm

        :param kwargs:
        :return:
        '''

        LOG.info(_("[VRM-CINDER] start clone_vm()"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get(
            'template_id') + '/action/clone'
        new_url = self._generate_url(path)

        linked_clone = kwargs.get('linked_clone')
        if linked_clone is None:
            linked_clone = False
        LOG.info(_("[VRM-CINDER] start clone_vm()"))
        body = {
            'name': 'cinder-vm-' + kwargs.get('volume_id'),
            'group': 'FSP',
            'description': 'cinder-driver-temp-vm',
            'autoBoot': 'false',
            'isLinkClone': linked_clone,
            'vmConfig': self._combine_vmConfig_4_clone(**kwargs),
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))

        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri, isShortQuery=1)
        LOG.info(_("[VRM-CINDER] end clone_vm()"))
        return body.get('urn')

    def import_vm_from_glance(self, **kwargs):
        '''import_vm_from_glance

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start import_vm_from_glance()"))
        uri = self.site_uri + '/vms/action/import'
        method = 'POST'
        new_url = self._generate_url(uri)
        link_nfs = kwargs.get('linkClone')
        if link_nfs:
            name = kwargs.get('image_id')
        else:
            name = kwargs.get('volume_id')

        is_template = kwargs.get("is_template")
        if is_template is None or is_template:
            template = 'true'
        else:
            template = 'false'

        if CONF.glance_host is None or str(CONF.glance_port) is None \
                or FC_DRIVER_CONF.glance_server_ip is None:
            raise exception.ParameterNotFound(param='glance_host or '
                                                    'glance_port or glance_ip')

        endpoint = CONF.glance_host + ":" + str(CONF.glance_port)
        token = kwargs.get('auth_token')
        serviceIp = FC_DRIVER_CONF.glance_server_ip
        body = {
            'name': 'cinder-vm-' + name,
            'group': 'FSP',
            'description': 'cinder-glance-vm',
            'autoBoot': 'false',
            'location': kwargs.get("cluster_urn"),
            'osOptions': self._combine_os_options(**kwargs),
            'protocol': "glance",
            'vmConfig': self._combine_vmConfig_4_import(**kwargs),
            'isTemplate': template,
            'glanceConfig': {
                'imageID': kwargs.get('image_id'),
                'endPoint': endpoint,
                'serverIp': serviceIp,
                'token': token}
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end import_vm_from_glance()"))
        return body.get('urn')

    def import_vm_from_uds(self, **kwargs):
        '''import_vm_from_uds

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start import_vm_from_uds()"))
        uri = self.site_uri + '/vms/action/import'
        method = 'POST'
        new_url = self._generate_url(uri)
        link_nfs = kwargs.get('linkClone')
        if link_nfs:
            name = kwargs.get('image_id')
        else:
            name = kwargs.get('volume_id')

        is_template = kwargs.get("is_template")
        if is_template is None or is_template:
            template = 'true'
        else:
            template = 'false'

        if FC_DRIVER_CONF.s3_store_access_key_for_cinder is None or \
                FC_DRIVER_CONF.s3_store_secret_key_for_cinder is None:
            LOG.error(_("[VRM-CINDER] some params is None, please check: "
                        "s3_store_access_key_for_cinder, "
                        "s3_store_secret_key_for_cinder"))
            raise exception.ParameterNotFound(
                param='s3_store_access_key_for_cinder or '
                      's3_store_secret_key_for_cinder')

        uds_name = FC_DRIVER_CONF.s3_store_access_key_for_cinder
        uds_password = FC_DRIVER_CONF.s3_store_secret_key_for_cinder
        location = kwargs.get('image_location')
        location = location.split(":")
        if len(location) != 4:
            msg = _('image_location is invalid')
            LOG.error(msg)
            raise exception.ImageUnacceptable(image_id=kwargs.get('image_id'),
                                              reason=msg)
        serverIp = location[0].strip()
        port = location[1].strip()
        bucketName = location[2].strip()
        key = location[3].strip()

        body = {
            'name': 'cinder-vm-' + name,
            'group': 'FSP',
            'description': 'cinder-uds-vm',
            'autoBoot': 'false',
            'location': kwargs.get("cluster_urn"),
            'osOptions': self._combine_os_options(**kwargs),
            'protocol': "uds",
            'vmConfig': self._combine_vmConfig_4_import(**kwargs),
            'isTemplate': template,
            's3Config': {
                'serverIp': serverIp,
                'port': port,
                'accessKey': uds_name,
                'secretKey': uds_password,
                'bucketName': bucketName,
                'key': key
            }
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end import_vm_from_uds()"))
        return body.get('urn')

    def import_vm_from_nfs(self, **kwargs):
        '''import_vm_from_nfs

            'import_vm_from_image': ('POST',
                 ('/vms/action/import', None, None, None),
                 {},
                 dict({
                     'name': 'name',
                     'location': kwargs.get('cluster_urn'),
                     'autoBoot': 'false',
                     'url': kwargs.get('url'),
                     'protocol': 'nfs',
                     'vmConfig': {
                         'cpu': {
                             'quantity': 1
                         },
                         'memory': {
                             'quantityMB': 1024
                         },
                         'disks': [
                             {
                                 'pciType': 'IDE',
                                 'datastoreUrn': kwargs.get('ds_urn'),
                                 'quantityGB': kwargs.get('vol_size'),
                                 'volType': 0,
                                 'sequenceNum': 1,
                             },
                         ],
                     },
                     'osOptions': {
                         'osType': 'Windows',
                         'osVersion': 32
                     }
                 }),
                 True),
        '''

        LOG.info(_("[VRM-CINDER] start import_vm_from_nfs()"))
        uri = self.site_uri + '/vms/action/import'
        method = 'POST'
        new_url = self._generate_url(uri)
        link_nfs = kwargs.get('linkClone')
        if link_nfs:
            name = kwargs.get('image_id')
        else:
            name = kwargs.get('volume_id')

        is_template = kwargs.get("is_template")
        if is_template is None or is_template:
            template = 'true'
        else:
            template = 'false'

        body = \
            {
                'name': 'cinder-vm-' + name,
                'group': 'FSP',
                'description': 'cinder-nfs-vm',
                'autoBoot': 'false',
                'location': kwargs.get("cluster_urn"),
                'osOptions': self._combine_os_options(**kwargs),
                'protocol': "nfs",
                'vmConfig': self._combine_vmConfig_4_import(**kwargs),
                'url': kwargs.get('image_location'),
                'isTemplate': template
            }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end import_vm_from_nfs()"))
        return body.get('urn')

    def create_volume_from_extend(self, **kwargs):
        '''create_volume_from_extend

        create_linkclone_volume

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start create_volume_from_extend()"))
        image_type = kwargs.get('image_type')

        if image_type == "nfs":
            LOG.info(_("[VRM-CINDER] start create_volume_from_nfs"))
            vm_urn = self.import_vm_from_nfs(**kwargs)
        elif image_type == 'uds':
            LOG.info(_("[VRM-CINDER] start create_volume_from_uds"))
            vm_urn = self.import_vm_from_uds(**kwargs)
        else:
            LOG.info(_("[VRM-CINDER] start create_volume_from_glance"))
            vm_urn = self.import_vm_from_glance(**kwargs)

        vm_id = vm_urn[-10:]
        vm = self.query_vm(vm_id=vm_id)
        vm_config = vm['vmConfig']
        disks = vm_config['disks']
        volume_urn = None
        if kwargs.get('volume_sequence_num') is None:
            kwargs['volume_sequence_num'] = 1
        for disk in disks:
            if int(disk['sequenceNum']) == int(kwargs['volume_sequence_num']):
                volume_urn = disk['volumeUrn']
                break
        if volume_urn is None:
            msg = (_("[VRM-CINDER] no available disk"))
            LOG.error(msg)
            self.delete_vm(vm_id=vm_id)
            raise exception.ImageUnacceptable(image_id=kwargs['image_id'],
                                              reason=msg)
        try:
            self.detach_vol_from_vm(vm_id=vm_id, volume_urn=volume_urn)
        except Exception as ex:
            LOG.error(_('detach volume is failed'))
            self.delete_vm(vm_id=vm_id)
            raise ex

        self.delete_vm(vm_id=vm_id)
        LOG.info(_("[VRM-CINDER] end ()"))
        return volume_urn

    def check_template_status(self, vm_id):
        LOG.info(_("[VRM-CINDER] start check template status()"))
        uri = self.site_uri + '/vms/' + vm_id + '/competition'
        method = 'GET'
        new_url = self._generate_url(uri)
        resp, body = self.vrmhttpclient.request(new_url, method)
        LOG.info(_("[VRM-CINDER] template info %s") % body)
        body.get('status')

    def check_template(self, **kwargs):
        '''check_template

        :param kwargs:
        :return:
        '''
        template_id = kwargs.get('template_id')
        vm_name = kwargs.get('vm_name')
        templates = self.get_templates()
        id = None
        for template in templates:
            if template_id is not None:
                urn = template.get('urn')
                id = urn[-10:]
                if id == template_id:
                    LOG.info(_("[VRM-CINDER] template exists [%s]"),
                             template_id)
                    return id

            if vm_name is not None:
                name = template.get('name')
                if name == vm_name:
                    urn = template.get('urn')
                    id = urn[-10:]
                    LOG.info(_("[VRM-CINDER] vm is exists [%s]"), vm_name)
                    return id
        return id

    def create_volume_from_template(self, **kwargs):
        '''create_volume_from_template

        create_linkclone_volume

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start create_volume_from_template()"))
        template = kwargs['image_location']
        kwargs['template_id'] = template[-10:]
        is_exist = self.check_template(**kwargs)
        if is_exist is None:
            msg = (_("[VRM-CINDER] no such template %s "),
                   kwargs.get('template_id'))
            raise exception.ImageUnacceptable(image_id=kwargs['image_id'],
                                              reason=msg)

        while True:
            template_vm = self.query_vm(vm_id=kwargs['template_id'])
            LOG.info(_("[VRM-CINDER] template_vm status is %s"),
                     template_vm.get('status'))
            if 'creating' == template_vm.get('status'):
                sleep(10)
            elif 'stopped' == template_vm.get('status'):
                break
            else:
                msg = (_("[VRM-CINDER] template isn't available %s "),
                       kwargs.get('template_id'))
                LOG.error(msg)
                raise exception.ImageUnacceptable(image_id=kwargs['image_id'],
                                                  reason=msg)

        vm_config = template_vm['vmConfig']
        template_disks = vm_config['disks']
        if len(template_disks) != 1:
            msg = _("template must have one disk")
            LOG.error(msg)
            raise exception.ImageUnacceptable(image_id=kwargs['image_id'],
                                              reason=msg)

        vm_urn = self.clone_vm(**kwargs)
        vm_id = vm_urn[-10:]
        vm = self.query_vm(vm_id=vm_id)
        vm_config = vm['vmConfig']
        disks = vm_config['disks']
        volume_urn = None
        if kwargs.get('volume_sequence_num') is None:
            kwargs['volume_sequence_num'] = 1
        for disk in disks:
            if int(disk['sequenceNum']) == int(kwargs['volume_sequence_num']):
                volume_urn = disk['volumeUrn']
                break
        if volume_urn is None:
            msg = (_("[VRM-CINDER] no available disk"))
            LOG.error(msg)
            self.delete_vm(vm_id=vm_id)
            raise exception.ImageUnacceptable(image_id=kwargs['image_id'],
                                              reason=msg)
        try:
            self.delete_vm(vm_id=vm_id, isReserveDisks=1)
        except Exception as ex:
            LOG.error(_("delete vm(reserveDisk) is failed "))
            self.delete_vm(vm_id=vm_id)
            raise ex
        LOG.info(_("[VRM-CINDER] end ()"))
        return volume_urn

    def create_linkclone_from_template(self, **kwargs):
        '''create_linkclone_from_template

        create_linkclone_volume

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start create_linkclone_from_template()"))
        kwargs['linked_clone'] = True
        return self.create_volume_from_template(**kwargs)

    def create_linkClone_from_extend(self, **kwargs):
        '''create_linkClone_from_extend

        :param kwargs:
        :return:
        '''
        LOG.debug(_("[VRM-CINDER] start create_linkClone_volume()"))
        vrm_template_lock.acquire()

        try:
            kwargs['linkClone'] = True
            vm_name = 'cinder-vm-' + kwargs.get('image_id')
            LOG.info(_("[VRM-CINDER] vm_name is %s"), vm_name)
            kwargs['vm_name'] = vm_name
            vm_id = self.check_template(**kwargs)
            kwargs.pop('vm_name')
            LOG.info(_("[VRM-CINDER] vm_id is %s"), vm_id)
            image_type = kwargs.get('image_type')
            if kwargs.get('volume_sequence_num') is None:
                kwargs['volume_sequence_num'] = 1
            if 1 < int(kwargs['volume_sequence_num']):
                msg = (_("[VRM-CINDER] volume_sequence_num is %s "),
                       kwargs['volume_sequence_num'])
                LOG.error(msg)
                raise exception.ImageUnacceptable(image_id=kwargs['image_id'],
                                                  reason=msg)

            if vm_id is None:
                kwargs["is_template"] = True
                if image_type == 'nfs':
                    vm_urn = self.import_vm_from_nfs(**kwargs)
                    kwargs.pop('linkClone')
                    LOG.info(_("[VRM-CINDER] import_vm_from_nfs vm_urn is %s"),
                             vm_urn)
                    vm_id = vm_urn[-10:]
                    kwargs['image_location'] = vm_id

                elif image_type == 'uds':
                    vm_urn = self.import_vm_from_uds(**kwargs)
                    kwargs.pop('linkClone')
                    LOG.info(_("[VRM-CINDER] import_vm_from_uds vm_urn is %s"),
                             vm_urn)
                    vm_id = vm_urn[-10:]
                    kwargs['image_location'] = vm_id

                else:
                    vm_urn = self.import_vm_from_glance(**kwargs)
                    kwargs.pop('linkClone')
                    LOG.info(
                        _("[VRM-CINDER] import_vm_from_glance vm_urn is %s"),
                        vm_urn)
                    vm_id = vm_urn[-10:]
                    kwargs['image_location'] = vm_id

            else:
                kwargs.pop('linkClone')
                kwargs['image_location'] = vm_id
        except Exception as ex:
            vrm_template_lock.release()
            raise ex

        vrm_template_lock.release()
        return self.create_linkclone_from_template(**kwargs)

    def get_templates(self, **kwargs):
        '''get_templates

        'list_templates': ('GET',
                   ('/vms', None, None, None),
                   {'limit': kwargs.get('limit'),
                    'offset': kwargs.get('offset'),
                    'scope': kwargs.get('scope'),
                    'isTemplate': 'true'
                   },
                   {},
                   False),
        '''

        LOG.info(_("[VRM-CINDER] start _get_templates()"))
        uri = '/vms'
        method = 'GET'
        path = self.site_uri + uri

        offset = 0
        templates = []
        while True:
            parames = {
                'limit': self.limit,
                'offset': offset,
                'scope': kwargs.get('scope'),
                'isTemplate': 'true'
            }
            appendix = self._joined_params(parames)
            new_url = self._generate_url(path, appendix)
            resp, body = self.vrmhttpclient.request(new_url, method)
            total = int(body.get('total') or 0)
            if total > 0:
                res = body.get('vms')
                templates += res
                offset += len(res)
                if offset >= total or len(templates) >= total or len(
                        res) < self.limit:
                    break
            else:
                break

        LOG.info(_("[VRM-CINDER] end ()"))
        return templates

    def create_vm(self, **kwargs):
        '''create_vm

        :param kwargs:
        :return:
        '''

        LOG.info(_("[VRM-CINDER] start create_vm()"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri
        new_url = self._generate_url(path)

        body = {
            'name': 'cinder-driver-temp-' + kwargs.get('volume_id'),
            'group': 'FSP',
            'description': 'cinder-driver-temp-vm',
            'autoBoot': 'false',
            'location': kwargs.get("cluster_urn"),
            'vmConfig': self._combine_empty_vmConfig(**kwargs),
            'osOptions': self._combine_os_options(**kwargs)
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))

        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end create_vm()"))
        return body

    def export_vm_to_glance(self, **kwargs):
        '''export_vm_to_glance

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start export_vm_to_glance"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get(
            'vm_id') + '/action/export'
        new_url = self._generate_url(path)
        if CONF.glance_host is None or str(CONF.glance_port) is None \
                or FC_DRIVER_CONF.glance_server_ip is None:
            raise exception.ParameterNotFound(param='glance_host or '
                                                    'glance_port or glance_ip')

        endpoint = CONF.glance_host + ":" + str(CONF.glance_port)
        token = kwargs.get('auth_token')
        serviceIp = FC_DRIVER_CONF.glance_server_ip
        
        format = 'xml' if FC_DRIVER_CONF.export_version == 'v1.2' else 'ovf'
        body = {
            'name': kwargs.get('image_id'),
            'format': format,
            'protocol': 'glance',
            'isOverwrite': 'false',
            'vmConfig': self._combine_vmConfig_4_export(**kwargs),
            'glanceConfig': {
                'imageID': kwargs.get('image_id'),
                'endPoint': endpoint,
                'serverIp': serviceIp,
                'token': token}
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end export_vm_to_glance"))
        return body.get('urn')

    def export_vm_to_uds(self, **kwargs):
        '''export_vm_to_uds

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start export_vm_to_uds"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get(
            'vm_id') + '/action/export'
        new_url = self._generate_url(path)

        if FC_DRIVER_CONF.s3_store_access_key_for_cinder is None \
                or FC_DRIVER_CONF.s3_store_secret_key_for_cinder is None \
                or FC_DRIVER_CONF.uds_port is None or FC_DRIVER_CONF.uds_ip \
                is None \
                or FC_DRIVER_CONF.uds_bucket_name is None:
            LOG.error(_("[VRM-CINDER] some params is None, please check: "
                        "s3_store_access_key_for_cinder, "
                        "s3_store_secret_key_for_cinder, uds_port, uds_ip, "
                        "uds_bucket_name"))
            raise exception.ParameterNotFound(
                param='s3_store_access_key_for_cinder '
                      'or s3_store_secret_key_for_cinder or'
                      'uds_port or uds_serverIp or uds_bucket_name')

        uds_name = FC_DRIVER_CONF.s3_store_access_key_for_cinder
        uds_password = FC_DRIVER_CONF.s3_store_secret_key_for_cinder
        port = FC_DRIVER_CONF.uds_port
        serverIp = FC_DRIVER_CONF.uds_ip
        bucketName = FC_DRIVER_CONF.uds_bucket_name
        key = kwargs.get('image_id')
        bucket_type = FC_DRIVER_CONF.uds_bucket_type
        if bucket_type is not None:
            if str(bucket_type) == 'wildcard':
                if kwargs.get('project_id') is None:
                    LOG.error(_("project_id is none "))
                    raise exception.ParameterNotFound(
                        param='project_id is none')
                else:
                    bucketName += kwargs.get('project_id')

        LOG.info(_("[VRM-CINDER] bucketName is %s"), bucketName)
                      
        format = 'xml' if FC_DRIVER_CONF.export_version == 'v1.2' else 'ovf'
        body = {
            'name': kwargs.get('image_id'),
            'format': format,
            'protocol': 'uds',
            'isOverwrite': 'false',
            'vmConfig': self._combine_vmConfig_4_export(**kwargs),
            's3Config': {
                'serverIp': serverIp,
                'port': port,
                'accessKey': uds_name,
                'secretKey': uds_password,
                'bucketName': bucketName,
                'key': key}
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))
        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end export_vm_to_uds"))
        return body.get('urn')

    def export_vm_to_nfs(self, **kwargs):
        '''export_vm_to_nfs

            Post <vm_uri>/<id>/action/export HTTP/1.1
            Host: https://<ip>:<port>
            Content-Type: application/json; charset=UTF-8
            Accept: application/json;version=<version>; charset=UTF-8
            X-Auth-Token: <Authen_TOKEN>
            {
            "name":string,
            "url:string,
            "username":administrator
            "password":string
            }
        '''

        LOG.info(_("[VRM-CINDER] start export_vm_to_nfs()"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get(
            'vm_id') + '/action/export'
        new_url = self._generate_url(path)

        format = 'xml' if FC_DRIVER_CONF.export_version == 'v1.2' else 'ovf'
        body = {
            'name': kwargs.get('image_id'),
            'url': kwargs.get('image_url') + '/' + kwargs.get('image_id'),
            'vmConfig': self._combine_vmConfig_4_export(**kwargs),
            'format': format,
            'protocol': 'nfs',
            'isOverwrite': 'false'
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))

        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end export_vm_to_nfs()"))
        return body.get('urn')

    def get_volume_sequence_num(self, vm_body, volume_urn):
        volume_sequence_num = 1
        if vm_body is not None:
            vm_uri = vm_body.get('uri')
            volume_body = self.query_vm(vm_id=vm_uri[-10:])
            if volume_body is not None:
                for item in volume_body.get('vmConfig').get('disks'):
                    if item.get('volumeUrn') == volume_urn:
                        volume_sequence_num = item.get("sequenceNum")
        else:
            LOG.info(_("[VRM-CINDER] vm_body is null)"))
        return volume_sequence_num

    def export_volume_to_image(self, **kwargs):
        '''export_volume_to_image

        :param kwargs:
        :return:
        '''
        LOG.info(_("[VRM-CINDER] start export_volume_to_image()"))
        vm_body = self.query_vm_volume(**kwargs)
        if vm_body is not None:
            kwargs['volume_sequence_num'] = self.get_volume_sequence_num(
                vm_body,
                kwargs.get('volume_urn'))
            vm_uri = vm_body.get('uri')
            kwargs['vm_id'] = vm_uri[-10:]
            LOG.info(_("[VRM-CINDER] volume is already attached"))
            self.export_attached_volume_to_image(**kwargs)
            LOG.info(_("[VRM-CINDER] volume_sequence_num %s"),
                     kwargs['volume_sequence_num'])
            return kwargs['volume_sequence_num']

        vm_body = self.create_vm(**kwargs)
        vm_uri = vm_body.get('uri')

        kwargs['vm_id'] = vm_uri[-10:]

        if kwargs.get('shareable') == 'share':
            kwargs['volume_sequence_num'] = 2
        else:
            kwargs['volume_sequence_num'] = 1

        try:
            self.attach_vol_to_vm(**kwargs)
        except Exception as ex:
            LOG.error(_("[VRM-CINDER] attach volume is error "))
            self.delete_vm(**kwargs)
            raise ex
        try:
            self.export_attached_volume_to_image(**kwargs)
        except Exception as ex:
            LOG.error(_("[VRM-CINDER]  export vm is error "))
            self.detach_vol_from_vm(**kwargs)
            self.delete_vm(**kwargs)
            raise ex

        self.detach_vol_from_vm(**kwargs)

        self.delete_vm(**kwargs)

        LOG.info(_("[VRM-CINDER] end export_volume_to_nfs()"))
        LOG.info(_("[VRM-CINDER] volume_sequence_num %s"),
                 kwargs['volume_sequence_num'])
        return kwargs['volume_sequence_num']

    def export_attached_volume_to_image(self, **kwargs):
        image_type = kwargs.get('image_type')
        if image_type == 'nfs':
            LOG.info(_('[VRM-CINDER]  export_vm_to_nfs'))
            self.export_vm_to_nfs(**kwargs)
        elif image_type == 'uds':
            LOG.info(_('VRM-CINDER]  export_vm_to_uds'))
            self.export_vm_to_uds(**kwargs)
        else:
            LOG.info(_('VRM-CINDER]  export_vm_to_glance'))
            self.export_vm_to_glance(**kwargs)

        LOG.info(_("[VRM-CINDER] end export_attached_volume_to_image()"))

    def query_vm_volume(self, **kwargs):
        '''query_vm_volume

         URL:https://192.168.106.111:8443/OmsPortal/service/sites/4286080F/vms
         ?scope=urn%3Asites%3A4286080F%3Avolumes%3A137
         &detail=2
        '''

        LOG.info(_("[VRM-CINDER] start query_vm_volume()"))
        uri = '/vms'
        method = 'GET'
        path = self.site_uri + uri
        parames = {
            'scope': kwargs.get('volume_urn'),
            'detail': 0
        }
        appendix = self._joined_params(parames)
        new_url = self._generate_url(path, appendix)
        resp, body = self.vrmhttpclient.request(new_url, method)
        total = int(body.get('total') or 0)
        vm = None
        if total >= 1:
            vms = body.get('vms')
            vm = vms[0]
        else:
            LOG.info(_("[VRM-CINDER] find no vms()"))
        LOG.info(_("[VRM-CINDER] end query_vm_volume()"))
        return vm

    def migrate_vm_volume(self, **kwargs):
        '''migrate_vm_volume

            Post /<vm_uri>/<vm_id>/action/migratevol HTTP/1.1
            Host: https://<ip>:<port>
            Content-Type: application/json; charset=UTF-8
            Accept: application/json;version=<version>; charset=UTF-8
            X-Auth-Token: <Authen_TOKEN>
            {
            "disks":[
            {
            "volumeUrn":string
            "datastoreUrn":string //urn:sites:1:datastores:1
            }
            ],
            "speed": integer
            }
        '''

        LOG.info(_("[VRM-CINDER] start migrate_vm_volume()"))
        uri = '/vms'
        method = 'POST'
        path = self.site_uri + uri + '/' + kwargs.get(
            'vm_id') + '/action/migratevol'
        new_url = self._generate_url(path)

        body = {
            "disks": [
                {
                    "volumeUrn": kwargs.get('volume_urn'),
                    "datastoreUrn": kwargs.get('dest_ds_urn'),
                    'migrateType': kwargs.get('migrate_type')
                }
            ],
            "speed": kwargs.get('speed')
        }
        resp, body = self.vrmhttpclient.request(new_url, method,
                                                body=json.dumps(body))

        task_uri = body.get('taskUri')
        self.task_proxy.wait_task(task_uri=task_uri)
        LOG.info(_("[VRM-CINDER] end migrate_vm_volume()"))
        return body.get('urn')
