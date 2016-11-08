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

import json
import math
import os
import random

from cinder import context as cinder_context
from cinder import exception
from cinder.i18n import _
from cinder import volume
from cinder.volume import driver
from cinder.volume.drivers.huawei.vrm.cluster_proxy import ClusterProxy
from cinder.volume.drivers.huawei.vrm.conf import FC_DRIVER_CONF
from cinder.volume.drivers.huawei.vrm.datastore_proxy import DatastoreProxy
from cinder.volume.drivers.huawei.vrm import exception as driver_exception
from cinder.volume.drivers.huawei.vrm.host_proxy import HostProxy
from cinder.volume.drivers.huawei.vrm.http_client import VRMHTTPClient
from cinder.volume.drivers.huawei.vrm.vm_proxy import VmProxy
from cinder.volume.drivers.huawei.vrm.volume_proxy import VolumeProxy
from cinder.volume.drivers.huawei.vrm.volume_snapshot_proxy import \
    VolumeSnapshotProxy
from cinder.volume import utils as volume_utils

from oslo_config import cfg
from oslo_log import log as logging

backend_opts = [
    cfg.StrOpt('volume_driver',
               default='cinder.volume.drivers.huawei.vrm.vrm_driver.VRMDriver',
               help='Driver to use for volume creation'),
]


def metadata_to_dict(metadata):
    result = {}
    for item in metadata:
        if not item.get('deleted'):
            result[item['key']] = item['value']
    return result


LOG = logging.getLogger(__name__)
CONF = cfg.CONF

fc_plugin_conf = [
    cfg.StrOpt('vrm_config_file',
               default=None,
               help='vrm_config_file'),
    cfg.BoolOpt('vrm_thin_provision',
                default=False,
                help='Switch of thin provisioning support'),
    cfg.FloatOpt('vrm_over_ratio',
                 default=1.0,
                 help='Ratio of thin provisioning'),
    cfg.IntOpt('vrm_reserved_percentage',
               default=0,
               help='Reserved percentage of the backend volumes'),
    cfg.ListOpt('vrm_ds_name',
                default=[],
                help='vrm_ds_name'),
    cfg.ListOpt('current_list',
                default=[],
                help='current_list'),
    cfg.IntOpt('affine_rate',
               default=1,
               help='affine_rate'),
]


class VRMDriver(driver.VolumeDriver):
    VENDOR = 'Huawei'
    BACKEND = 'VRM'
    VERSION = 'v1.1'

    def __init__(self, *args, **kwargs):
        '''__init__

        __init__

        :param args:
        :param kwargs:
        :return:
        '''
        super(VRMDriver, self).__init__(*args, **kwargs)
        LOG.info(_("[VRM-CINDER] start VRMDriver __init__()"))

        self.context = None
        self.volume_api = volume.API()
        self.SHARED_HOSTS = []
        self.SHARED_DATASTORES = []
        self.SHARED_VOLUMES = []

        self.LAST_SHARED_HOSTS = self.SHARED_HOSTS
        self.LAST_SHARED_DATASTORES = self.SHARED_DATASTORES
        self.LAST_SHARED_VOLUMES = self.SHARED_VOLUMES
        self.left_periodrate = CONF.vrm_sm_periodrate

        if self.configuration:
            LOG.info(_("[VRM-CINDER] append configuration"))
            self.configuration.append_config_values(fc_plugin_conf)
        else:
            LOG.info(_("[VRM-CINDER] no configuration exception"))
            raise driver_exception.NoNeededData

        over_ratio = self.configuration.get('vrm_over_ratio')
        if over_ratio is None:
            self.over_ratio = 1.0
        else:
            LOG.info(_("[VRM-CINDER] super_ratio [%s]"), over_ratio)
            self.over_ratio = over_ratio

        thin_provision = self.configuration.vrm_thin_provision
        if thin_provision is not None:
            self.thin_provision = False
        else:
            LOG.info(_("[VRM-CINDER] thin_provision [%s]"), thin_provision)
            self.thin_provision = bool(thin_provision)

        reserved_percentage = self.configuration.get('vrm_reserved_percentage')
        if reserved_percentage is None:
            self.reserved_percentage = 0
        else:
            LOG.info(_("[VRM-CINDER] reserved_percentage [%s]"),
                     reserved_percentage)
            self.reserved_percentage = int(reserved_percentage)

        self.pool_list = self.configuration.get('vrm_ds_name')
        if not self.pool_list:
            LOG.error(_("[VRM-CINDER] vrm_ds_name is None exception"))
            raise driver_exception.NoNeededData
        self.pool_list = list(set(self.pool_list))

        self.current_list = self.configuration.get('current_list')
        self.affine_rate = self.configuration.get('affine_rate')

        self.volume_proxy = VolumeProxy()
        self.vm_proxy = VmProxy()
        self.volume_snapshot_proxy = VolumeSnapshotProxy()
        self.host_proxy = HostProxy()
        self.cluster_proxy = ClusterProxy()
        self.datastore_proxy = DatastoreProxy()
        self.vrmhttpclient = VRMHTTPClient()

        self.site_urn = None
        self.site_uri = None
        self.cluster_urn = None

        self.shared_hosts = []
        self.shared_datastores = []
        self.shared_volumes = []
        self.auth_token = None
        self.log_count = 0

        LOG.info(_("[VRM-CINDER] end __init__()"))

    def _get_host_datastore_vol(self):
        '''_get_host_datastore_vol

        get_host_datastore_vol

        :return:
        '''
        self.shared_hosts = []
        self.shared_datastores = []
        self.shared_volumes = []

        self.shared_hosts = self.host_proxy.list_host()
        hosturns = [host['urn'] for host in self.shared_hosts]
        hosturns.sort()
        hosturns_set = set(hosturns)

        datastores = self.datastore_proxy.list_datastore()
        sharetypes = [t.lower() for t in FC_DRIVER_CONF.vrm_ds_types]
        for datastore in datastores:
            storage_type = datastore['storageType'].lower()
            ds_name = datastore['name']
            if self.pool_list is not None:
                if ds_name in self.pool_list:
                    # LOG.info(_("[VRM-CINDER] get the vrm_sm_datastore_name"))
                    self.shared_datastores.append(datastore)
                else:
                    # LOG.info(_("vrm_sm_datastore_name useless"))
                    continue

            if storage_type in sharetypes:
                hosts = datastore['hosts']
                hosts.sort()
                hosts_set = set(hosts)

                # LOG.info(_("hosts_set %s") % hosts_set)
                if FC_DRIVER_CONF.vrm_ds_hosts_share:
                    if len(hosturns_set - hosts_set) == 0:
                        # LOG.info(_("[VRM-CINDER] append ds share"))
                        self.shared_datastores.append(datastore)
                else:
                    if len(hosturns_set & hosts_set) > 0:
                        LOG.info(_("[VRM-CINDER] append ds"))
                        self.shared_datastores.append(datastore)

        if len(self.shared_datastores) <= 0:
            LOG.info(_("[VRM-CINDER] can not found any shared datastores "))
            raise driver_exception.NotFound()

#        LOG.info(_("[VRM-CINDER] end get_host_datastore_vol()"))
        return (
            self.shared_hosts, self.shared_datastores, self.shared_volumes)

    def _refresh_storage_info(self, refresh=False):
        '''_refresh_storage_info

        _refresh_storage_info

        :param refresh:
        :return:
        '''
#        LOG.info(_("[BRM-DRIVER] start _refresh_storage_info(%s) "), refresh)
        if refresh is True:
            self.LAST_SHARED_HOSTS = self.SHARED_HOSTS
            self.LAST_SHARED_DATASTORES = self.SHARED_DATASTORES
            self.LAST_SHARED_VOLUMES = self.SHARED_VOLUMES

            self.SHARED_HOSTS, self.SHARED_DATASTORES, self.SHARED_VOLUMES = \
                self._get_host_datastore_vol()
            self.left_periodrate = CONF.vrm_sm_periodrate

            self.log_count = self.log_count + 1
            if self.log_count >= 20:
                LOG.info(_("[CINDER-BRM] refreshed shared hosts :[ %s ]"),
                         self.SHARED_HOSTS)
                LOG.info(_("[CINDER-BRM] refreshed shared datastores :[ %s ]"),
                         self.SHARED_DATASTORES)
                self.log_count = 0
#        LOG.info(_("[BRM-DRIVER] end _refresh_storage_info(%s) "), refresh)

    def do_setup(self, context):
        '''do_setup

        do_setup

        :param context:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start do_setup() "))
        LOG.info(_("[VRM-CINDER]  del environ http_proxy https_proxy"))
        if os.getenv('http_proxy'):
            LOG.info(_("[VRM-CINDER]  del environ http_proxy"))
            del os.environ['http_proxy']
        if os.getenv('https_proxy'):
            LOG.info(_("[VRM-CINDER]  del environ https_proxy"))
            del os.environ['https_proxy']
        self.context = context
        self.vrmhttpclient.init()
        self.site_urn = self.vrmhttpclient.get_siteurn()
        self._refresh_storage_info(True)
        clusters = self.cluster_proxy.list_cluster()
        self.cluster_urn = clusters[0].get('urn')
        LOG.info(_("[CINDER-BRM] end do_setup"))

    def check_for_setup_error(self):
        '''check_for_setup_error

        check_for_setup_error

        :return:
        '''
        # LOG.info(_("[BRM-DRIVER] start check_for_setup_error() "))
        if len(self.SHARED_HOSTS) == 0 or len(self.SHARED_DATASTORES) == 0:
            LOG.info(_(
                "[CINDER-BRM] check_for_setup_error, "
                "shared datasotre not found"))
            raise driver_exception.NoNeededData

    def _build_volume_stats(self):
        '''_build_volume_stats

        '''
        # LOG.info(_("[BRM-DRIVER] start _build_volume_stats() "))
        stats = {}
        stats["pools"] = []
        stats['driver_version'] = self.VERSION
        stats['storage_protocol'] = 'VRM'
        stats['vendor_name'] = self.VENDOR

        backend = self.configuration.get('volume_backend_name')
        if backend is None:
            stats['volume_backend_name'] = self.BACKEND
        else:
            stats['volume_backend_name'] = self.configuration.get(
                'volume_backend_name')

        return stats

    def _try_get_volume_stats(self, refresh=False):
        '''_try_get_volume_stats

        _try_get_volume_stats

        :param refresh:If 'refresh' is True, run the update first.
        :return:Return the current state of the volume service.
        '''
#        LOG.info(_("[BRM-DRIVER] start _try_get_volume_stats() "))
        if refresh:
            self.left_periodrate -= 1
            if self.left_periodrate <= 0:
                self._refresh_storage_info(refresh)

        stats = self._build_volume_stats()
        ds_meta = {}
        ds_names = [ds['name'] for ds in self.SHARED_DATASTORES]
        for pool in self.pool_list:
            if pool not in ds_names:
                continue
            new_pool = {}
            ds_meta['ds_name'] = pool
            datastore = self._choose_datastore(ds_meta)
            if datastore.get('storageType') == 'advanceSan' and datastore.get(
                    'version') is not None:
                new_pool.update(dict(consistencygroup_support=True))
            if 'NORMAL' != datastore['status']:
                new_pool.update(dict(
                    pool_name=pool,
                    free_capacity_gb=0,
                    reserved_percentage=self.reserved_percentage,
                    total_capacity_gb=0,
                    provisioned_capacity_gb=0,
                    max_over_subscription_ratio=self.over_ratio,
                    affine_rate=1
                ))
                stats["pools"].append(new_pool)
                continue
                
            if self.current_list is not None and pool in self.current_list:
                new_pool.update(dict(
                    pool_name=pool,
                    free_capacity_gb=datastore['freeSizeGB'],
                    reserved_percentage=self.reserved_percentage,
                    total_capacity_gb=datastore['capacityGB'],
                    provisioned_capacity_gb=datastore['usedSizeGB'],
                    max_over_subscription_ratio=self.over_ratio,
                    affine_rate=self.affine_rate
                ))
            else:
                new_pool.update(dict(
                    pool_name=pool,
                    free_capacity_gb=datastore['freeSizeGB'],
                    reserved_percentage=self.reserved_percentage,
                    total_capacity_gb=datastore['capacityGB'],
                    provisioned_capacity_gb=datastore['usedSizeGB'],
                    max_over_subscription_ratio=self.over_ratio,
                    affine_rate=1
                ))
            if self.thin_provision is True:
                new_pool.update(dict(
                    thin_provisioning_support=True,
                    thick_provisioning_support=False
                ))
            else:
                new_pool.update(dict(
                    thin_provisioning_support=False,
                    thick_provisioning_support=True
                ))
            tier_size = datastore.get('tierSize', None)
            type_v3 = []
            if tier_size and len(tier_size) >= 3:
                if tier_size[0] > 0:
                    type_v3.append('ssd')
                if tier_size[1] > 0:
                    type_v3.append('sas')
                if tier_size[2] > 0:
                    type_v3.append('nl_sas')
            if len(type_v3) > 0:
                type_v3_str = ';'.join(type_v3)
                LOG.info(_("[CINDER-BRM] type of v3 is %s"), type_v3_str)
                new_pool.update(dict(type=type_v3_str))
            stats["pools"].append(new_pool)

        # LOG.info(_("[CINDER-BRM] (%d)--%s"), self.left_periodrate, stats)

        return stats

    def get_volume_stats(self, refresh=False):
        '''get_volume_stats

        get_volume_stats

        :param refresh:If 'refresh' is True, run the update first.
        :return:Return the current state of the volume service.
        '''
#        LOG.info(_("[BRM-DRIVER] start get_volume_stats() "))
        try:
            stats = self._try_get_volume_stats(refresh)
        except Exception as ex:
            LOG.info(_("[CINDER-BRM] get volume stats Exception (%s)"), ex)
            stats = self._build_volume_stats()
        return stats

    def check_and_modify_thin(self, ds_urn, thin):
        '''check_and_modify_thin

        [ DSWARE] /[LOCAL, SAN, LUN]
        :param ds_urn:
        :param thin:
        :return:
        '''
        LOG.info(_("[CINDER-BRM] start check_and_modify_thin (%s)"), ds_urn)
        for datastore in self.SHARED_DATASTORES:
            # LOG.info(_("[CINDER-BRM] ds_urn (%s)"), ds_urn)
            LOG.info(_("[CINDER-BRM] datastore (%s)"), datastore['urn'])
            if datastore['urn'] == ds_urn:
                ds_type = str(datastore['storageType']).upper()
                LOG.info(_("[CINDER-BRM] ds_type (%s)"), ds_type)
                if ds_type in ['LOCAL', 'SAN', 'LUN']:
                    LOG.info(_("[CINDER-BRM] return False (%s)"), ds_urn)
                    return False
                if ds_type in ['DSWARE']:
                    LOG.info(_("[CINDER-BRM] return True (%s)"), ds_urn)
                    return True
        return thin

    def check_thin(self, datastore, thin):
        '''check_thin

        [ DSWARE] /[LOCAL, SAN, LUN]
        :param ds_urn:
        :param thin:
        :return:
        '''
        # LOG.info(_("[CINDER-BRM] start check_thin (%s)"), datastore)
        ds_type = str(datastore['storageType']).upper()
        # LOG.info(_("[CINDER-BRM] ds_type (%s)"), ds_type)
        if ds_type in ['LOCAL', 'SAN', 'LUN']:
            # LOG.info(_("[CINDER-BRM] return False (%s)"), datastore)
            return False
        if ds_type in ['DSWARE']:
            # LOG.info(_("[CINDER-BRM] return True (%s)"), datastore)
            return True
        return thin

    def _check_and_choice_datastore(self, ds_meta):
        '''_check_and_choice_datastore

        _check_and_choice_datastore

        :param ds_meta:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start _check_and_choice_datastore() "))
        datastoreUrn = ds_meta['datastoreUrn']
        hypervisorIp = ds_meta['hypervisorIp']
        quantityGB = ds_meta['quantityGB']
        storageType = ds_meta['storageType']
        isThin = ds_meta['isThin']
        hypervisorUrn = None
        if hypervisorIp:
            for host in self.SHARED_HOSTS:
                if host['ip'].strip() == hypervisorIp.strip():
                    hypervisorUrn = host['urn']
                    break
            if hypervisorUrn is None:
                LOG.info(_("[CINDER-BRM] can not found hypervisorip=%s"),
                         hypervisorIp)
                raise exception.HostNotFound(host=hypervisorIp)

        if datastoreUrn:
            for datastore in self.SHARED_DATASTORES:
                if datastore['urn'] == datastoreUrn:
                    this_storageType = datastore['storageType']
                    this_isThin = datastore['isThin']
                    this_freeSizeGB = int(datastore['freeSizeGB'])
                    this_hosts = datastore['hosts']

                    if this_storageType.lower() == storageType.lower() and str(
                            this_isThin).lower() == str(
                            isThin).lower() and this_freeSizeGB > quantityGB:
                        if hypervisorUrn is None:
                            return datastore['urn']
                        elif hypervisorUrn in this_hosts:
                            return datastore['urn']
                    raise driver_exception.NotFound()
            raise driver_exception.NotFound()
        ds_hosts = None
        ds_urn = None
        random.shuffle(self.SHARED_DATASTORES)
        for datastore in self.SHARED_DATASTORES:

            this_isThin = datastore['isThin']
            this_freeSizeGB = int(datastore['freeSizeGB'])
            ds_hosts = datastore['hosts']
            if this_freeSizeGB < quantityGB:
                continue
            if isThin is None:
                ds_urn = datastore['urn']
                break

            elif str(this_isThin).lower() == str(isThin).lower():
                ds_urn = datastore['urn']
                break

        if ds_urn is None:
            raise driver_exception.NotFound()
        random.shuffle(ds_hosts)
        host_urn = ds_hosts[0]
        return ds_urn, host_urn

    def _choose_datastore(self, ds_meta):
        '''_choose_datastore

        _check_and_choice_datastore

        :param ds_meta:
        :return:
        '''
        for datastore in self.SHARED_DATASTORES:
            if ds_meta['ds_name'] == datastore['name']:
                return datastore

        raise driver_exception.NotFound()

    def _vrm_pack_provider_location(self, volume_body):
        '''_vrm_pack_provider_location

        _vrm_pack_provider_location

        :param volume:
        :param volume_body:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start _vrm_pack_provider_location() "))
        fc_ip = FC_DRIVER_CONF.fc_ip

        provider_location = ""
        provider_location += ('addr=' + fc_ip + ':' + str(CONF.vrm_port) + ',')
        provider_location += ('uri=' + volume_body.get('uri') + ',')
        provider_location += ('urn=' + volume_body.get('urn') + ',')
        provider_location += \
            ('datastoreUrn=' + volume_body.get('datastoreUrn') + ',')
        provider_location += ('isThin=' + str(volume_body.get('isThin')) + ',')
        provider_location += \
            ('storageType=' + volume_body.get('storageType') + ',')
        provider_location += ('type=' + volume_body.get('type'))

        return provider_location

    def _vrm_unpack_provider_location(self, provider_location, key=None):
        '''_vrm_unpack_provider_location

        :param provider_location:
        :param key:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start _vrm_unpack_provider_location() "))
        kvalue = None
        kvs = {}
        if not isinstance(provider_location, None) and len(
                provider_location) > 0:
            items = provider_location.split(',')
            for item in items:
                (ki, eqi, vi) = item.partition('=')
                kvs[ki] = vi
                if key and key == ki:
                    kvalue = vi

        return kvalue, kvs

    def _vrm_get_volume_meta(self, id):
        '''_vrm_create_volume

        :param id:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start _vrm_get_volume_meta() "))
        model_update = {}
        metadata = {}

        if id:
            volume_body = self.volume_proxy.query_volume(id=id)
            if volume_body is not None:
                model_update[
                    'provider_location'] = self._vrm_pack_provider_location(
                    volume_body)
                sd_sn = volume_body.get('storageSN', None)
                lun_id = volume_body.get('lunId', None)
                replication_driver_data = {}
                if lun_id:
                    LOG.info(_("[BRM-DRIVER] lun id is %s"), lun_id)
                    replication_driver_data.update({'lun_id': lun_id})
                if sd_sn:
                    LOG.info(_("[BRM-DRIVER] sn is %s"), sd_sn)
                    replication_driver_data.update({'sn': sd_sn})
                if len(replication_driver_data) > 0:
                    LOG.info(_("[BRM-DRIVER] replication_driver_data is %s"),
                             sd_sn)
                    model_update['replication_driver_data'] = json.dumps(
                        replication_driver_data)
                urn = volume_body['urn']
                uri = volume_body['uri']
                storage_type = volume_body.get('storageType')
                metadata.update({'urn': urn})
                metadata.update({'uri': uri})
                if storage_type is not None:
                    LOG.info(
                        _("[BRM-DRIVER] the storage type of volume is %s"),
                        storage_type)
                    storage_type = 'FC_' + storage_type
                    metadata.update({'StorageType': storage_type})
                volInfoUrl = volume_body.get('volInfoUrl', None)
                if volInfoUrl:
                    metadata.update({'quantityGB': volume_body['quantityGB']})
                    metadata.update({'volInfoUrl': volInfoUrl})
                pvscsi_support = volume_body.get('pvscsiSupport')
                if pvscsi_support == 1:
                    metadata.update({'hw:passthrough': 'true'})

        return model_update, metadata

    def create_consistencygroup(self, context, group):
        LOG.info(_("[BRM-DRIVER] create consistencygroup. "))
        return

    def update_consistencygroup(self, context, group,
                                add_volumes=None, remove_volumes=None):
        """Updates a consistency group."""
        return None, None, None

    def _vrm_delete_volume(self, volume):
        '''_vrm_delete_volume

        :param volume:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start _vrm_delete_volume() "))
        vol_metas = volume['volume_metadata']
        if vol_metas is not None:
            for meta in vol_metas:
                LOG.info(_("[BRM-DRIVER] volume_metadata is [%s:%s] "),
                         meta.key, meta.value)
        provider_location = volume['provider_location']
        if provider_location is None or provider_location == '':
            LOG.error(_("[BRM-DRIVER]provider_location is null "))
            vol_meta = volume.get('volume_metadata')
            vol_meta_dict = metadata_to_dict(vol_meta)
            vol_uri = vol_meta_dict.get('uri')
            if vol_uri is not None:
                fc_vol_urn = vol_meta_dict.get('urn')
                fc_vol_id = fc_vol_urn[fc_vol_urn.rfind(':') + 1:]
                self._check_replications(fc_vol_id)
                self._vrm_delete_volume_vm(volume_urn=fc_vol_urn,
                                           volume_uri=vol_uri)
            return
        fc_volume_urn, items = \
            self._vrm_unpack_provider_location(volume['provider_location'],
                                               'urn')
        fc_vol_id = fc_volume_urn[fc_volume_urn.rfind(':') + 1:]
        self._check_replications(fc_vol_id)
        volume_uri, items = \
            self._vrm_unpack_provider_location(volume['provider_location'],
                                               'uri')
        self._vrm_delete_volume_vm(volume_urn=fc_volume_urn,
                                   volume_uri=volume_uri)

    def _vrm_delete_volume_vm(self, volume_urn, volume_uri):
        '''_vrm_delete_volume_vm

        :param volume_urn:
        :param volume_uri:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start _vrm_delete_volume_vm() "))
        kwargs = {}
        kwargs['volume_urn'] = volume_urn
        vm_body = self.vm_proxy.query_vm_volume(**kwargs)
        if vm_body is None:
            LOG.info(_("[BRM-DRIVER]vm attached is zero "))
            self.volume_proxy.delete_volume(volume_uri=volume_uri)
        else:
            vm_uri = vm_body.get('uri')
            vm_status = vm_body.get('status')
            kwargs['vm_id'] = vm_uri[-10:]
            kwargs['mode'] = 'force'
            if vm_status not in ['stopped']:
                self.vm_proxy.stop_vm(**kwargs)
            self.vm_proxy.detach_vol_from_vm(**kwargs)
            self.volume_proxy.delete_volume(volume_uri=volume_uri)

    def _check_quick_start(self, volume_id):
        '''_check_quick_start

        :param volume_id:
        :return:
        '''
        admin_context = cinder_context.get_admin_context()
        admin_metadata = self.db.volume_admin_metadata_get(admin_context,
                                                           volume_id)
        quick_start = admin_metadata.get('quick_start')
        if quick_start is not None and str(quick_start).lower() == 'true':
            return True
        else:
            return False

    def _check_replications(self, fc_vol_id):

        if fc_vol_id:
            try:
                body = self.volume_proxy.query_volume_replications(
                    volume_id=fc_vol_id)
                LOG.info(
                    _("[BRM-DRIVER] check replications : replications is %s"),
                    body.get('replications'))
            except Exception as ex:
                LOG.error(_("[BRM-DRIVER] get volume replications expection"))
                LOG.exception(ex)
                return
            if body.get('replications'):
                LOG.error(_(
                    "[BRM-DRIVER] The volume %s to deleted has replications, "
                    "could not be deleted ,extended or migrated."), fc_vol_id)
                raise driver_exception.FusionComputeDriverException()

    def delete_consistencygroup(self, context, group):

        LOG.info(_("[BRM-DRIVER] enter delete_consistencygroup() "))
        model_update = {}
        model_update['status'] = 'deleted'
        volumes = self.db.volume_get_all_by_group(context, group['id'])
        if volumes:
            for volume_ref in volumes:
                try:
                    self.delete_volume(volume_ref)
                    volume_ref['status'] = 'deleted'
                except Exception as ex:
                    LOG.error(
                        _("[BRM-DRIVER] delete_consistencygroup failed. "))
                    LOG.exception(ex)
                    volume_ref['status'] = 'error_deleting'
                    model_update['status'] = 'error_deleting'

        LOG.info(_("[BRM-DRIVER] end delete_consistencygroup() "))

        return model_update, volumes

    def create_volume(self, volume):
        '''create_volume

        create_volume
        {
            "name":string,
            quantityGB:integer,
            datastoreUrn:string,
            "isThin":boolean,
            "type":string,
            indepDisk:boolean,
            persistentDisk:boolean
        }

        :param volume:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start create_volume() "))

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        linked_clone = vol_meta_dict.get('linked_clone')
        if linked_clone is None:
            linked_clone = False
        elif str(linked_clone).lower() == 'true':
            linked_clone = True
        else:
            linked_clone = False

        # check quick start
        if not linked_clone and self._check_quick_start(volume.get('id')):
            linked_clone = True

        if linked_clone:
            LOG.info(_("[BRM-DRIVER] linked_clone volume. do nothing. "))
            return

        args_dict = {}

        args_dict['name'] = volume['name']
        args_dict['size'] = int(volume['size'])
        args_dict['uuid'] = volume['id']
        shareable = volume.get('shareable')
        if shareable and True == shareable:
            LOG.info(_("[CINDER-VRM] shareable"))
            args_dict['type'] = 'share'

        else:
            args_dict['type'] = 'normal'

        is_thin = FC_DRIVER_CONF.vrm_is_thin

        ds_meta = {}
        try:
            ds_meta['ds_name'] = volume.get('host').split('#')[1]
        except exception.CinderException as ex:
            LOG.info(
                _(
                    "[CINDER-BRM] host format exception, host is %s ") %
                volume.get('host'))
            raise ex

        datastore = self._choose_datastore(ds_meta)
        if datastore:
            LOG.info(_("[CINDER-VRM] datastore [%s],"), datastore)
            if str(datastore.get('storageType')).upper() in ['LUN']:
                LOG.info(_("[CINDER-VRM] rdm disk [%s]"), volume['id'])
                args_dict['size'] = int(datastore.get('capacityGB'))
                args_dict['independent'] = True

            args_dict['ds_urn'] = datastore.get('urn')
            is_thin = self.check_thin(datastore, is_thin)
            args_dict['is_thin'] = is_thin

            context = cinder_context.get_admin_context()

            volume_type_id = volume.get('volume_type_id')
            if volume_type_id:
                LOG.info(_("[BRM-DRIVER] query volume type id is %s"),
                         volume_type_id)
                volume_type_extra_specs = self.db.volume_type_extra_specs_get(
                    context, volume_type_id)
                LOG.info(_("[BRM-DRIVER] volume_type_extra_specs is %s"),
                         str(volume_type_extra_specs))
                if volume_type_extra_specs and str(volume_type_extra_specs.get(
                        'hw:passthrough')).lower() == 'true':
                    args_dict[
                        'support_pvscsi'] = 1
            support_pvscsi = vol_meta_dict.get('hw:passthrough')
            if support_pvscsi and str(support_pvscsi).lower() == 'true':
                args_dict[
                    'support_pvscsi'] = 1

            body = self.volume_proxy.create_volume(**args_dict)

            temp_str = body.get('urn')
            fc_vol_id = temp_str[temp_str.rfind(':') + 1:]
            LOG.info(_("[CINDER-VRM] fc_vol_id [%s] ") % fc_vol_id)
            model_update, metadata = self._vrm_get_volume_meta(id=fc_vol_id)
            self.db.volume_metadata_update(context, volume['id'], metadata,
                                           False)
            
            self.volume_proxy.wait_task(**body)

            try:
                model_update, metadata = self._vrm_get_volume_meta(
                    id=fc_vol_id)
            except Exception as ex:
                LOG.error(
                    _(
                        "[CINDER-VRM] get volume information failed after "
                        "create volume.volume id is [%s] ") % fc_vol_id)
                LOG.exception(ex)
            return model_update
        else:
            raise exception.VolumeDriverException

    def _register_volume(self, volume):
        '''_register_volume

        _register_volume

        :param volume:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start _register_volume() "))
        volume_metadata = volume['volume_metadata']
        volume_metadata_dict = metadata_to_dict(volume_metadata)
        model_update = {}

        volume_urn = volume_metadata_dict.get('volumeUrn')
        volume_id = volume_urn.split(':')[-1]
        volume_body = self.volume_proxy.get_volume(vol_id=volume_id)
        model_update['provider_location'] = self._vrm_pack_provider_location(
            volume_body)
        model_update['volume_urn'] = volume_body['urn']
        return model_update

    def delete_volume(self, volume):
        '''delete_volume

        delete_volume

        :param volume:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start delete_volume() "))
        self._vrm_delete_volume(volume)

    def create_export(self, context, volume):
        '''create_export

        create_export

        :param context:
        :param volume:
        :return:
        '''
        pass

    def remove_export(self, context, volume):
        '''remove_export

        remove_export

        :param context:
        :param volume:
        :return:
        '''
        pass

    def ensure_export(self, context, volume):
        '''ensure_export

        ensure_export

        :param context:
        :param volume:
        :return:
        '''
        pass

    def check_for_export(self, context, volume_id):
        '''check_for_export

        check_for_export

        :param context:
        :param volume_id:
        :return:
        '''
        pass

    def create_snapshot(self, snapshot):
        '''create_snapshot

        create_snapshot

        :param snapshot:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start create_snapshot() "))
        model_update = {}
        volume = self.db.volume_get(self.context, snapshot['volume_id'])
        vol_meta = volume['volume_metadata']
        vol_meta_dict = metadata_to_dict(vol_meta)
        vol_urn = vol_meta_dict.get('urn')
        if vol_urn is None:
            LOG.error(_("vol_urn  is null."))

        def volume_uri_to_number(uri):
            hi, si, ti = uri.rpartition('/')
            return ti

        snapshot_id = snapshot['id']
        snapshot_uuid = str(snapshot_id).replace('-', '')
        body = self.volume_snapshot_proxy.create_volumesnapshot(
            snapshot_uuid=snapshot_uuid, vol_urn=vol_urn)

        if body['urn'] is None:
            LOG.error(_("Trying to create snapshot failed, volume id is: %s"),
                      snapshot['volume_id'])
            raise driver_exception.FusionComputeDriverException()

        return model_update

    def delete_snapshot(self, snapshot):
        '''delete_snapshot

        delete_snapshot

        :param snapshot:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start delete_snapshot() "))
        model_update = {}
        snapshot_id = snapshot['id']
        snapshot_uuid = str(snapshot_id).replace('-', '')
        body = self.volume_snapshot_proxy.query_volumesnapshot(
            uuid=snapshot_uuid)
        if body is None:
            return model_update
        self.volume_snapshot_proxy.delete_volumesnapshot(id=snapshot_uuid)

        return model_update

    def create_volume_from_snapshot(self, volume, snapshot):
        '''create_volume_from_snapshot

        :param volume:
        :param snapshot:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start create_volume_from_snapshot()"))
        args_dict = {}

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        linked_clone = vol_meta_dict.get('linked_clone')
        if linked_clone is None:
            linked_clone = False
        elif str(linked_clone).lower() == 'true':
            linked_clone = True
        else:
            linked_clone = False
        if linked_clone is True:
            LOG.warn(_("[BRM-DRIVER] linked_clone volume not support!!"))
            raise exception.CinderException

        full_clone = vol_meta_dict.get('full_clone')
        if full_clone is None:
            full_clone = '1'
        elif str(full_clone) == '0':
            full_clone = '0'
        else:
            full_clone = '1'
        args_dict['full_clone'] = full_clone

        model_update = {}
        snapshot_id = snapshot['id']
        os_vol_id = volume['id']
        shareable = volume.get('shareable')
        if shareable and True == shareable:
            LOG.info(_("[CINDER-VRM] shareable"))
            voltype = 'share'
        else:
            voltype = 'normal'
        snapshot_uuid = str(snapshot_id).replace('-', '')

        volume_size = int(volume.get('size'))
        snapshot_size = int(snapshot.get('volume_size'))

        if volume_size != snapshot_size:
            args_dict['volume_size'] = volume_size

        args_dict['snapshot_uuid'] = snapshot_uuid
        args_dict['volume_name'] = volume['name']
        args_dict['volume_uuid'] = os_vol_id
        args_dict['type'] = voltype

        body = self.volume_snapshot_proxy.create_volume_from_snapshot(
            **args_dict)
        vol_urn = body['urn']
        fc_vol_id = vol_urn.split(':')[-1]

        model_update, metadata = self._vrm_get_volume_meta(id=fc_vol_id)
        context = cinder_context.get_admin_context()
        self.db.volume_metadata_update(context, os_vol_id, metadata, False)

        return model_update

    def create_cloned_volume(self, volume, src_volume):
        LOG.info(_("[BRM-DRIVER] start create_cloned_volume()"))
        volume['is_thin'] = True
        LOG.info(_("[BRM-DRIVER] start create_cloned_volume()"))
        model_update = self.create_volume(volume)
        dest_volume_size = volume['size']
        src_volume_size = src_volume['size']
        if dest_volume_size != src_volume_size:
            raise exception.InvalidParameterValue(err=_('valid volume size'))

        dest_volume_uri, items = self._vrm_unpack_provider_location(
            model_update['provider_location'], 'uri')
        dest_volume_urn, items = self._vrm_unpack_provider_location(
            model_update['provider_location'], 'urn')
        src_vol_meta = src_volume.get('volume_metadata')
        src_vol_meta_dict = metadata_to_dict(src_vol_meta)
        src_volume_uri = src_vol_meta_dict.get('uri')
        dest_volume_id = dest_volume_uri.split('/')[
            len(dest_volume_uri.split('/')) - 1]
        src_volume_id = src_volume_uri.split('/')[
            len(src_volume_uri.split('/')) - 1]
        args_dict = {}
        args_dict['src_volume_id'] = src_volume_id
        args_dict['dest_volume_urn'] = dest_volume_urn

        try:
            self.volume_proxy.clone_volume(**args_dict)
        except exception.CinderException as ex:
            volume['provider_location'] = model_update['provider_location']
            LOG.info(
                _("[CINDER-BRM] clone_volume exception , delete (%s)") %
                model_update['provider_location'])
            self.delete_volume(volume)
            raise ex
        return model_update

    def clone_image(self, context, volume, image_location, image_meta,
                    image_service):
        '''lone_image

        :param volume:
        :param image_location:
        :param image_id:
        :param image_meta:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start clone_image "))

        properties = image_meta.get('properties', None)

        # check quick start
        if properties is not None:
            quick_start = properties.get('quick_start', None)
            if quick_start is not None and str(quick_start).lower() == 'true':
                LOG.info(_("[BRM-DRIVER] image has quick start property"))
                # update quick_start in admin metadata
                admin_context = cinder_context.get_admin_context()
                self.db.volume_admin_metadata_update(admin_context,
                                                     volume.get('id'),
                                                     {'quick_start': 'True'},
                                                     False)
                return None, False

        if properties is None:
            return None, False
        elif 'template' != properties.get('__image_source_type', None):
            LOG.info(_("[BRM-DRIVER] image_type is not template"))
            return None, False
        else:
            LOG.info(_("[BRM-DRIVER] image_type is template"))

        image_type = 'template'

        context = cinder_context.get_admin_context()
        args_dict = {}
        vol_size = int(volume.get('size'))
        image_id = image_meta['id']
        self._check_image_os(image_id, image_meta, vol_size)

        args_dict['image_id'] = image_id
        os_vol_id = volume.get('id')
        args_dict['volume_id'] = os_vol_id
        args_dict['volume_size'] = vol_size

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        linked_clone = vol_meta_dict.get('linked_clone')
        if linked_clone is None:
            linked_clone = False
        elif str(linked_clone).lower() == 'true':
            linked_clone = True
        else:
            linked_clone = False

        hw_image_location = properties.get('__image_location', None)
        if hw_image_location is None or hw_image_location == "":
            msg = _('[BRM-DRIVER] hw_image_location is null')
            LOG.error(msg)
            raise exception.ImageUnacceptable(image_id=image_id, reason=msg)

        args_dict['image_location'] = hw_image_location
        LOG.info(_('[BRM-DRIVER] image_location is %s') % args_dict[
            'image_location'])

        args_dict['image_type'] = image_type

        ds_meta = {}
        try:
            ds_meta['ds_name'] = volume.get('host').split('#')[1]
        except exception.CinderException as ex:
            raise ex

        datastore = self._choose_datastore(ds_meta)
        if not datastore:
            LOG.info(_("[CINDER-VRM] datastore [%s],"), datastore)
            raise exception.InvalidParameterValue(err=_('invalid datastore'))

        args_dict['ds_urn'] = datastore.get('urn')
        is_thin = FC_DRIVER_CONF.vrm_is_thin
        is_thin = self.check_thin(datastore, is_thin)
        args_dict['is_thin'] = is_thin

        args_dict['cluster_urn'] = self._get_cluster_by_dsurn(
            datastore.get('urn'))
        LOG.info(_("[BRM-DRIVER] cluster_urn [%s]") % args_dict['cluster_urn'])

        if args_dict.get('volume_sequence_num') is None:
            args_dict['volume_sequence_num'] = 1

        LOG.info(_("[BRM-DRIVER] %s image_type is  template ") % image_id)
        if linked_clone:
            urn = self.vm_proxy.create_linkclone_from_template(**args_dict)
        else:
            urn = self.vm_proxy.create_volume_from_template(**args_dict)

        temp_str = str(urn)
        fc_vol_id = temp_str[temp_str.rfind(':') + 1:]

        share = volume.get('shareable')
        LOG.info('[BRM-DRIVER] shareable [%s]', share)
        if str(share).lower() == 'true':
            try:
                self.volume_proxy.modify_volume(volume_id=fc_vol_id,
                                                type='share')
            except Exception as ex:
                LOG.error(_("modify volume to share is failed "))
                self.delete_volume(volume)
                raise ex

        model_update, metadata = self._vrm_get_volume_meta(fc_vol_id)
        self.db.volume_metadata_update(context, os_vol_id, metadata, False)
        return model_update, True

    def copy_image_to_volume(self, context, volume, image_service, image_id):
        '''clone_image

        :param volume:
        :param image_location:
        :param image_id:
        :param image_meta:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start copy_image_to_volume [%s]") % image_id)
        image_meta = image_service.show(context, image_id)
        args_dict = {}
        vol_size = int(volume.get('size'))
        self._check_image_os(image_id, image_meta, vol_size)

        args_dict['image_id'] = image_id

        min_disk = image_meta.get('min_disk')
        if self._check_quick_start(volume.get('id')):
            args_dict['quick_start'] = True
            if min_disk:
                args_dict['image_size'] = int(min_disk)
            else:
                msg = _('[BRM-DRIVER] image min_disk is none when create from '
                        'quick start image')
                LOG.error(msg)
                raise exception.VolumeDriverException(message=msg)

        os_vol_id = volume.get('id')
        args_dict['volume_id'] = os_vol_id
        args_dict['volume_size'] = vol_size
        args_dict['is_thin'] = FC_DRIVER_CONF.vrm_is_thin

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        linked_clone = vol_meta_dict.get('linked_clone')
        args_dict['volume_urn'] = vol_meta_dict.get('urn')
        if linked_clone is None:
            linked_clone = False
        elif str(linked_clone).lower() == 'true':
            linked_clone = True
        else:
            linked_clone = False

        # check quick start
        if not linked_clone and self._check_quick_start(volume.get('id')):
            linked_clone = True
        msg = _('[BRM-DRIVER] image_meta [%s]' % image_meta)
        LOG.info(msg)

        properties = image_meta.get('properties', None)
        if properties is None or properties == "":
            image_type = 'glance'
        else:
            args_dict['volume_sequence_num'] = properties.get('__sequence_num')
            image_type = properties.get('__image_source_type', None)
            types = ['template', 'nfs', 'uds', 'glance']

            if image_type is not None and image_type not in types:
                msg = _('[BRM-DRIVER]  image type is not support ')
                LOG.error(msg)
                raise exception.ImageUnacceptable(image_id=image_id,
                                                  reason=msg)

            if image_type is None:
                image_type = 'glance'

            if image_type != 'glance':
                hw_image_location = properties.get('__image_location', None)
                if hw_image_location is None or hw_image_location == "":
                    msg = _('[BRM-DRIVER] hw_image_location is null')
                    LOG.error(msg)
                    raise exception.ImageUnacceptable(image_id=image_id,
                                                      reason=msg)

                args_dict['image_location'] = hw_image_location
                LOG.info(_('[BRM-DRIVER] image_location is %s') % args_dict[
                    'image_location'])

        args_dict['image_type'] = image_type
        ds_meta = {}
        try:
            ds_meta['ds_name'] = volume.get('host').split('#')[1]
        except exception.CinderException as ex:
            LOG.info(_(
                "[CINDER-BRM] host format exception, host is %s ") %
                     volume.get('host'))
            raise ex

        datastore = self._choose_datastore(ds_meta)
        if not datastore:
            LOG.info(_("[CINDER-VRM] datastore [%s],"), datastore)
            raise exception.InvalidParameterValue(err=_('found no datastore'))

        args_dict['ds_urn'] = datastore.get('urn')
        is_thin = FC_DRIVER_CONF.vrm_is_thin
        is_thin = self.check_thin(datastore, is_thin)
        if linked_clone:
            if not is_thin:
                LOG.error(_("[CINDER-VRM] datastore does support linedclone"))
                raise exception.InvalidParameterValue(
                    err=_('datastore does support linedclone'))
        args_dict['is_thin'] = is_thin

        args_dict['cluster_urn'] = self._get_cluster_by_dsurn(
            datastore.get('urn'))
        LOG.info(
            _("[BRM-DRIVER] self.cluster_urn [%s]") % args_dict['cluster_urn'])

        args_dict['auth_token'] = context.auth_token

        if args_dict.get('volume_sequence_num') is None:
            args_dict['volume_sequence_num'] = 1

        if linked_clone:
            urn = self.vm_proxy.create_linkClone_from_extend(**args_dict)
        else:
            try:
                temp_str = str(vol_meta_dict.get('urn'))
                fc_vol_id = temp_str[temp_str.rfind(':') + 1:]
                self.volume_proxy.modify_volume(volume_id=fc_vol_id,
                                                type='normal')
            except Exception as ex:
                LOG.error(_("modify volume to normal is failed "))
                self.delete_volume(volume)
                raise ex
            urn = self.vm_proxy.create_volume_from_extend(**args_dict)

        temp_str = str(urn)
        fc_vol_id = temp_str[temp_str.rfind(':') + 1:]
        model_update, metadata = self._vrm_get_volume_meta(fc_vol_id)

        share = volume.get('shareable')
        LOG.info('[BRM-DRIVER] shareable [%s]', share)
        if str(share).lower() == 'true':
            try:
                self.volume_proxy.modify_volume(volume_id=fc_vol_id,
                                                type='share')
            except Exception as ex:
                LOG.error(_("modify volume to share is failed "))
                self.delete_volume(volume)
                raise ex

        self.db.volume_metadata_update(context, os_vol_id, metadata, False)

    def _check_image_os(self, image_id, image_meta, vol_size):
        min_disk = image_meta.get('min_disk')
        if min_disk:
            min_disk = int(min_disk)
            if min_disk < 4 and vol_size != min_disk:
                prop = image_meta.get('properties')
                if prop:
                    os_type = prop.get('__os_type')
                    os_version = prop.get('__os_version')
                    if os_type and os_version:
                        if os_type == 'Windows' and \
                                (os_version.startswith(
                                    'Windows XP') or os_version.startswith(
                                    'Windows Server 2003')):
                            msg = _(
                                "[BRM-DRIVER] volume  size must equal image "
                                "min disk size while image min disk size is "
                                "smaller than 4G and os is Windows XP or "
                                "Windows Server 2003")
                            LOG.error(msg)
                            raise exception.ImageUnacceptable(
                                image_id=image_id, reason=msg)

    def _generate_image_metadata(self, min_disk, location,
                                 volume_sequence_num, os_option, instance):
        """_generate_image_metadata

        :param name: image name
        :param location: image location
        :param os_option: os type and version
        :param instance:
        :return:
        """
        if volume_sequence_num is None:
            LOG.info(_("volume_sequence_num is None"))
            volume_sequence_num = 1
        metadata = {'__image_location': location or '',
                    '__image_source_type': FC_DRIVER_CONF.export_image_type,
                    '__sequence_num': volume_sequence_num}
        if os_option is not None:
            if os_option.get('__os_version') is not None:
                metadata['__os_version'] = os_option.get('__os_version')

            if os_option.get('__os_type') is not None:
                metadata['__os_type'] = os_option.get('__os_type')

        LOG.info(_("image metadata is: %s"), json.dumps(metadata))
        return {'properties': metadata, 'min_disk': min_disk}

    def _generate_image_location(self, image_id, context):
        """_generate_image_location
        generate image location: '172.17.1.30:/image/base/uuid/uuid.ovf'
        :param image_id:
        :return:
        """
        if FC_DRIVER_CONF.export_image_type == 'nfs':
            fc_image_path = FC_DRIVER_CONF.fc_image_path
            if fc_image_path:
                format = 'xml' if FC_DRIVER_CONF.export_version == 'v1.2' else\
                    'ovf'
                return '%s/%s/%s.%s' % (fc_image_path, image_id, image_id,
                                        format)
            else:
                LOG.info(_("fc_image_path is null"))
        elif FC_DRIVER_CONF.export_image_type == 'uds':
            if FC_DRIVER_CONF.uds_ip is not None and \
                            FC_DRIVER_CONF.uds_port is not None and \
                            FC_DRIVER_CONF.uds_bucket_name is not None:
                    uds_bucket_name = FC_DRIVER_CONF.uds_bucket_name
                    bucket_type = FC_DRIVER_CONF.uds_bucket_type

            LOG.info(str(bucket_type))
            LOG.info(context.tenant)

            if bucket_type is not None:
                if str(bucket_type) == 'wildcard':
                    project_tmp_id = context.to_dict()['project_id']
                    LOG.info(project_tmp_id)
                    if project_tmp_id is None:
                        LOG.error(_("project_id is none "))
                        raise exception.ParameterNotFound(
                            param='project_id is none')
                    else:
                        uds_bucket_name += project_tmp_id
                ret = '%s:%s:%s:%s' % (
                    FC_DRIVER_CONF.uds_ip, FC_DRIVER_CONF.uds_port,
                    uds_bucket_name,
                    image_id)
            return ret
        else:
            return None

    def copy_volume_to_image(self, context, volume, image_service, image_meta):
        '''copy_volume_to_image

        :param context:
        :param volume:
        :param image_service:
        :param image_meta:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start copy_volume_to_image() "))

        fc_image_path = FC_DRIVER_CONF.fc_image_path
        image_id = image_meta.get('id')
        if '/' in str(image_id):
            image_id = image_id.split('/')[-1]

        metadata = {'__image_location': '',
                    '__image_source_type': FC_DRIVER_CONF.export_image_type}
        image_property = {'properties': metadata}
        image_service.update(context, image_id, image_property,
                             purge_props=False)

        volume_id = volume.get('id')
        vol_size = int(volume.get('size'))
        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)

        vol_image_meta = None
        try:
            vol_image_meta = self.volume_api.get_volume_image_metadata(
                context, volume)
        except exception.CinderException as ex:
            LOG.error(_('[BRM-DRIVER] get_volume_image_metadata is error'))

        vol_image_meta_dic = None
        if vol_image_meta:
            vol_image_meta_dic = dict(vol_image_meta.iteritems())

        args_dict = {}
        args_dict['volume_id'] = volume_id
        args_dict['volume_size'] = vol_size
        args_dict['image_id'] = image_id
        args_dict['image_url'] = fc_image_path
        args_dict['project_id'] = context.to_dict()['project_id']

        share = volume.get('shareable')
        LOG.info('[BRM-DRIVER] shareable [%s]', share)
        if str(share).lower() == 'false':
            args_dict['shareable'] = 'normal'
        else:
            args_dict['shareable'] = 'share'

        if vol_meta_dict.get('urn') is None:
            msg = _('[BRM-DRIVER] urn is null')
            LOG.error(msg)
            raise exception.InvalidVolumeMetadata(reason=msg)

        args_dict['volume_urn'] = str(vol_meta_dict.get('urn'))
        LOG.info(_("volume_urn is %s") % args_dict['volume_urn'])

        if self.SHARED_HOSTS is None or len(self.SHARED_HOSTS) == 0:
            msg = _('[BRM-DRIVER] SHARED_HOSTS is none')
            LOG.error(msg)
            raise exception.VolumeDriverException(message=msg)

        fc_vol_id = \
            args_dict['volume_urn'][args_dict['volume_urn'].rfind(':') + 1:]
        volume_body = self.volume_proxy.query_volume(id=fc_vol_id)
        source_ds_urn = volume_body['datastoreUrn']
        LOG.info(_("[BRM-DRIVER] datastoreUrn  [%s]") % source_ds_urn)
        args_dict['cluster_urn'] = self._get_cluster_by_dsurn(source_ds_urn)
        LOG.info(
            _("[BRM-DRIVER] cluster_urn  [%s]") % args_dict['cluster_urn'])

        # args_dict['cluster_urn'] = self.cluster_urn
        name = image_service.show(context, image_id).get('name')
        args_dict['image_type'] = FC_DRIVER_CONF.export_image_type
        args_dict['auth_token'] = context.auth_token

        location = self._generate_image_location(image_id, context)
        metadata = {'__image_location': location or '',
                    '__image_source_type': FC_DRIVER_CONF.export_image_type}
        image_property = {'properties': metadata}
        image_service.update(context, image_id, image_property,
                             purge_props=False)

        try:
            LOG.info(_('location %s  ') % location)
            volume_sequence_num = self.vm_proxy.export_volume_to_image(
                **args_dict)
        except Exception as ex:
            LOG.info(_("[BRM-DRIVER] deletedelete image id:[%s]") % image_id)
            image_service.delete(context, image_id)
            raise ex

        metadata = self._generate_image_metadata(vol_size, location,
                                                 volume_sequence_num,
                                                 vol_image_meta_dic, None)
        if 'glance' != args_dict.get('image_type'):
            image_service.update(context, image_id, {},
                                 data='/home/vhd/G1-1.vhd')
        image_service.update(context, image_id, metadata, purge_props=False)
        LOG.info(_('image %s  create success') % name)

    def attach_volume(self, context, volume_id, instance_uuid,
                      host_name_sanitized, mountpoint):
        '''attach_volume

        :param context:
        :param volume_id:
        :param instance_uuid:
        :param host_name_sanitized:
        :param mountpoint:
        :return:
        '''
        pass

    def detach_volume(self, context, volume):
        '''detach_volume

        :param context:
        :param volume:
        :return:
        '''
        pass

    def initialize_connection(self, volume, connector):
        '''initialize_connection

        :param volume:
        :param connector:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start initialize_connection() "))
        connection = {'data': {}}
        LOG.info('volume: %s', volume)
        admin_context = cinder_context.get_admin_context()
        vol_meta = self.db.volume_metadata_get(admin_context, volume['id'])
        connection['vol_urn'] = vol_meta['urn']
        return connection

    def terminate_connection(self, volume, connector, **kwargs):
        '''terminate_connection

        :param volume:
        :param connector:
        :param force:
        :return:
        '''
        pass

    def retype(self, context, volume, new_type, diff, host):
        '''retype

        :param context:
        :param volume:
        :param new_type:
        :param diff:
        :param host:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start retype() "))
        LOG.info(_(" new volume type [%s]"), new_type)

        args_dict = {}
        is_thin = FC_DRIVER_CONF.vrm_is_thin
        if str(is_thin).lower() == 'true':
            args_dict['migrate_type'] = 1
        else:
            args_dict['migrate_type'] = 2
        shareable = volume.get('shareable')
        if shareable is True:
            LOG.info(_("[BRM-DRIVER] shareable"))

        if diff and diff.get('extra_specs'):
            pvscsi_support = diff.get('extra_specs').get('hw:passthrough')
            if pvscsi_support and pvscsi_support[0] != pvscsi_support[1]:
                msg = (_('rdm volume can only retyped to rdm volume type.'))
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        extra_specs = new_type.get('extra_specs')
        if extra_specs is None:
            LOG.info(_("[BRM-DRIVER] extra_specs is None"))
            return True, None
        new_backend_name = extra_specs.get('volume_backend_name')
        if new_backend_name is None:
            LOG.info(_("[BRM-DRIVER] new_backend_name is None"))
            return True, None

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        linked_clone = vol_meta_dict.get('linked_clone')
        if linked_clone is not None:
            if str(linked_clone).lower() == 'true':
                msg = (_('linked volume can not be retyped. '))
                LOG.error(msg)
                raise exception.InvalidInput(reason=msg)

        snapshots = self.db.snapshot_get_all_for_volume(context,
                                                        volume.get('id'))
        if len(snapshots):
            msg = _("Volume still has %d dependent snapshots") % len(snapshots)
            raise exception.InvalidVolume(reason=msg)

        try:
            source_ds_name = volume.get('host').split('#')[1]
        except exception.CinderException as ex:
            LOG.info(_("[CINDER-BRM] host format exception, host is %s ") %
                     volume.get('host'))
            raise ex

        LOG.info(_(" source_ds_name [%s]"), source_ds_name)

        try:
            new_ds_name = volume_utils.extract_host(host['host'], 'pool')
        except exception.CinderException as ex:
            LOG.info(
                _("[CINDER-BRM] host format exception, host is %s ") %
                volume.get('host'))
            raise ex

        LOG.info(_(" new_ds_name [%s]"), new_ds_name)

        if source_ds_name == new_ds_name:
            LOG.info(_("[CINDER-BRM] source ds_name == dest ds_name"))
            return True, None

        datastores = self.datastore_proxy.list_datastore()
        for datastore in datastores:
            ds_name = datastore.get('name')
            if ds_name is not None:
                LOG.info(_(" ds_name [%s]"), ds_name)
                if new_ds_name == ds_name:
                    args_dict['dest_ds_urn'] = datastore.get('urn')
                    LOG.info(_(" new_ds_name [%s]"), new_ds_name)
                    break

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        volume_urn = vol_meta_dict.get('urn')
        args_dict['volume_urn'] = volume_urn
        fc_vol_id = volume_urn[volume_urn.rfind(':') + 1:]
        volume_body = self.volume_proxy.query_volume(id=fc_vol_id)
        source_ds_urn = volume_body['datastoreUrn']

        args_dict['volume_id'] = fc_vol_id
        args_dict['speed'] = 30

        if None == args_dict.get('dest_ds_urn'):
            LOG.info(_("[BRM-DRIVER] no dest_ds_urn"))
            return False, None
        else:
            if source_ds_urn == args_dict['dest_ds_urn']:
                LOG.info(_("[BRM-DRIVER] same ds [%s]"), source_ds_urn)
                return True, None
            vm_body = self.vm_proxy.query_vm_volume(**args_dict)
            self._check_replications(fc_vol_id)
            if vm_body is None:
                self.volume_proxy.migrate_volume(**args_dict)
            else:
                vm_urn = vm_body.get('urn')
                vm_id = vm_urn[-10:]
                args_dict['vm_id'] = vm_id
                self.vm_proxy.migrate_vm_volume(**args_dict)

        LOG.info(_("[CINDER-VRM] fc_vol_id [%s] ") % fc_vol_id)
        model_update, metadata = self._vrm_get_volume_meta(id=fc_vol_id)
        self.db.volume_metadata_update(context, volume['id'], metadata, False)
        LOG.info(_("[BRM-DRIVER] retype return"))
        return True, None

    def manage_existing(self, volume, existing_ref):
        """Brings an existing backend storage object under Cinder management.

        existing_ref is passed straight through from the API request's
        manage_existing_ref value, and it is up to the driver how this should
        be interpreted.  It should be sufficient to identify a storage object
        that the driver should somehow associate with the newly-created cinder
        volume structure.
        """

        LOG.info(_("[BRM-DRIVER] start manage_existing() "))
        metadata = dict(
            (item['key'], item['value']) for item in volume['volume_metadata'])
        volInfoUrl = metadata.get('volInfoUrl', None)
        if volInfoUrl is None:
            LOG.info(_("manage_existing: volInfoUrl is None"))
            raise driver_exception.FusionComputeDriverException()

        name = volume['name']
        uuid = volume['id']
        shareable = volume.get('shareable')
        if shareable is True:
            voltype = 'share'
        else:
            voltype = 'normal'

        quantity_GB = int(volume['size'])

        args_dict = {}
        args_dict['name'] = name
        args_dict['quantityGB'] = quantity_GB
        args_dict['type'] = voltype
        args_dict['volInfoUrl'] = volInfoUrl
        args_dict['uuid'] = uuid

        args_dict['maxReadBytes'] = 0
        args_dict['maxWriteBytes'] = 0
        args_dict['maxReadRequest'] = 0
        args_dict['maxWriteRequest'] = 0
        body = self.volume_proxy.manage_existing(**args_dict)

        model_update = {}
        temp_str = str(body.get('urn'))
        fc_vol_id = temp_str[temp_str.rfind(':') + 1:]
        model_update, metadata = self._vrm_get_volume_meta(id=fc_vol_id)
        context = cinder_context.get_admin_context()
        self.db.volume_metadata_update(context, uuid, metadata, False)
        if existing_ref:
            LOG.info("manage_existing: existing_ref[%s]", existing_ref)
            for key, value in existing_ref.items():
                self.db.volume_glance_metadata_create(context,
                                                      uuid,
                                                      key, value)

        return model_update

    def manage_existing_get_size(self, volume, existing_ref):
        """Return size of volume to be managed by manage_existing.

        When calculating the size, round up to the next GB.
        """
        vol_size = 0
        for meta in volume['volume_metadata']:
            LOG.error("meta: %s" % str(meta))
            if meta.key == 'quantityGB':
                vol_size = int(meta.value)
                break

        volume['size'] = vol_size

        return vol_size

    def unmanage(self, volume):
        """Removes the specified volume from Cinder management.

        Does not delete the underlying backend storage object.

        For most drivers, this will not need to do anything.  However, some
        drivers might use this call as an opportunity to clean up any
        Cinder-specific configuration that they have associated with the
        backend storage object.

        :param volume:
        :return:
        """

        LOG.info(_("[BRM-DRIVER] start unmanage() "))

        provider_location = volume['provider_location']
        if provider_location is None or provider_location == '':
            LOG.error(_("[BRM-DRIVER]provider_location is null "))
            return
        volume_uri, items = \
            self._vrm_unpack_provider_location(volume['provider_location'],
                                               'uri')
        self.volume_proxy.unmanage(volume_uri=volume_uri)

    def migrate_volume(self, context, volume, host):
        '''migrate_volume

        :param context:
        :param volume:
        :param host:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start migrate_volume() "))

        raise NotImplementedError()

    def copy_volume_data(self, context, src_vol, dest_vol, remote=None):
        '''copy_volume_data

        :param context:
        :param src_vol:
        :param dest_vol:
        :param remote:
        :return:
        '''
        msg = (_('copy_volume_data. '))
        LOG.error(msg)
        raise NotImplementedError()

    def extend_volume(self, volume, new_size):
        '''extend_volume

        :param volume:
        :param new_size:
        :return:
        '''
        LOG.info(_("[BRM-DRIVER] start extend_volume() "))
        vol_metas = volume['volume_metadata']
        if vol_metas  is not  None :
            for meta in vol_metas:
                msg = _("[BRM-DRIVER] volume_metadata is [%s:%s] "), \
                        meta.key, meta.value
                LOG.info(msg)

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        vol_uri = vol_meta_dict.get('uri')
        fc_vol_urn = vol_meta_dict.get('urn')
        if fc_vol_urn is not None:
            fc_vol_id = fc_vol_urn[fc_vol_urn.rfind(':') + 1:]
            self._check_replications(fc_vol_id)

        LOG.info(_("[CINDER-VRM]extend_volume fc_vol_id [%s] ") % fc_vol_id)

        if vol_uri is not None:
            temp_str = str(vol_meta_dict.get('urn'))
            fc_vol_id = temp_str[temp_str.rfind(':') + 1:]
            vrm_volume = self.volume_proxy.query_volume(id=fc_vol_id)
            sizeGB = vrm_volume.get('quantityGB')
            if int(sizeGB) < int(new_size):
                self._check_datastore_capacity(vrm_volume, volume['size'],
                                               new_size)
                self.volume_proxy.extend_volume(volume_uri=vol_uri,
                                                size=new_size * 1024)
            else:
                LOG.info(_("[CINDER-VRM]sizeGB[%s] avoid extend") % sizeGB)
        else:
            raise exception.ExtendVolumeError

        fc_vol_id = fc_vol_urn[fc_vol_urn.rfind(':') + 1:]
        model_update, metadata = self._vrm_get_volume_meta(id=fc_vol_id)
        context = cinder_context.get_admin_context()
        self.db.volume_metadata_update(context, volume['id'], metadata, False)

    def _check_datastore_capacity(self, fc_volume, old_size, new_size):
        # keep same strage with capacity_filter
        datastore_urn = fc_volume.get('datastoreUrn')
        extra_size = new_size - old_size
        if datastore_urn:
            datastore_id = datastore_urn[datastore_urn.rfind(':') + 1:]
            datastore = self.datastore_proxy.query_datastore(id=datastore_id)

            if 'NORMAL' != datastore['status']:
                msg = _('Datastore status is abnormal.')
                raise exception.ExtendVolumeError(message=msg)

            reserved = float(self.reserved_percentage) / 100
            total = float(datastore['capacityGB'])
            free_space = datastore['freeSizeGB']
            free = free_space - math.floor(total * reserved)

            if self.thin_provision and self.over_ratio >= 1:
                provisioned_ratio = ((datastore['usedSizeGB'] +
                                      extra_size) / total)
                if provisioned_ratio >= self.over_ratio:
                    LOG.warning(_(
                        "Insufficient free space for thin provisioning. "
                        "The ratio of provisioned capacity over total "
                        "capacity "
                        "%(provisioned_ratio).2f has exceeded the maximum "
                        "over "
                        "subscription ratio %(oversub_ratio).2f."),
                        {"provisioned_ratio": provisioned_ratio,
                         "oversub_ratio": self.over_ratio})
                    msg = _('Insufficient free space for thin provisioning.')
                    raise exception.ExtendVolumeError(message=msg)
                else:
                    free_virtual = free * self.over_ratio
                    if free_virtual < extra_size:
                        msg = _(
                            'Insufficient free space for thin provisioning.')
                        raise exception.ExtendVolumeError(message=msg)
                    else:
                        return True

            if free < extra_size:
                msg = _(
                    'The new size of the volume exceed the capacity of the '
                    'datastore.')
                raise exception.ExtendVolumeError(message=msg)

    def backup_volume(self, context, backup, backup_service):
        """Create a new backup from an existing volume.

        backup['status']
        backup['object_count']
        backup['_sa_instance_state']
        backup['user_id']
        backup['service']:q

        backup['availability_zone']
        backup['deleted']
        backup['created_at']
        backup['updated_at']
        backup['display_description']
        backup['project_id']
        backup['host']
        backup['container']
        backup['volume_id']
        backup['display_name']
        backup['fail_reason']
        backup['deleted_at']
        backup['service_metadata']
        backup['id']
        backup['size']
        -------- -------- -------- --------
        backup['backup_type']
        backup['volume_name']
        backup['snap_name']
        backup['snap_id']
        backup['snap_parent_name']
        backup['snap_last_name']
        backup['clone_volume_name']
        backup['storage_ip']
        backup['storage_pool_id']
        backup['volume_offset']
        backup['incremental']
        backup['is_close_volume']
        backup['is_bootable']
        backup['image_id']

        backup['volume_size']
        volume_file
        backup_metadata

backup db:CREATED_AT | UPDATED_AT | DELETED_AT | DELETED
| ID | VOLUME_ID | USER_ID | PROJECT_ID | HOST | AVAILABILITY_ZONE
| DISPLAY_NAME | DISPLAY_DESCRIPTION | CONTAINER | STATUS | FAIL_REASON
| SERVICE_METADATA | SERVICE | SIZE | OBJECT_COUNT


snapshot db: CREATED_AT|UPDATED_AT|DELETED_AT| DELETED |ID |VOLUME_ID|USER_ID
  |PROJECT_ID| STATUS  | PROGRESS | VOLUME_SIZE | SCHEDULED_AT | DISPLAY_NAME
  | DISPLAY_DESCRIPTION | PROVIDER_LOCATION | ENCRYPTION_KEY_ID |
  VOLUME_TYPE_ID | CGSNAPSHOT_ID

        """
        vol_second_os = self.db.volume_get(context, backup['volume_id'])

        LOG.info(
            ('Creating a new backup for volume %s.') % vol_second_os['name'])
        volume_file = {}
        if vol_second_os.get('snapshot_id') != backup['container']:
            LOG.error(
                _("snapshot id is %s") % vol_second_os.get('snapshot_id'))
            LOG.error(_("backup container is %s") % backup['container'])
            raise exception.InvalidSnapshot(
                reason="snapshot id not equal backup container")

        vol_meta = vol_second_os.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        volume_urn = vol_meta_dict.get('urn')
        fc_vol_id = volume_urn[volume_urn.rfind(':') + 1:]
        vol_second_fc = self.volume_proxy.query_volume(id=fc_vol_id)

        volume_file['volume_name'] = vol_second_fc['volNameOnDev']
        last_snap_id_os = vol_second_os['snapshot_id']
        last_snap_id_fc = str(vol_second_os['snapshot_id']).replace('-', '')
        last_snap_os = self.db.snapshot_get(context, last_snap_id_os)
        vol_first_id = last_snap_os['volume_id']
        volume_file['source_volume_id'] = vol_first_id

        ext_params = vol_second_fc.get('drExtParams')
        LOG.info(_("[VRM-CINDER] ext_params [%s]"), ext_params)
        if ext_params:
            ext_params_dic = json.loads(ext_params)

        storage_type = vol_second_fc.get('storageType')
        sd_sn = vol_second_fc.get('storageSN')
        if storage_type == 'advanceSan':
            volume_file['storage_type'] = 4  # advanceSan v3 volume
            support_pvscsi = vol_second_fc.get('pvscsiSupport')
            if support_pvscsi is not None and support_pvscsi == 1:
                volume_file['storage_type'] = 5  # RDM : direct io volume
        else:
            volume_file['storage_type'] = 0
            LOG.info(_("[VRM-CINDER] ext_params [%s]"), ext_params_dic)
            volume_file['storage_ip'] = ext_params_dic['dsMgntIp']
            volume_file['storage_pool_id'] = ext_params_dic['dsResourceId']

        LOG.info(_("[VRM-CINDER] volume_file [%s]"), volume_file)
        backup_list = self.db.backup_get_by_volume_id(context, vol_first_id)

        last_backup = None
        if backup_list is not None:
            for back_tmp in backup_list:
                if back_tmp['status'] != "available" and back_tmp[
                    'status'] != "restoring":
                    continue
                if last_backup is None:
                    last_backup = back_tmp
                else:
                    if last_backup['created_at'] < back_tmp['created_at']:
                        last_backup = back_tmp

        if last_backup is None:
            volume_file['backup_type'] = 0
            volume_file['parent_id'] = None
            volume_file['parent_snapshot_url'] = None
        else:
            LOG.info(_("last_backup %s") % last_backup['id'])
            volume_file['backup_type'] = 1
            volume_file['parent_id'] = last_backup['id']

            if last_backup['service_metadata'] is None:
                raise exception.InvalidVolumeMetadata(
                    reason="backup service_metadata is none")

            service_meta = last_backup['service_metadata']
            str_service_meta = json.loads(service_meta)
            parent_snapshot_id_os = str_service_meta.get('snap_id')
            if parent_snapshot_id_os is not None:
                parent_snapshot_id_fc = \
                    str(parent_snapshot_id_os).replace('-', '')

                if storage_type == 'advanceSan':
                    parent_snapshot_fc = \
                        self.volume_snapshot_proxy.query_volumesnapshot(
                            uuid=parent_snapshot_id_fc)
                    if parent_snapshot_fc and parent_snapshot_fc.\
                            get('snapshotLunId') is not None:
                        volume_file[
                            'parent_snapshot_url'] = \
                            'http://' + sd_sn + '/' + parent_snapshot_fc.get(
                            'snapshotLunId')
                else:
                    volume_file['parent_snapshot_url'] = 'http://' + \
                                                         ext_params_dic[
                                                             'dsMgntIp'] + \
                                                         '/' + \
                                                         ext_params_dic[
                                                             'dsResourceId'] +\
                                                         '/' + \
                                                         parent_snapshot_id_fc
            else:
                raise exception.InvalidVolumeMetadata(
                    reason="snapshot_id is none")

        LOG.info(_("vol_first_id is %s") % vol_first_id)
        vol_first_os = self.db.volume_get(context, vol_first_id)
        vol_first_meta = vol_first_os.get('volume_metadata')
        vol_first_meta_dict = metadata_to_dict(vol_first_meta)
        volume_first_urn = vol_first_meta_dict.get('urn')
        fc_first_vol_id = volume_first_urn[volume_urn.rfind(':') + 1:]
        LOG.info(_("fc_first_vol_id is %s") % fc_first_vol_id)

        vol_source_fc = self.volume_proxy.query_volume(id=fc_first_vol_id)
        LOG.info(_("vol_source_fc linkCloneParent is %s") % vol_source_fc[
            'linkCloneParent'])
        if vol_source_fc['linkCloneParent'] is not None:
            volume_file['is_clone_volume'] = True
            try:
                vol_linked_fc = self.volume_proxy.query_volume(
                    id=vol_source_fc['linkCloneParent'])
                if storage_type == 'advanceSan':
                    volume_file['clone_volume_url'] = 'http://' + \
                                                      vol_linked_fc[
                                                          'storageSN'] \
                                                      + '/' + vol_linked_fc[
                                                          'lunId']
                else:
                    linked_ext_params_dic = json.loads(
                        vol_linked_fc.get('drExtParams'))
                    volume_file['clone_volume_url'] = 'http://' + \
                                                      linked_ext_params_dic[
                                                          'dsMgntIp'] \
                                                      + '/' + \
                                                      linked_ext_params_dic[
                                                          'dsResourceId'] \
                                                      + '/' + vol_source_fc[
                                                          'linkCloneParent']
            except Exception:
                LOG.error(_("clone colume not exit"))
                volume_file['clone_volume_url'] = None
        else:
            volume_file['is_clone_volume'] = False
            volume_file['clone_volume_url'] = None

        volume_file['snapshot_name'] = last_snap_os['name']
        volume_file['snapshot_id'] = last_snap_id_fc

        LOG.info(_("[VRM-CINDER] volume_file [%s]"), volume_file)

        volume_file['volume_size'] = vol_second_os['size']
        LOG.info(_("[VRM-CINDER] volume_file [%s]"), volume_file)
        if storage_type == 'advanceSan':
            last_snapshot = self.volume_snapshot_proxy.query_volumesnapshot(
                uuid=last_snap_id_fc)
            if last_snapshot and last_snapshot.get(
                    'snapshotLunId') is not None:
                volume_file['snapshot_url'] = 'http://' + sd_sn \
                                              + '/' + last_snapshot.get(
                    'snapshotLunId')
        else:
            volume_file['snapshot_url'] = \
                'http://' + ext_params_dic['dsMgntIp'] + '/' + \
                ext_params_dic['dsResourceId']  + '/' + last_snap_id_fc

        volume_file['bootable'] = False
        volume_file['image_id'] = None
        try:
            vol_image_meta = \
                self.volume_api.get_volume_image_metadata(context,
                                                          vol_second_os)
            if vol_image_meta:
                vol_image_meta_dic = dict(vol_image_meta.iteritems())
                volume_file['image_id'] = vol_image_meta_dic.get('image_id')
                volume_file['bootable'] = True

        except exception.CinderException as ex:
            LOG.error(
                _('[BRM-DRIVER] get_volume_image_metadata is error [%s]'), ex)

        if vol_first_os.get('bootable') is True:
            volume_file['bootable'] = True

        LOG.info(_("[VRM-CINDER] volume_file [%s]"), volume_file)

        try:
            backup_service.backup(backup, volume_file)
        finally:
            LOG.info(('cleanup for volume %s.') % vol_second_os['name'])

    def restore_backup(self, context, backup, volume, backup_service):
        """Restore an existing backup to a new or existing volume."""

        LOG.info(('restore_backup for volume %s.') % volume['name'])
        volume_file = {}

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        volume_urn = vol_meta_dict.get('urn')
        fc_vol_id = volume_urn[volume_urn.rfind(':') + 1:]
        vol_second_fc = self.volume_proxy.query_volume(id=fc_vol_id)
        ext_params = vol_second_fc.get('drExtParams')
        if ext_params:
            LOG.info(_("[VRM-CINDER] ext_params [%s]"), ext_params)
            ext_params_dic = json.loads(vol_second_fc.get('drExtParams'))
            LOG.info(_("[VRM-CINDER] ext_params [%s]"), ext_params_dic)
        storage_type = vol_second_fc.get('storageType')
        volume_file['restore_type'] = 0
        if backup['volume_id'] == volume['id']:
            volume_file['restore_type'] = 1
            service_meta = backup['service_metadata']
            str_service_meta = json.loads(service_meta)
            last_snapshot_id_openstack = str_service_meta.get('snap_id')
            last_snapshot_id_fc = str(last_snapshot_id_openstack).replace('-',
                                                                          '')
            last_snapshot_fc = self.volume_snapshot_proxy.query_volumesnapshot(
                uuid=last_snapshot_id_fc)
            if last_snapshot_fc:  # only the last backup has snapshot
                if storage_type == 'advanceSan':
                    volume_file['lastest_snapshot_url'] = 'http://' + \
                                                          vol_second_fc[
                                                              'storageSN'] +\
                                                          '/' + \
                                                          last_snapshot_fc[
                                                              'snapshotLunId']
                else:
                    volume_file['lastest_snapshot_url'] = 'http://' + \
                                                          ext_params_dic[
                                                              'dsMgntIp'] \
                                                          + '/' + \
                                                          ext_params_dic[
                                                              'dsResourceId'] \
                                                          + '/' + \
                                                          last_snapshot_id_fc

        if storage_type == 'advanceSan':
            volume_file['storage_type'] = 4
            pvscsi_support = vol_second_fc.get('pvscsiSupport')
            if pvscsi_support is not None and pvscsi_support == 1:
                volume_file['storage_type'] = 5  # RDM: direct io volume
            volume_file['volume_url'] = 'http://' + vol_second_fc[
                'storageSN'] + '/' + vol_second_fc['lunId']
        else:
            volume_file['storage_type'] = 0
            volume_file['volume_url'] = vol_second_fc['volInfoUrl']
            LOG.info(_("[VRM-CINDER] volume_file [%s]"), volume_file)
            volume_file['storage_ip'] = ext_params_dic['dsMgntIp']
            volume_file['storage_pool_id'] = ext_params_dic['dsResourceId']
            volume_file['volume_offset'] = True
            volume_file['volume_name'] = vol_second_fc['volNameOnDev']

        if vol_second_fc['linkCloneParent'] is not None:
            try:
                vol_linked_fc = self.volume_proxy.query_volume(
                    id=vol_second_fc['linkCloneParent'])
                if vol_second_fc['storageType'] == 'advanceSan':
                    volume_file['clone_volume_url'] = 'http://' + \
                                                      vol_linked_fc[
                                                          'storageSN'] + \
                                                      vol_linked_fc['lunId']
                else:
                    linked_ext_params_dic = json.loads(
                        vol_linked_fc.get('drExtParams'))
                    volume_file['clone_volume_url'] = 'http://' + \
                                                      linked_ext_params_dic[
                                                          'dsMgntIp'] \
                                                      + '/' + \
                                                      linked_ext_params_dic[
                                                          'dsResourceId'] \
                                                      + '/' + vol_second_fc[
                                                          'linkCloneParent']
            except Exception:
                LOG.error(_("clone colume not exit"))
                volume_file['clone_volume_url'] = None
        else:
            volume_file['clone_volume_url'] = None

        try:
            backup_service.restore(backup, volume['id'], volume_file)
        finally:
            LOG.info(('cleanup for volume %s.') % volume['name'])

    def delete_cgsnapshot(self, context, cgsnapshot):
        """Delete a cgsnapshot."""
        model_update = {}
        model_update['status'] = cgsnapshot['status']
        cgsnapshot_id = cgsnapshot['id']
        LOG.info(_('[BRM-DRIVER] start to delete cgsnapshot [%s]'),
                 cgsnapshot_id)

        snapshots = self.db.snapshot_get_all_for_cgsnapshot(context,
                                                            cgsnapshot_id)
        if snapshots and len(snapshots) > 0:
            for snapshot in snapshots:
                snapshot_id = snapshot['id']
                snapshot_uuid = str(snapshot_id).replace('-', '')
                body = self.volume_snapshot_proxy.query_volumesnapshot(
                    uuid=snapshot_uuid)
                if body is None:
                    msg = _(
                        '[BRM-DRIVER] snapshot [%s] is not exist while deleting'
                        ' cgsnapshot [%s]'),
                    snapshot_uuid,
                    cgsnapshot_id
                    LOG.warn(msg)
                    snapshot['status'] = 'deleted'
                    continue
                try:
                    self.volume_snapshot_proxy.delete_volumesnapshot(
                        id=snapshot_uuid)
                    snapshot['status'] = 'deleted'
                except Exception as ex:
                    msg = _('[BRM-DRIVER] delete snapshot [%s] error while '
                            'deleting cgsnapshot [%s]'), snapshot_uuid, \
                          cgsnapshot_id
                    LOG.error(msg)
                    LOG.exception(ex)
                    snapshot['status'] = 'error_deleting'
                    model_update['status'] = 'error_deleting'
        return model_update, snapshots

    def create_cgsnapshot(self, context, cgsnapshot):
        model_update = {}
        consistencygroup_id = cgsnapshot['consistencygroup_id']
        cgsnapshot_id = cgsnapshot['id']
        snapshots = self.db.snapshot_get_all_for_cgsnapshot(context,
                                                            cgsnapshot_id)
        volumes = self.db.volume_get_all_by_group(context, consistencygroup_id)
        snapshot_uuids = []
        if volumes is None or len(volumes) <= 0:
            return model_update
        else:
            for volume_ref in volumes:
                for snapshot in snapshots:
                    if snapshot['volume_id'] == volume_ref['id']:
                        vol_meta = volume_ref['volume_metadata']
                        vol_meta_dict = metadata_to_dict(vol_meta)
                        vol_urn = vol_meta_dict.get('urn')
                        if vol_urn is None:
                            LOG.warn(_(
                                "vol_urn  is null while creating cgsnapshot."))
                        snapshot_id = snapshot['id']
                        snapshot_uuid = str(snapshot_id).replace('-', '')
                        msg = _(
                            "creating snapshot snapshot_uuid  is [%s], "
                            "vol_urn is [%s]"), snapshot_uuid, vol_urn
                        LOG.info(msg)
                        try:
                            body = self.volume_snapshot_proxy. \
                                create_volumesnapshot(
                                snapshot_uuid=snapshot_uuid, vol_urn=vol_urn,
                                enable_active=False)
                        except Exception as ex:
                            LOG.exception(ex)
                            snapshot['status'] = 'error'
                            model_update['status'] = 'error'

                        if body['urn'] is None:
                            snapshot['status'] = 'error'
                            model_update['status'] = 'error'
                        snapshot_uuids.append(snapshot_uuid)
            try:
                self.volume_snapshot_proxy.active_snapshots(
                    snapshot_uuids=snapshot_uuids)
            except Exception as ex:
                LOG.exception(ex)
                for snapshot in snapshots:
                    snapshot['status'] = 'error'
                model_update['status'] = 'error'

        return model_update, snapshots

    def _get_cluster_by_dsurn(self, ds_urn):
        """Return clusterUrn by dsurn
        """
        # datastore_id = ds_urn[ds_urn.rfind(':') + 1:]
        clusters = self.cluster_proxy.list_cluster()
        random.shuffle(clusters)
        for cluster in clusters:
            cluster_urn = cluster.get('urn')
            args_dict = {}
            args_dict['scope'] = cluster_urn
            try:
                datastores = self.datastore_proxy.list_datastore(**args_dict)
                for datastore in datastores:
                    if datastore['urn'] == ds_urn:
                        hosts = self.cluster_proxy.list_hosts(
                            clusterUrn=cluster_urn)
                        for host in hosts:
                            if host["status"] == "normal" and \
                                            host["isMaintaining"] is False:
                                return cluster_urn

            except Exception as ex:
                LOG.error(
                    _('[BRM-DRIVER] get_volume_image_metadata is error [%s]'),
                    ex)
        msg = _('[BRM-DRIVER] get cluster is none')
        LOG.error(msg)
        raise exception.VolumeDriverException(message=msg)


