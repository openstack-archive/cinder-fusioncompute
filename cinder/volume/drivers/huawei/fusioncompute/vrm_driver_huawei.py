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
[VRM DRIVER] 

"""

from oslo_config import cfg
from oslo_log import log as logging

from cinder import context as cinder_context
from cinder.i18n import _
from cinder.volume.drivers.huawei.vrm.vrm_driver import VRMDriver

try:
    from eventlet import sleep
except ImportError:
    from time import sleep


def metadata_to_dict(metadata):
    result = {}
    for item in metadata:
        if not item.get('deleted'):
            result[item['key']] = item['value']
    return result


LOG = logging.getLogger(__name__)
CONF = cfg.CONF


class VRMDriverHuawei(VRMDriver):
    VENDOR = 'Huawei'
    BACKEND = 'VRM'
    VERSION = 'v1.1'

    def __init__(self, *args, **kwargs):
        '''__init__

        :param args:
        :param kwargs:
        :return:
        '''
        super(VRMDriverHuawei, self).__init__(*args, **kwargs)

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
        LOG.info(_("[CINDER-VRM] start create_volume() "))

        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        context = cinder_context.get_admin_context()

        temp_str = vol_meta_dict.get('urn')
        fc_vol_id = temp_str[temp_str.rfind(':') + 1:]

        name = volume.get('name')
        LOG.info(
            _("[CINDER-VRM] fc_vol_id [%s], name is %s ") % (fc_vol_id, name))
        model_update, metadata = self._vrm_get_volume_meta(id=fc_vol_id)
        self.db.volume_metadata_update(context, volume['id'], metadata, False)
        try:
            self.volume_proxy.update_custom_properties(volume_urn=temp_str,
                                                       external_uuid=volume[
                                                           'id'])
            # modify volume name
            self.volume_proxy.modify_volume(volume_id=fc_vol_id, name=name)
        except Exception as ex:
            LOG.error(
                _("[CINDER-VRM] modify volume information failed after create "
                  "volume.volume id is [%s] ") % fc_vol_id)
            LOG.exception(ex)
        return model_update

    def delete_volume(self, volume):
        '''delete_volume

        :param volume:
        :return:
        '''
        LOG.info(_("[CINDER-VRM] start delete_volume() "))
        vol_meta = volume.get('volume_metadata')
        vol_meta_dict = metadata_to_dict(vol_meta)
        fc_volume_name = vol_meta_dict.get('fc_volume_name')
        temp_str = vol_meta_dict.get('urn')
        LOG.info(_("[CINDER-VRM] urn is %s ,fc_volume_name is %s") % (
        temp_str, fc_volume_name))
        fc_vol_id = temp_str[temp_str.rfind(':') + 1:]
        try:
            self.volume_proxy.update_custom_properties(volume_urn=temp_str,
                                                       external_uuid="")
            # modify volume name
            self.volume_proxy.modify_volume(volume_id=fc_vol_id,
                                            name=fc_volume_name)
        except Exception as ex:
            LOG.error(
                _(
                    "[CINDER-VRM] modify volume information failed after create"
                    " volume.volume id is [%s] ") % fc_vol_id)
            LOG.exception(ex)
        return

    def _try_get_volume_stats(self, refresh=False):
        '''_try_get_volume_stats

        :param refresh:If 'refresh' is True, run the update first.
        :return:Return the current state of the volume service.
        '''
#        LOG.info(_("[BRM-DRIVER] start _try_get_volume_stats() "))
        if refresh:
            self.left_periodrate -= 1
            if self.left_periodrate <= 0:
                self._refresh_storage_info(refresh)

        vrm_driver.py        stats = self._build_volume_stats()
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
                    free_capacity_gb="infinite",
                    reserved_percentage=self.reserved_percentage,
                    total_capacity_gb="infinite",
                    provisioned_capacity_gb=datastore['usedSizeGB'],
                    max_over_subscription_ratio=self.over_ratio,
                    affine_rate=self.affine_rate
                ))
            else:
                new_pool.update(dict(
                    pool_name=pool,
                    free_capacity_gb="infinite",
                    reserved_percentage=self.reserved_percentage,
                    total_capacity_gb="infinite",
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
