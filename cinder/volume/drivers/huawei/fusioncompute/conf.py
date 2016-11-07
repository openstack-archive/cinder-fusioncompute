# vim: tabstop=4 shiftwidth=4 softtabstop=4
"""
[VRM DRIVER] CONFIG
"""

from oslo_config import cfg


vrm_http_opts = [
    cfg.StrOpt('vrm_version',
               default='v6.0',
               help='Management version of VRM'),
    cfg.BoolOpt('vrm_ssl',
                default=True,
                help='Use SSL connection'),
    cfg.StrOpt('vrm_port',
               default=7443,
               help='Management port of VRM'),
    cfg.StrOpt('vrm_user',
               default='gesysman',
               help='User name for the VRM'),
    cfg.StrOpt('vrm_password',
               default='f57d750a00797c3c63da3f7bb6c9ffe7dbf2126af90019bfcaf753390064135c',
               help='Password for the VRM'),
    cfg.StrOpt('vrm_authtype',
               default='0',
               help='User name for the VRM'),
    cfg.StrOpt('vrm_usertype',
               default='2',
               help='User type for the VRM'),
    cfg.IntOpt('vrm_retries',
               default=3,
               help='retry times for http request'),
    cfg.IntOpt('vrm_snapshot_sleeptime',
               default=300,
               help='sleep times for retry request'),
    cfg.IntOpt('vrm_vol_snapshot_retries',
               default=3,
               help='retry times for http request'),
    cfg.IntOpt('vrm_timeout',
               default=30000,
               help='timeout(s) for task result'),
    cfg.IntOpt('vrm_limit',
               default=100,
               help='limit per step for retrive mass reources result set'),
    cfg.IntOpt('vrm_sm_periodrate',
               default=1,
               help='timeout(s) for task result'),
]
VRM_group = cfg.OptGroup(name='VRM', title='VRM config')
VRM_opts = [
    cfg.StrOpt('fc_user',
               help='FusionCompute user name'),
    cfg.StrOpt('fc_pwd_for_cinder', secret=True,
               help='FusionCompute user password'),
    cfg.StrOpt('fc_ip',
               help='Management IP of FusionCompute'),
    cfg.StrOpt('fc_image_path',
               help='NFS Image server path'),
    cfg.StrOpt('glance_server_ip',
               help='FusionSphere glance server ip'),
    cfg.StrOpt('s3_store_access_key_for_cinder', secret=True,
               help='FusionCompute uds image access key'),
    cfg.StrOpt('s3_store_secret_key_for_cinder', secret=True,
               help='FusionCompute uds image secret key'),
    cfg.StrOpt('export_image_type',
               help='FusionCompute export image type, such as: nfs, glance, uds'),
    cfg.StrOpt('uds_ip',
               help='FusionSphere uds server ip'),
    cfg.StrOpt('uds_port',
               help='FusionSphere uds server port'),
    cfg.StrOpt('uds_bucket_name',
               help='FusionSphere uds server bucket name'),
    cfg.StrOpt('uds_bucket_type',
               help='FusionSphere uds server bucket type fixed or wildcard'),
    cfg.ListOpt('vrm_ds_types',
                default=['advanceSan', 'DSWARE', 'san', 'NAS', 'LUNPOME', 'LOCAL', 'LOCALPOME'],
                help='Management port of VRM'),
    cfg.StrOpt('vrm_ds_hosts_share',
               default='false',
               help='FusionSphere support all host model'),
    cfg.StrOpt('vrm_is_thin',
               default='true',
               help='FusionSphere create volume default type'),
    cfg.StrOpt('export_version',
               default='v6.0',
               help='export version of VHD'),
]
CONF = cfg.CONF
CONF.register_group(VRM_group)
CONF.register_opts(VRM_opts, VRM_group)
CONF.register_opts(vrm_http_opts)
FC_DRIVER_CONF = CONF.VRM


