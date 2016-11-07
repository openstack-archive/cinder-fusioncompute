"""
    FC Driver utils function
"""
import hashlib
import sys
import traceback

from cinder.i18n import _
from oslo_log import log as logging

LOG = logging.getLogger(__name__)


def log_exception(exception=None):
    """log_exception

    :param exception:
    :return:
    """

    if exception:
        # TODO
        pass

    etype, value, track_tb = sys.exc_info()
    error_list = traceback.format_exception(etype, value, track_tb)
    for error_info in error_list:
        LOG.error(error_info)


def str_drop_password_key(str_data):
    """str_drop_password_key

    remove json password key item
    :param data:
    :return:
    """

    dict_data = eval(str_data)
    if isinstance(dict_data, dict):
        drop_password_key(dict_data)
        return str(dict_data)
    else:
        LOG.info(_("[BRM-DRIVER] str_data can't change to dict, str_data:(%s) "), str_data)
        return


def drop_password_key(data):
    """
    remove json password key item
    :param data:
    :return:
    """
    encrypt_list = ['password', 'vncpassword', 'oldpassword',
                    'domainpassword', 'vncoldpassword', 'vncnewpassword',
                    'auth_token', 'token', 'fc_pwd', 'accessKey',
                    'secretKey']
    for key in data.keys():
        if key in encrypt_list:
            del data[key]
        elif data[key] and isinstance(data[key], dict):
            drop_password_key(data[key])

def sha256_based_key(key):
    """sha256_based_key

    generate sha256 based key
    :param key:
    :return:
    """
    hash_ = hashlib.sha256()
    hash_.update(key)
    return hash_.hexdigest()

Delete_Snapshot_Code = ['10400004']