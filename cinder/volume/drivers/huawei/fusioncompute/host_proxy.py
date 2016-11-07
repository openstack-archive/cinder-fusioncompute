# vim: tabstop=4 shiftwidth=4 softtabstop=4

"""
[VRM DRIVER] VRM CLIENT.

"""

from oslo_log import log as logging

from cinder.volume.drivers.huawei.vrm.base_proxy import BaseProxy


TASK_WAITING = 'waiting'
TASK_RUNNING = 'running'
TASK_SUCCESS = 'success'
TASK_FAILED = 'failed'
TASK_CANCELLING = 'cancelling'
TASK_UNKNOWN = 'unknown'

LOG = logging.getLogger(__name__)


class HostProxy(BaseProxy):
    '''HostProxy

    '''
    def __init__(self):
        super(HostProxy, self).__init__()

    def list_host(self, **kwargs):
        '''list_host

        :param kwargs:
        :return:
        '''
        # LOG.info(_("[VRM-CINDER] start list_host()"))
        uri = '/hosts'
        method = 'GET'
        path = self.site_uri + uri

        offset = 0
        hosts = []
        while True:
            parameters = {'limit': self.limit,
                          'offset': offset,
                          'scope': kwargs.get('scope')}

            appendix = self._joined_params(parameters)
            new_url = self._generate_url(path, appendix)
            resp, body = self.vrmhttpclient.request(new_url, method)
            total = int(body.get('total') or 0)
            if total > 0:
                res = body.get('hosts')
                hosts += res
                offset += len(res)
                if offset >= total or len(hosts) >= total or len(res) < self.limit:
                    break
            else:
                break

        return hosts


