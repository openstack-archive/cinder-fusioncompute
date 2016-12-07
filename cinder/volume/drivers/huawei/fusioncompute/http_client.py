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
import requests
import urlparse

from oslo_config import cfg
from oslo_log import log as logging

from cinder.i18n import _
from cinder.volume.drivers.huawei.fusioncompute.conf import FC_DRIVER_CONF
from cinder.volume.drivers.huawei.fusioncompute import exception as driver_exception
from cinder.volume.drivers.huawei.fusioncompute import utils as apiutils

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


class VRMHTTPClient(object):
    """Executes volume driver commands on VRM."""

    USER_AGENT = 'VRM-HTTP-Client for OpenStack'
    RESOURCE_URI = 'uri'
    TASK_URI = 'taskUri'
    BASIC_URI = '/service'
    vrm_commands = None

    def __init__(self):
        '''VRMHTTPClient init

        __init__

        :return:
        '''
        fc_ip = FC_DRIVER_CONF.fc_ip
        fc_image_path = FC_DRIVER_CONF.fc_image_path
        fc_user = FC_DRIVER_CONF.fc_user
        fc_pwd = FC_DRIVER_CONF.fc_pwd_for_cinder
        self.ssl = CONF.vrm_ssl
        self.host = fc_ip
        self.port = CONF.vrm_port
        self.user = fc_user
        self.userType = CONF.vrm_usertype
        self.password = apiutils.sha256_based_key(fc_pwd)
        self.retries = CONF.vrm_retries
        self.timeout = CONF.vrm_timeout
        self.limit = CONF.vrm_limit
        self.image_url = fc_image_path
        self.image_type = '.xml'

        self.versions = None
        self.version = None
        self.auth_uri = None
        self.auth_url = None

        self.auth_token = None

        self.sites = None
        self.site_uri = None
        self.site_urn = None
        self.site_url = None

        self.shared_hosts = None
        self.shared_datastores = None
        self.shared_volumes = None

    def _generate_url(self, path, query=None, frag=None):
        '''_generate_url

        _generate_url

        :param path:
        :param query:
        :param frag:
        :return:
        '''
        if CONF.vrm_ssl:
            scheme = 'https'
        else:
            scheme = 'http'
        fc_ip = FC_DRIVER_CONF.fc_ip

        netloc = str(fc_ip) + ':' + str(CONF.vrm_port)
        if path.startswith(self.BASIC_URI):
            url = urlparse.urlunsplit((scheme, netloc, path, query, frag))
        else:
            url = urlparse.urlunsplit(
                (scheme, netloc, self.BASIC_URI + str(path), query, frag))
        return url

    def _http_log_req(self, args, kwargs):
        '''_http_log_req

        _http_log_req

        :param args:
        :param kwargs:
        :return:
        '''
        string_parts = ['\n curl -i']
        for element in args:
            if element in ('GET', 'POST', 'DELETE', 'PUT'):
                string_parts.append(' -X %s' % element)
            else:
                string_parts.append(' %s' % element)

        for element in kwargs['headers']:
            header = ' -H "%s: %s"' % (element, kwargs['headers'][element])
            string_parts.append(header)

        if 'body' in kwargs:
            string_parts.append(" -d '%s'" % (kwargs['body']))

    def _http_log_resp(self, resp):
        '''_http_log_resp

        _http_log_resp

        :param resp:
        :return:
        '''
        try:
            if resp.status_code:
                if int(resp.status_code) != 200:
                    LOG.info(_("RESP status_code: [%s]"), resp.status_code)
        except Exception:
            LOG.info(_("[VRM-CINDER] _http_log_resp exception"))

    def request(self, url, method, **kwargs):
        '''request

        request

        :param url:
        :param method:
        :param kwargs:
        :return:
        '''
        auth_attempts = 0
        attempts = 0
        step = 1
        while True:
            step *= 2
            attempts += 1
            if not self.auth_url or not self.auth_token or not self.site_uri:
                LOG.info(_("[VRM-CINDER] auth_url is none. "))

            kwargs.setdefault('headers', {})['X-Auth-Token'] = self.auth_token
            try:
                resp, body = self.try_request(url, method, **kwargs)
                return resp, body

            except driver_exception.Unauthorized as e:
                LOG.error('[VRM-CINDER] error message is :%s' % e)
                if e.errorCode == '10000040':
                    if auth_attempts > 2:
                        LOG.error("license error.")
                        raise driver_exception.ClientException(101)
                if auth_attempts > 10:
                    raise driver_exception.ClientException(101)
                LOG.info("Unauthorized, reauthenticating.")
                attempts -= 1
                auth_attempts += 1
                sleep(step)
                self.authenticate()
                continue
            except driver_exception.ClientException as ex:
                if attempts > self.retries:
                    LOG.info(_("[VRM-CINDER] ClientException "))
                    raise ex
                if 500 <= ex.code <= 599:
                    LOG.info(_("[VRM-CINDER] ClientException "))
                else:
                    LOG.info(_("[VRM-CINDER] ClientException "))
                    raise ex
            except requests.exceptions.ConnectionError as ex:
                LOG.error("Connection Error: %s" % ex)
                LOG.error("Connection Error: %s" % ex.message)
                raise ex
            LOG.info(
                "Failed attempt(%s of %s), retrying in %s seconds" %
                (attempts, self.retries, step))
            sleep(step)

    def try_request(self, url, method, **kwargs):
        '''try_request

        request

        :param url:
        :param method:
        :param kwargs:
        :return:
        '''

        no_version = False
        if not self.version:
            no_version = True
        if url.endswith('session'):
            no_version = True

        kwargs.setdefault('headers', kwargs.get('headers', {}))
        kwargs['headers']['User-Agent'] = self.USER_AGENT
        if no_version:
            kwargs['headers']['Accept'] = 'application/json;charset=UTF-8'
        else:
            version = self.version.lstrip(' v')
            if url.endswith('/action/export'):
                export_version = FC_DRIVER_CONF.export_version
                version = '1.2' if export_version == 'v1.2' else \
                    self.version.lstrip(' v')
            kwargs['headers']['Accept'] = 'application/json;version=' + \
                                          version + ';charset=UTF-8'
            kwargs['headers']['X-Auth-Token'] = self.auth_token
        kwargs['headers']['Accept-Language'] = 'en_US'
        if 'body' in kwargs:
            if kwargs['body'] and len(kwargs['body']) > 0:
                kwargs['headers'][
                    'Content-Type'] = 'application/json;charset=UTF-8'
                kwargs['data'] = kwargs['body']

            #body = apiutils.str_drop_password_key(kwargs['body'])
            # LOG.info(_("[VRM-CINDER] request body [%s]"), body)
            del kwargs['body']

        self._http_log_req((url, method,), kwargs)
        resp = requests.request(
            method,
            url,
            verify=False,
            **kwargs)
        self._http_log_resp(resp)

        if resp.content:
            try:
                body = json.loads(resp.content)
            except ValueError:
                body = None
        else:
            body = None
#        LOG.info(_("[VRM-CINDER] request status_code [%d]"), resp.status_code)
        if resp.status_code >= 400:
            LOG.error(_("error response, error is %s"), body)
            raise driver_exception.exception_from_response(resp, body)

        return resp, body

    def _prepare_version_and_auth_url(self):
        '''_prepare_version_and_auth_url

        _prepare_version_and_auth_url

        :return:
        '''
        self.version = CONF.vrm_version
        self.auth_uri = '/service/session'
        self.auth_url = self._generate_url(self.auth_uri)

    def _prepare_auth_token(self):
        '''_prepare_auth_token

        _prepare_auth_token

        :return:
        '''
        uri = '/service/session'
        new_url = self._generate_url(uri)
        # self.auth_token = None
        headers = {'X-Auth-User': self.user,
                   'X-Auth-Key': self.password,
                   'X-Auth-UserType': self.userType, }
        resp, body = self.try_request(new_url, 'POST', headers=headers)
        if resp.status_code in (200, 204):
            self.auth_token = resp.headers['x-auth-token']

    def _prepare_site_uri(self):
        '''_prepare_site_uri

        _prepare_site_uri

        :return:
        '''
        self.site_uri = self.site_urn = self.site_url = None
        url = self._generate_url('/sites')
        headers = {'X-Auth-Token': self.auth_token}

        resp, body = self.try_request(url, 'GET', headers=headers)
        if resp.status_code in (200, 204):
            self.sites = body['sites']
            if len(self.sites) == 1:
                self.site_uri = self.sites[0]['uri']
                self.site_urn = self.sites[0]['urn']
                self.site_url = self._generate_url(self.site_uri)
                return
            else:
                for si in self.sites:
                    if si['urn'] == FC_DRIVER_CONF.vrm_siteurn:
                        self.site_uri = si['uri']
                        self.site_urn = si['urn']
                        self.site_url = self._generate_url(self.site_uri)
                        return

                raise driver_exception.NotFound()

    def authenticate(self):
        '''authenticate

        authenticate

        :return:
        '''
        self._prepare_version_and_auth_url()
        self._prepare_auth_token()
        self._prepare_site_uri()

        if not self.version:
            LOG.info(_("[VRM-CINDER] (%s)"), 'AuthorizationFailure')
            raise driver_exception.AuthorizationFailure
        if not self.auth_url:
            LOG.info(_("[VRM-CINDER] (%s)"), 'AuthorizationFailure')
            raise driver_exception.AuthorizationFailure
        if not self.site_uri:
            LOG.info(_("[VRM-CINDER] (%s)"), 'AuthorizationFailure')
            raise driver_exception.AuthorizationFailure

    def get_version(self):
        '''get_version

        get_version

        :return:
        '''
        return self.version

    def get_siteurn(self):
        '''get_siteurn

        get_siteurn

        :return:
        '''
        if self.site_uri is None:
            self.init()
        return self.site_urn

    def get_siteuri(self):
        '''get_siteuri

        get_siteurn

        :return:
        '''
        if self.site_uri is None:
            self.init()
        return self.site_uri

    def init(self):
        '''init

        init

        :return:
        '''
        LOG.info(_("[VRM-CINDER] start init()"))
        self.authenticate()




