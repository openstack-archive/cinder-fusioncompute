# vim: tabstop=4 shiftwidth=4 softtabstop=4
"""
[VRM DRIVER] EXCEPTION
"""

from cinder import exception

"""
Exception definitions.
"""


class UnsupportedFeature(exception.CinderException):
    '''UnsupportedFeature

    UnsupportedFeature
    '''
    pass


class UnsupportedVersion(UnsupportedFeature):
    '''UnsupportedVersion

    UnsupportedVersion
    '''
    pass


class UnsupportedCommand(UnsupportedFeature):
    '''UnsupportedCommand

    UnsupportedCommand
    '''
    pass


class AuthorizationFailure(exception.CinderException):
    '''AuthorizationFailure

    AuthorizationFailure
    '''
    pass


class NoNeededData(exception.CinderException):
    '''NoNeededData

    NoNeededData
    '''
    pass


class ClientException(exception.CinderException):
    '''ClientException

    ClientException
    '''
    def __init__(self, code=None, message=None, error_code=None, error_des=None):

        self.code = code
        self.errorCode = error_code
        self.errorDes = error_des
        if message:
            self.message = message
        else:
            self.message = "client exception."
        super(ClientException, self).__init__(self.message)

    def __str__(self):
        formatted_string = "%s (HTTP %s)" % (self.message, self.code)
        if self.errorCode:
            formatted_string += " (errorCode: %s)" % self.errorCode

        if self.errorDes:
            formatted_string += " (errorDes: %s)" % self.errorDes

        return formatted_string


class BadRequest(ClientException):
    """BadRequest

    HTTP 400 - Bad request: you sent some malformed data.
    """
    http_status = 400
    message = "Bad request"


class Unauthorized(ClientException):
    """Unauthorized

    HTTP 401 - Unauthorized: bad credentials.
    """
    http_status = 401
    message = "Unauthorized"


class Forbidden(ClientException):
    """Forbidden

    HTTP 403 - Forbidden: your credentials don't give you access to this
    resource.
    """
    http_status = 403
    message = "Forbidden"


class NotFound(ClientException):
    """NotFound

    HTTP 404 - Not found
    """
    http_status = 404
    message = "Not found"


class OverLimit(ClientException):
    """OverLimit

    HTTP 413 - Over limit: you're over the API limits for this time period.
    """
    http_status = 413
    message = "Over limit"


class HTTPNotImplemented(ClientException):
    """HTTPNotImplemented

    HTTP 501 - Not Implemented: the server does not support this operation.
    """
    http_status = 501
    message = "Not Implemented"


class FusionComputeDriverException(ClientException):
    '''FusionComputeDriverException

    FusionComputeDriverException
    '''
    http_status = 500
    message = "FusionCompute driver exception occurred."


_code_map = dict((c.http_status, c) for c in [BadRequest, Unauthorized,
                                              Forbidden, NotFound,
                                              OverLimit, HTTPNotImplemented])


def exception_from_response(response, body):
    """
    Return an instance of an ClientException or subclass
    based on an requests response.

    Usage::

        resp, body = requests.request(...)
        if resp.status_code != 200:
            raise exception_from_response(resp, rest.text)
    """
    cls = _code_map.get(response.status_code, ClientException)
    if body:
        error_code = body.get('errorCode', None)
        error_des = body.get('errorDes', None)
        return cls(code=response.status_code, errorCode=error_code, errorDes=error_des)
    else:
        return cls(code=response.status_code)
