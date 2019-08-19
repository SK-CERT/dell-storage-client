""" This module contains Session for communication with Dell Storage Manager (DSM) API. """
from typing import Optional

import urllib3
import requests
from requests.auth import HTTPBasicAuth
from requests.structures import CaseInsensitiveDict

from dell_storage_api.storage_center import StorageCenter, StorageCenterCollection


class DsmSession:
    """
    This class represents HTTP Session with Dell Storage Manager (DSM). After successful login, underlying
    requests.Session object holds login cookie used to authorize all further requests to DSM API until its expiration.
    DsmSession object holds two important properties, 'base_url' and 'session' which are passed down to child objects
    like Storage Centers, Servers or Volumes. These child elements can then perform their own specific API calls to DSM
    by combining base_url, specific API endpoint and their unique Instance ID to create complete API endpoint URL and
    send requests to this complete endpoint using authenticated session.
    """
    API_VERSION_HEADER = 'x-dell-api-verions'
    LOGIN_ENDPOINT = '/ApiConnection/Login'
    LOGOUT_ENDPOINT = '/ApiConnection/Logout'
    STORAGE_CENTER_LIST_ENDPOINT = '/ApiConnection/ApiConnection/%s/StorageCenterList'

    def __init__(self, username: str, password: str, host: str, port: int = 3033,
                 api_version: str = '3.0', verify_cert: bool = True) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._auth = HTTPBasicAuth(username, password)
        self._api_version = api_version
        self.base_url = 'https://%s:%s/api/rest' % (host, port)
        self.session = requests.Session()
        self.session.headers = CaseInsensitiveDict({'Content-Type': 'application/json',
                                                    'Accept': 'application/json',
                                                    self.API_VERSION_HEADER: self._api_version})
        self.session.verify = verify_cert
        if not verify_cert:
            # Silence Warning about untrusted certificates if 'verify_cert' is None
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.conn_instance_id = None

    @property
    def api_version(self) -> str:
        """
        Return API version used by DSM
        :return: DSM API version
        """
        return self._api_version

    @api_version.setter
    def api_version(self, value: str) -> None:
        """
        Set API version used by DSM
        :param value: New DSM API version
        :return: None
        """
        self._api_version = value
        self.session.headers[self.API_VERSION_HEADER] = value

    @property
    def username(self) -> str:
        """
        Return username used to authenticate this session with DSM
        :return: Username used for authentication with DSM
        """
        return self._username

    @username.setter
    def username(self, value: str) -> None:
        """
        Set username used to authenticate this session with DSM
        :param value: New Username
        :return: None
        """
        self._username = value
        self._auth.username = value

    @property
    def login_url(self) -> str:
        """
        Return complete URL to API login endpoint
        :return: URL for login to DSM
        """
        return self.base_url + self.LOGIN_ENDPOINT

    @property
    def logout_url(self) -> str:
        """
        Return complete URL for API logout endpoint
        :return: URL for logout from DSM
        """
        return self.base_url + self.LOGOUT_ENDPOINT

    @property
    def sc_list_url(self) -> Optional[str]:
        """
        Return complete URL for listing Storage Centers managed by this DSM
        :return: URL for Storage Center listing
        """
        endpoint = self.STORAGE_CENTER_LIST_ENDPOINT % self.conn_instance_id if self.conn_instance_id else ''
        return (self.base_url + endpoint) if endpoint else None

    def set_password(self, password: str) -> None:
        """
        Set new password for authentication with DSM
        :param password: New password value
        :return: None
        """
        self._auth.password = password

    def login(self) -> bool:
        """
        Perform call to API login endpoint. If authentication is successful, this method returns boolean value based
        on result of login call.
        :return: True if authentication completed successfully, otherwise False
        """
        success = False
        resp = self.session.post(url=self.login_url, auth=self._auth)
        if resp.status_code == 200:
            reported_api_version = resp.json().get('apiVersion', None)
            if reported_api_version:
                self.api_version = reported_api_version
            try:
                self.conn_instance_id = resp.json()['instanceId']
            except KeyError:
                print("ERROR: SCM API did not report connection instance ID")
            else:
                success = True
        else:
            print("ERROR: Login failed (%d) - %s" % (resp.status_code, resp.text))
        return success

    def logout(self, silent: bool = False) -> None:
        """
        Performs call to api logout endpoint. Status messages about result of logout operation can be silenced by
        specifying silent = True
        :param silent: Whether this method should print result of the logout operation
        :return: None
        """
        resp = self.session.post(url=self.logout_url)
        if resp.status_code == 204:
            if not silent:
                print("Logout - OK")
        else:
            if not silent:
                print("WARNING: Logout failed (%d) - %s" % (resp.status_code, resp.text))

    def storage_centers(self) -> StorageCenterCollection:
        """
        Return collection of storage centers managed by this DSM
        :return:
        """
        url = self.sc_list_url
        storage_centers = StorageCenterCollection()
        if url is None:
            print("ERROR: Missing Connection ID, try logging in first")
        else:
            resp = self.session.get(url=url)
            if resp.status_code == 200:
                for storage_center in resp.json():
                    storage_centers.add(StorageCenter(req_session=self.session,
                                                      base_url=self.base_url,
                                                      name=storage_center['name'],
                                                      instance_id=storage_center['instanceId'],
                                                      serial_num=storage_center['scSerialNumber'],
                                                      ip_addr=storage_center['hostOrIpAddress']))

            else:
                print("ERROR: Failed to load Storage Center list (%d) - %s" % (resp.status_code, resp.text))
        return storage_centers
