""" This module contains classes that represent Storage Centers managed by Dell Storage manager (DSM) """
from typing import Any, Dict, Optional

import requests

from dell_storage_api.storage_object import StorageObject, StorageObjectFolder, StorageObjectCollection, \
    StorageObjectFolderCollection
from dell_storage_api.volume import Volume, VolumeCollection, VolumeFolder
from dell_storage_api.server import Server, ServerCollection


class StorageCenter(StorageObject):
    """
    Class representing physical Storage Center managed by DSM
    """
    SERVER_FOLDER_LIST_ENDPOINT = '/StorageCenter/StorageCenter/%s/ServerFolderList'
    SERVER_LIST_ENDPOINT = '/StorageCenter/StorageCenter/%s/ServerList'
    SERVER_FOLDER_ENDPOINT = '/StorageCenter/ScServerFolder/%s'

    VOLUME_FOLDER_LIST_ENDPOINT = '/StorageCenter/StorageCenter/%s/VolumeFolderList'
    VOLUME_LIST_ENDPOINT = '/StorageCenter/StorageCenter/%s/VolumeList'

    def __init__(self, req_session: requests.Session, base_url: str, name: str,
                 instance_id: str, serial_num: str, ip_addr: str) -> None:
        super(StorageCenter, self).__init__(req_session, base_url, name, instance_id)
        self.serial_num = serial_num
        self.ip_addr = ip_addr

    @property
    def server_folder_list_url(self) -> str:
        """
        Return complete URL to list all server folders in this Storage Center
        :return: URL for Server Folder listing
        """
        return self.build_url(self.SERVER_FOLDER_LIST_ENDPOINT)

    @property
    def server_list_url(self) -> str:
        """
        Return complete URL to list all servers defined in this Storage Center
        :return: URL for Server listing
        """
        return self.build_url(self.SERVER_LIST_ENDPOINT)

    @property
    def volume_folder_list_url(self) -> str:
        """
        Return complete URL to list all volume folders in this Storage Center
        :return: URL for Volume Folder listing
        """
        return self.build_url(self.VOLUME_FOLDER_LIST_ENDPOINT)

    @property
    def volume_list_url(self) -> str:
        """
        Return complete URL fo list all volumes present in this Storage Center
        :return: URL for Volume listning
        """
        return self.build_url(self.VOLUME_LIST_ENDPOINT)

    def server_folder_list(self) -> StorageObjectFolderCollection:
        """
        Return collection of all Server Folders in this Storage Center
        :return: Collection of all Server Folders
        """
        return self._fetch_object_list(self.server_folder_list_url)

    def server_list(self) -> ServerCollection:
        """
        Return collection of all servers defined in this Storage Center
        :return: Collection of all Servers
        """
        result = ServerCollection()
        for server_data in self._fetch_object_list(self.server_list_url):
            result.add(Server.from_json(req_session=self.session,
                                        base_url=self.base_url,
                                        source_dict=server_data))
        return result

    def volume_folder_list(self) -> StorageObjectFolderCollection:
        """
        Return collection of all Volume Folders in this Storage Center
        :return: Collection of all Volume Folders
        """
        result = StorageObjectFolderCollection()
        for volume_folder_data in self._fetch_object_list(url=self.volume_folder_list_url):
            result.add(VolumeFolder.from_json(req_session=self.session,
                                              base_url=self.base_url,
                                              source_dict=volume_folder_data))
        return result

    def volume_list(self) -> VolumeCollection:
        """
        Return collection of all volumes present in this Storage Center
        :return: Collection of all volumes
        """
        result = VolumeCollection()
        for volume_data in self._fetch_object_list(url=self.volume_list_url):
            result.add(Volume.from_json(req_session=self.session,
                                        base_url=self.base_url,
                                        source_dict=volume_data))
        return result

    def _find_volume_folder_root(self) -> Optional[StorageObjectFolder]:
        """
        Internal method to find root Volume Folder that contains all other Volumes and Volume Folders. Method returns
        None in case that there is problem with data fetching or lookup.
        :return: Root Volume Folder or None in case of failure
        """
        all_folders = self.volume_folder_list()
        if not all_folders:
            print("Error: Failed to fetch volume folder list")
            return None
        else:
            root_folder = self.volume_folder_list().root_folder()
            if root_folder is None:
                print("Error: Failed to lookup root volume folder in list of all folders. "
                      "This really should not happen")
                return None
            else:
                return root_folder

    def new_volume_folder(self, name: str, parent_folder_id: str = '') -> Optional[VolumeFolder]:
        """
        Create new Volume Folder in Storage Center and return object representing this new folder. This method
        returns None in case there is a problem with folder creation.
        :param name: Name for the new Volume Folder
        :param parent_folder_id: Instance ID of parent folder. Defaults to root folder
        :return: new VolumeFolder object or None in case of failure
        """
        if not parent_folder_id:
            parent_folder = self._find_volume_folder_root()
            if parent_folder is not None:
                parent_folder_id = parent_folder.instance_id
            else:
                print("Error: Failed to create new volume folder")
                return None
        if parent_folder_id is None:
            print("Error: Failed to create new volume folder")
            return None
        url = self.base_url + VolumeFolder.ENDPOINT
        payload = {"Name": name,
                   "Parent": parent_folder_id,
                   "StorageCenter": self.instance_id}
        resp = self.session.post(url, json=payload)
        if resp.status_code == 201:
            return VolumeFolder.from_json(self.session, self.base_url, resp.json())
        else:
            print("Error: Failed to create new volume folder.")
            return None

    def new_volume(self, name: str, size: str, volume_folder_id: str = '') -> Optional[Volume]:
        """
        Create new Volume in Storage Center and return object representing this new volume. This method returns None
        in case there is a problem with volume creation.
        :param name: Name of the new volume
        :param size: Size of the new volume (e.g: '100GB' or '1.5TB')
        :param volume_folder_id: Instance ID of folder in which this volume will be created. Defaults to root folder
        :return: new Volume object or None in case of failure
        """
        if not volume_folder_id:
            volume_folder = self._find_volume_folder_root()
            if volume_folder is not None:
                volume_folder_id = volume_folder.instance_id
            else:
                print("Error: Failed to create new volume")
                return None
        if volume_folder_id is None:
            print("Error: Failed to create new volume")
            return None
        url = self.base_url + Volume.ENDPOINT
        payload = {"Name": name,
                   "Size": size,
                   "StorageCenter": self.instance_id,
                   "VolumeFolder": volume_folder_id}
        resp = self.session.post(url, json=payload)
        if resp.status_code == 201:
            return Volume.from_json(self.session, self.base_url, resp.json())
        else:
            print("Error: Failed to create new volume. (%d) - %s" % (resp.status_code, resp.text))
            return None

    def _fetch_object_list(self, url: str) -> Dict[Any, Any]:
        """
        Internal generic method to fetch list of object from supplied URL. This method returns raw dictionary created
        from json in response body. This method returns empty dictionary if there is problem with data fetching
        :param url: URL of API endpoint that returns (json) list of objects
        :return: raw dictionary of objects returned by API endpoint
        """
        result: Dict[Any, Any] = {}
        resp = self.session.get(url)
        if resp.status_code == 200:
            result = resp.json()
        else:
            print("Error: Failed to fetch object list (%d) - %s" % (resp.status_code, resp.text))
        return result


class StorageCenterCollection(StorageObjectCollection):
    """
    Collection representing multiple physical Storage Centers managed by single DSM
    """

    def find_by_serial_num(self, serial_num: str) -> Optional[StorageCenter]:
        """
        Return Storage Center object with given serial number. In case there is no such Storage Center, return None
        :param serial_num: Serial number of physical Storage Center
        :return: StorageCenter object with given serial number or None
        """
        result = None
        for storage_center in self:
            if storage_center.serial_num == serial_num:
                result = storage_center
                break
        return result
