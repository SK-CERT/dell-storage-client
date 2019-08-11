import requests
from typing import Any

from scm_api.storage_object import *
from scm_api.volume import Volume, VolumeCollection


class StorageCenter(StorageObject):
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
    def server_folder_list_url(self) ->str:
        return self.build_url(self.SERVER_FOLDER_LIST_ENDPOINT)

    @property
    def server_list_url(self) ->str:
        return self.build_url(self.SERVER_LIST_ENDPOINT)

    @property
    def volume_folder_list_url(self) ->str:
        return self.build_url(self.VOLUME_FOLDER_LIST_ENDPOINT)

    @property
    def volume_list_url(self) ->str:
        return self.build_url(self.VOLUME_LIST_ENDPOINT)

    def server_folder_list(self) ->StorageObjectFolderCollection:
        return self._fetch_folder_list(self.server_folder_list_url)

    def server_list(self) ->StorageObjectCollection:
        return self._fetch_object_list(self.server_list_url)

    def volume_folder_list(self) ->StorageObjectFolderCollection:
        return self._fetch_folder_list(url=self.volume_folder_list_url)

    def volume_list(self) ->VolumeCollection:
        result = VolumeCollection()
        for volume_data in self._fetch_object_list(url=self.volume_list_url):
            result.add(Volume.from_json(req_session=self.session,
                                        base_url=self.base_url,
                                        source_dict=volume_data))
        return result

    def new_volume(self, name: str, size: str, volume_folder: str='') ->Optional[Volume]:
        if not volume_folder:
            all_folders = self.volume_folder_list()
            if not all_folders:
                print("Error: Failed to create new volume. Failed to fetch default volume folder")
                return None
            else:
                root_folder = self.volume_folder_list().root_folder()
                if root_folder is None:
                    print("Error: Failed to lookup root vlume folder in list of all folders. "
                          "This really should not happen")
                    return None
                else:
                    volume_folder = root_folder.instance_id
        url = self.base_url + Volume.ENDPOINT
        payload = {"Name": name,
                   "Size": size,
                   "StorageCenter": self.instance_id,
                   "VolumeFolder": volume_folder}
        resp = self.session.post(url, json=payload)
        if resp.status_code == 201:
            return Volume.from_json(self.session, self.base_url, resp.json())
        else:
            print("Error: Failed to create new volume. (%d) - %s" % (resp.status_code, resp.text))
            return None

    def _fetch_folder_list(self, url: str) ->StorageObjectFolderCollection:
        result = StorageObjectFolderCollection()
        resp = self.session.get(url)
        if resp.status_code == 200:
            for volume_folder in resp.json():
                result.add(StorageObjectFolder.from_json(self.session, self.base_url, volume_folder))
        else:
            print("Error: Failed to fetch folders (%d) - %s" % (resp.status_code, resp.text))
        return result

    def _fetch_object_list(self, url: str) ->Dict[Any, Any]:
        result: Dict[Any, Any] = {}
        resp = self.session.get(url)
        if resp.status_code == 200:
            result = resp.json()
        else:
            print("Error: Failed to fetch object list (%d) - %s" % (resp.status_code, resp.text))
        return result


class StorageCenterCollection(StorageObjectCollection):

    def find_by_serial_num(self, serial_num: str) -> Optional[StorageCenter]:
        result = None
        for storage_center in self:
            if storage_center.serial_num == serial_num:
                result = storage_center
                break
        return result
