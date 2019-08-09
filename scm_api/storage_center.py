from typing import Optional

import requests

from scm_api.server_folder import ServerFolder, ServerFolderCollection
from scm_api.storage_object import StorageObject, StorageObjectCollection


class StorageCenter(StorageObject):
    SERVER_FOLDER_LIST_ENDPOINT = '/StorageCenter/ScServerFolder'
    SERVER_FOLDER_ENDPOINT = '/StorageCenter/ScServerFolder/%s'

    def __init__(self, req_session: requests.Session, base_url: str, name: str,
                 instance_id: str, serial_num: str, ip_addr: str) -> None:
        super(StorageCenter, self).__init__(req_session, base_url, name, instance_id)
        self.serial_num = serial_num
        self.ip_addr = ip_addr


class StorageCenterCollection(StorageObjectCollection):

    def __init__(self) -> None:
        super(StorageCenterCollection, self).__init__()
        self.server_folders = ServerFolderCollection()

    def find_by_serial_num(self, serial_num: str) -> Optional[StorageCenter]:
        result = None
        for storage_center in self:
            if storage_center.serial_num == serial_num:
                result = storage_center
                break
        return result

    def get_server_folders(self) -> None:
        pass
