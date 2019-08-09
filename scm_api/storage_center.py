from typing import Optional

import requests

from scm_api.server_folder import ServerFolder, ServerFolderCollection
from scm_api.storage_object import StorageObject, StorageObjectCollection


class StorageCenter(StorageObject):
    SERVER_FOLDER_LIST_ENDPOINT = '/StorageCenter/StorageCenter/%s/ServerFolderList'
    SERVER_FOLDER_ENDPOINT = '/StorageCenter/ScServerFolder/%s'

    def __init__(self, req_session: requests.Session, base_url: str, name: str,
                 instance_id: str, serial_num: str, ip_addr: str) -> None:
        super(StorageCenter, self).__init__(req_session, base_url, name, instance_id)
        self.serial_num = serial_num
        self.ip_addr = ip_addr

    @property
    def server_folder_list_url(self) -> str:
        return self.base_url + self.SERVER_FOLDER_LIST_ENDPOINT % self.instance_id

    def server_folder_list(self) ->ServerFolderCollection:
        result = ServerFolderCollection()
        resp = self.session.get(self.server_folder_list_url)
        if resp.status_code == 200:
            for server_folder in resp.json():
                result.add(ServerFolder(req_session=self.session,
                                        base_url=self.base_url,
                                        instance_id=server_folder['instanceId'],
                                        name=server_folder['name'],
                                        parent_id=server_folder.get('parent', {}).get('instanceId', None)))
        else:
            print("Error: Cant fetch server folders (%d) - %s" % (resp.status_code, resp.text))
        return result


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
