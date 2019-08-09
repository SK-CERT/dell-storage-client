from requests import Session
from typing import Optional

from scm_api.storage_object import StorageObject, StorageObjectCollection


class ServerFolder(StorageObject):

    def __init__(self, req_session: Session, base_url: str, name: str, instance_id: str,
                 parent: Optional['ServerFolder']=None) -> None:
        super().__init__(req_session, base_url, name, instance_id)
        self.parent = parent

    @property
    def is_root(self) -> bool:
        return self.parent is None


class ServerFolderCollection(StorageObjectCollection):

    def find_by_parent(self, parent: ServerFolder) -> 'ServerFolderCollection':
        result = ServerFolderCollection()
        for server_folder in self:
            if server_folder.instance_id == parent.instance_id:
                result.add(server_folder)
        return result

    def root_folder(self) -> ServerFolder:
        for folder in self:
            if folder.is_root:
                return folder
