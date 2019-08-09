from requests import Session
from typing import Optional

from scm_api.storage_object import StorageObject, StorageObjectCollection, StorageObjectFolder


class ServerFolder(StorageObject, StorageObjectFolder):

    def __init__(self, req_session: Session, base_url: str, name: str, instance_id: str,
                 parent_id: Optional[str]=None) -> None:
        StorageObject.__init__(self,
                               req_session=req_session,
                               base_url=base_url,
                               name=name,
                               instance_id=instance_id)
        StorageObjectFolder.__init__(self, parent_id)


class ServerFolderCollection(StorageObjectCollection):

    def find_by_parent(self, parent: ServerFolder) -> 'ServerFolderCollection':
        result = ServerFolderCollection()
        for server_folder in self:
            if server_folder.instance_id == parent.instance_id:
                result.add(server_folder)
        return result

    def root_folder(self) -> Optional[ServerFolder]:
        for folder in self:
            if folder.is_root:
                return folder
        else:
            return None
