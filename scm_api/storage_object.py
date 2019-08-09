from collections import Iterable
from typing import Optional, Iterator, List, Dict
from requests import Session


class StorageObject:

    def __init__(self, req_session: Session, base_url: str, name: str, instance_id: str) -> None:
        self.session = req_session
        self.base_url = base_url
        self.name = name
        self.instance_id = instance_id

    def __str__(self) -> str:
        return "%s: %s (%s)" % (self.__class__, self.name, self.instance_id)


class StorageObjectFolder(StorageObject):

    def __init__(self, req_session: Session, base_url: str, name: str,
                 instance_id: str, parent_id: Optional[str]) -> None:
        super().__init__(req_session, base_url, name, instance_id)
        self.parent_id = parent_id

    @property
    def is_root(self) -> bool:
        return self.parent_id is None


class StorageObjectCollection(Iterable):

    def __iter__(self) -> Iterator:
        return self._store.values().__iter__()

    def __len__(self) -> int:
        return len(self._store)

    def __init__(self) -> None:
        self._store: Dict[str, StorageObject] = {}

    def add(self, storage_object: StorageObject) -> None:
        self._store[storage_object.instance_id] = storage_object

    def find_by_instance_id(self, instance_id: str) -> Optional[StorageObject]:
        return self._store.get(instance_id, None)

    def find_by_name(self, name: str) -> 'StorageObjectCollection':
        result = StorageObjectCollection()
        for storage_object in self:
            if storage_object.name == name:
                result.add(storage_object)
        return result

    def all_objects(self) -> List[StorageObject]:
        return [item for item in self._store.values()]


class StorageObjectFolderCollection(StorageObjectCollection):

    def root_folder(self) -> Optional[StorageObjectFolder]:
        for folder in self:
            if folder.is_root:
                return folder
        else:
            return None

    def find_by_parent_id(self, parent_id: str) -> 'StorageObjectFolderCollection':
        result = StorageObjectFolderCollection()
        for server_folder in self:
            if server_folder.instance_id == parent_id:
                result.add(server_folder)
        return result
