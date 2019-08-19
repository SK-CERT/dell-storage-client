"""
This module contains generic classes that serve as a base for creation of more specific classes that represent objects
in Dell Storage Manager (DSM) API.
Base classes:
    - StorageObject: Base for standalone objects (e.g.: Volumes, Servers)
    - StorageObjectFolder: Standalone objects can be grouped into folders in DSM. This object
                           represents such folders
    - StorageObjectCollection: Collection of StorageObject instances
    - StorageObjectFolderCollection: Collection of StorageObjectFolder instances
"""
from collections import Iterable
from typing import Any
from typing import Optional, Iterator, List, Dict

from requests import Session


class StorageObject:
    """
    Base class for more specific classes that represent standalone objects in DSM api, such as
    'Volume' or 'Server'.
    """

    def __init__(self, req_session: Session, base_url: str, name: str, instance_id: str) -> None:
        self.session = req_session
        self.base_url = base_url
        self.name = name
        self.instance_id = instance_id

    def __str__(self) -> str:
        return "%s: %s (%s)" % (self.__class__, self.name, self.instance_id)

    def build_url(self, endpoint_url: str) -> str:
        """
        Build complete URL to the API endpoint by replacing single formatting character (%s) in endpoint_url with
        instance_id of the object and combining it with base url.
        :param endpoint_url: URL of the api endpoint (e.g.: '/StorageCenter/ScVolume/%s/MapToServer')
        :return: Full URL to the api endpoint containing base_url, endpoint_url and instance ID of the storage object
        """
        return self.base_url + endpoint_url % self.instance_id

    @classmethod
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) -> 'StorageObject':
        """
        Class method that creates instance of StorageObject from supplied dictionary. Source dictionary is expected
        to contain at least 'instanceId' and 'name' keys.
        :param req_session: requests.Session object with stored login cookies. (passed down
        from dell_storage_api.session.DsmSession)
        :param base_url: base URL of DSM
        :param source_dict: Dictionary containing data about storage object
        :return: instance of a StorageObject class
        """
        return StorageObject(req_session=req_session,
                             base_url=base_url,
                             instance_id=source_dict['instanceId'],
                             name=source_dict['name'])


class StorageObjectFolder(StorageObject):
    """
    Base class for more specific classes that represent folders for objects in DSM api, such as
    'VolumeFolder' or 'ServerFolder'.
    """

    def __init__(self, req_session: Session, base_url: str, name: str,
                 instance_id: str, parent_id: Optional[str]) -> None:
        super().__init__(req_session, base_url, name, instance_id)
        self.parent_id = parent_id

    @property
    def is_root(self) -> bool:
        """
        Is this folder a root in folder hierarchy?
        :return: True if this folder is root, otherwise False
        """
        return self.parent_id is None

    @classmethod
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) -> 'StorageObjectFolder':
        """
        Class method that creates instance of StorageObjectFolder from supplied dictionary. Source dictionary is
        expected to contain at least 'instanceId', 'name' and 'parent' keys. 'parent' key is optional (it can be
        missing in case the folder is root) but if it's present, it should be dictionary containing at least
        'instanceId' key.
        :param req_session: requests.Session object with stored login cookies. (passed down
        from dell_storage_api.session.DsmSession)
        :param base_url: base URL of DSM
        :param source_dict: Dictionary containing data about storage object folder
        :return: instance of a StorageObjectFolder class
        """
        return StorageObjectFolder(req_session=req_session,
                                   base_url=base_url,
                                   instance_id=source_dict['instanceId'],
                                   name=source_dict['name'],
                                   parent_id=source_dict.get('parent', {}).get('instanceId', None)
                                   )


class StorageObjectCollection(Iterable):
    """
    Base class representing collection of StorageObject-s. Internally, StorageObjects are stored as dictionary
    indexed by object's instance ID.
    This class provides method for object searching by attributes that are common for every object type in
    DSM and those are 'name' and 'instanceId'.
    """

    def __iter__(self) -> Iterator:
        return self._store.values().__iter__()

    def __bool__(self) -> bool:
        return bool(self._store)

    def __len__(self) -> int:
        return len(self._store)

    def __init__(self) -> None:
        self._store: Dict[str, StorageObject] = {}

    def add(self, storage_object: StorageObject) -> None:
        """
        Add storage object to this collection
        :param storage_object: StorageObject to be added
        :return: None
        """
        self._store[storage_object.instance_id] = storage_object

    def find_by_instance_id(self, instance_id: str) -> Optional[StorageObject]:
        """
        Find StorageObject with specified instance_id in this collection. If no object is found, None is returned
        :param instance_id: Instance ID of a storage object to search for
        :return: StorageObject or None
        """
        return self._store.get(instance_id, None)

    def find_by_name(self, name: str) -> 'StorageObjectCollection':
        """
        Find StorageObject with specified name in this collection. Since names do not have to be unique, this method
        will always return StorageObjectCollection containing all the StorageObjects with given name. In case no
        StorageObject is found, returned collection is empty
        :param name: Name of the StorageObject to search for
        :return: StorageObjectCollection containing all the objects with given name
        """
        result = StorageObjectCollection()
        for storage_object in self:
            if storage_object.name == name:
                result.add(storage_object)
        return result

    def all_objects(self) -> List[StorageObject]:
        """
        Return all StorageObjects in this collection as a list
        :return: List of all storage objects in this collection
        """
        return [item for item in self._store.values()]


class StorageObjectFolderCollection(StorageObjectCollection):
    """
    Base class representing collection of StorageObjectFolder-s. Internally, StorageObjectFolders are stored as
    dictionary indexed by folders's instance ID.
    This class provides methods to search for root folders and to find folders taht belong to the specific
    parent folder.
    """

    def root_folder(self) -> Optional[StorageObjectFolder]:
        """
        Find and return StorageObjectFolder that is a root in folder hierarchy. If this collection does not contain
        root folder, return None
        :return: Root folder from this collection or None
        """
        for folder in self:
            if folder.is_root:
                root = folder
                break
        else:
            root = None
        return root

    def find_by_parent_id(self, parent_id: str) -> 'StorageObjectFolderCollection':
        """
        Create new StorageObjectFolderCollection that is subset of current collection and contains only
        StorageObjectFolders whose direct parent is folder with given parent_id (instance ID of a folder)
        :param parent_id: Instance ID of a parent folder
        :return: Folder collection containing folders with specific parent folder
        """
        result = StorageObjectFolderCollection()
        for server_folder in self:
            if server_folder.parent_id == parent_id:
                result.add(server_folder)
        return result
