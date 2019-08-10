from requests import Session
from typing import Dict, Any

from scm_api.storage_object import StorageObject, StorageObjectCollection


class Volume(StorageObject):

    ENDPOINT = '/StorageCenter/ScVolume'

    def __init__(self, req_session: Session, base_url: str, name: str, instance_id: str,
                 parent_folder_id: str, wwid: str) -> None:
        super().__init__(req_session, base_url, name, instance_id)
        self.parent_folder_id = parent_folder_id
        self.wwid = wwid

    @classmethod
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) ->'Volume':
        return Volume(req_session=req_session,
                      base_url=base_url,
                      name=source_dict['name'],
                      instance_id=source_dict['instanceId'],
                      parent_folder_id=source_dict['volumeFolder']['instanceId'],
                      wwid=source_dict['deviceId'])


class VolumeCollection(StorageObjectCollection):

    def find_by_parend_folder(self, folder_id: str) ->'VolumeCollection':
        result = VolumeCollection()
        for volume in self:
            if volume.parent_folder_id == folder_id:
                result.add(volume)
        return result
