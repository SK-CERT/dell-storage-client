from requests import Session
from typing import Dict, Any

from scm_api.storage_object import StorageObject, StorageObjectCollection, StorageObjectFolder


class Volume(StorageObject):

    ENDPOINT = '/StorageCenter/ScVolume'
    VOLUME_ENDPOINT = '/StorageCenter/ScVolume/%s'
    MAPPING_ENDPOINT = '/StorageCenter/ScVolume/%s/MapToServer'
    UNMAPPING_ENDPOINT = '/StorageCenter/ScVolume/%s/Unmap'
    RECYCLE_ENDPOINT = '/StorageCenter/ScVolume/%s/Recycle'
    EXPAND_TO_SIZE_ENDPOINT = '/StorageCenter/ScVolume/%s/ExpandToSize'
    EXPAND_ENDPOINT = '/StorageCenter/ScVolume/%s/Expand'

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

    @property
    def mapping_url(self) ->str:
        return self.build_url(self.MAPPING_ENDPOINT)

    @property
    def unmapping_url(self) ->str:
        return self.build_url(self.UNMAPPING_ENDPOINT)

    @property
    def recycle_url(self) ->str:
        return self.build_url(self.RECYCLE_ENDPOINT)

    @property
    def delete_url(self) ->str:
        return self.build_url(self.VOLUME_ENDPOINT)

    @property
    def expand_url(self) ->str:
        return self.build_url(self.EXPAND_ENDPOINT)

    @property
    def expand_to_size_url(self) ->str:
        return self.build_url(self.EXPAND_TO_SIZE_ENDPOINT)

    @property
    def modify_url(self) ->str:
        return self.build_url(self.VOLUME_ENDPOINT)

    @property
    def details_url(self) ->str:
        return self.build_url(self.VOLUME_ENDPOINT)

    def map_to_server(self, server_id: str) ->bool:
        success = False
        payload = {'Server': server_id}
        resp = self.session.post(self.mapping_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("OK - Volume mapped successfully")
        else:
            print("Error: Failed to map volume - %s" % resp.json().get('result'))
        return success

    def unmap(self) ->bool:
        success = False
        resp = self.session.post(self.unmapping_url)
        if resp.status_code == 204:
            success = True
            print('OK - Volume successfully unmapped')
        else:
            print('Error: Failed to unamp volume - %s' % resp.text)
        return success

    def expand(self, size: str) ->bool:
        success = False
        payload = {"ExpandAmount": size}
        resp = self.session.post(self.expand_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("OK - Volume expanded by %s" % size)
        else:
            print("Error: Failed to expand volume - %s" % resp.json().get('result'))
        return success

    def expand_to_size(self, size: str) ->bool:
        success = False
        payload = {"NewSize": size}
        resp = self.session.post(self.expand_to_size_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("OK - Volume expanded to size %s" % size)
        else:
            print("Error: Failed to expand volume - %s" % resp.json().get('result'))
        return success

    def recycle(self) ->bool:
        success = False
        resp = self.session.post(self.recycle_url)
        if resp.status_code == 204:
            success = True
            print('OK - Volume successfully moved to recycle bin')
        else:
            print('Error: Failed to recycle volume - %s' % resp.text)
        return success

    def delete(self) ->bool:
        success = False
        resp = self.session.delete(self.delete_url)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume successfully deleted")
        else:
            print("Error: Failed to delete volume - %s" % resp.json().get('result'))
        return success

    def _modify_volume(self, payload: Dict[str, str]) ->bool:
        # TODO: Move common functionality (like modify/rename/move) to base class
        success = False
        resp = self.session.put(self.modify_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume modified")
        else:
            print("Error: Failed to modify volume")
        return success

    def rename(self, new_name: str) ->bool:
        if self._modify_volume({"Name": new_name}):
            self.name = new_name
            return True
        else:
            return False

    def move_to_folder(self, volume_folder_id: str) ->bool:
        if self._modify_volume({"VolumeFolder": volume_folder_id}):
            self.parent_folder_id = volume_folder_id
            return True
        else:
            return False

    def details(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        resp = self.session.put(self.details_url)
        if resp.status_code == 200:
            result = resp.json()
        else:
            print("Error: Failed to fetch volume details")
        return result


class VolumeCollection(StorageObjectCollection):

    def find_by_parent_folder(self, folder_id: str) -> 'VolumeCollection':
        result = VolumeCollection()
        for volume in self:
            if volume.parent_folder_id == folder_id:
                result.add(volume)
        return result


class VolumeFolder(StorageObjectFolder):

    ENDPOINT = '/StorageCenter/ScVolumeFolder'
    VOLUME_FOLDER_ENDPOINT = '/StorageCenter/ScVolumeFolder/%s'

    @classmethod
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) ->'VolumeFolder':
        return VolumeFolder(req_session=req_session,
                            base_url=base_url,
                            name=source_dict["name"],
                            instance_id=source_dict["instanceId"],
                            parent_id=source_dict.get('parent', {}).get('instanceId', None))

    @property
    def modify_url(self):
        return self.build_url(self.VOLUME_FOLDER_ENDPOINT)
    
    @property
    def details_url(self):
        return self.build_url(self.VOLUME_FOLDER_ENDPOINT)
    
    @property
    def delete_url(self):
        return self.build_url(self.VOLUME_FOLDER_ENDPOINT)

    def _modify_volume_folder(self, payload: Dict[str, str]) ->bool:
        success = False
        resp = self.session.put(self.modify_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume folder modified")
        else:
            print("Error: Failed to modify volume folder")
        return success

    def rename(self, name: str) ->bool:
        if self._modify_volume_folder({"Name": name}):
            self.name = name
            return True
        else:
            return False

    def move_to_folder(self, parent_folder_id: str) ->bool:
        if self._modify_volume_folder({"VolumeFolder": parent_folder_id}):
            self.parent_folder_id = parent_folder_id
            return True
        else:
            return False

    def details(self) ->Dict[str, Any]:
        result: Dict[str, Any] = {}
        resp = self.session.put(self.details_url)
        if resp.status_code == 200:
            result = resp.json()
        else:
            print("Error: Failed to fetch volume folder details")
        return result

    def delete(self) ->bool:
        success = False
        resp = self.session.delete(self.delete_url)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume folder successfully deleted")
        else:
            print("Error: Failed to delete volume folder - %s" % resp.json().get('result'))
        return success
