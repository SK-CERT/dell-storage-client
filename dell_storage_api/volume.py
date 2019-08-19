""" This module contains classes for management of volumes in Storage Center managed by Dell Storage Manager"""
from typing import Dict, Any

from requests import Session

from dell_storage_api.storage_object import StorageObject, StorageObjectCollection, StorageObjectFolder


class Volume(StorageObject):
    """
    Class that represents volume in Storage Center
    """
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
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) -> 'Volume':
        """
        Class method that creates instance of Volume class from supplied dictionary. Source dictionary is expected
        to contain at least 'instanceId', 'name', 'deviceId' and 'volumeFolder' keys. Value of 'volumeFolder' is
        expected to be of type dict, containing at least key 'instanceId'
        :param req_session: requests.Session object with stored login cookies. (passed down
        from dell_storage_api.session.DsmSession)
        :param base_url: base URL of DSM
        :param source_dict: Dictionary containing data about Volume object
        :return: instance of a Volume class
        """
        return Volume(req_session=req_session,
                      base_url=base_url,
                      name=source_dict['name'],
                      instance_id=source_dict['instanceId'],
                      parent_folder_id=source_dict['volumeFolder']['instanceId'],
                      wwid=source_dict['deviceId'])

    @property
    def mapping_url(self) -> str:
        """
        Return complete URL for mapping this volume to specific server
        :return: URL for volume mapping
        """
        return self.build_url(self.MAPPING_ENDPOINT)

    @property
    def unmapping_url(self) -> str:
        """
        Return complete URL for unmapping this volume from servers to which it is currently mapped
        :return: URL for volume unmapping
        """
        return self.build_url(self.UNMAPPING_ENDPOINT)

    @property
    def recycle_url(self) -> str:
        """
        Return complete URL for moving this volume to recycle bin
        :return: URL for moving this volume to recycle bin
        """
        return self.build_url(self.RECYCLE_ENDPOINT)

    @property
    def delete_url(self) -> str:
        """
        Return complete URL to permanently remove this volume.
        WARNING: this action can not be undone
        :return: URL for removing this volume
        """
        return self.build_url(self.VOLUME_ENDPOINT)

    @property
    def expand_url(self) -> str:
        """
        Return complete URL for expanding this volume's capacity by specified amount.
        :return: URL for expanding this volume by specified amount
        """
        return self.build_url(self.EXPAND_ENDPOINT)

    @property
    def expand_to_size_url(self) -> str:
        """
        Return complete URL for expanding this volume's capacity to specified size
        :return: URL for expanding this volume to specified size
        """
        return self.build_url(self.EXPAND_TO_SIZE_ENDPOINT)

    @property
    def modify_url(self) -> str:
        """
        Return complete URL for modification of this volume. Modifiable attributes are 'Name' and 'VolumeFolder'
        :return: URL for modification of this volume
        """
        return self.build_url(self.VOLUME_ENDPOINT)

    @property
    def details_url(self) -> str:
        """
        Return complete URL for getting details about this volume
        :return: URL for details about this volume
        """
        return self.build_url(self.VOLUME_ENDPOINT)

    def map_to_server(self, server_id: str) -> bool:
        """
        Perform API call to DSM that maps this volume to server with instance ID specified by parameter 'server_id'. If
        supplied server_id is instance ID of a cluster, this volume will be mapped to every server that is part of that
        cluster.
        This operation fails if volume is already mapped to some server.
        :param server_id: Instance ID of server to which this volume will be mapped
        :return: True if operation is successful, otherwise False
        """
        success = False
        payload = {'Server': server_id}
        resp = self.session.post(self.mapping_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("OK - Volume '%s' (%s) sucessfully mapped to server." % (self.name, self.instance_id))
        else:
            print("Error: Failed to map volume - %s" % resp.json().get('result'))
        return success

    def unmap(self) -> bool:
        """
        Perform API call to DSM that unmaps this volume from any servers it is currently mapped to.
        WARNING: unmapping volume from active servers will cause those servers to loose connectivity with this volume.
        :return: True if operation is successful, otherwise False
        """
        success = False
        resp = self.session.post(self.unmapping_url)
        if resp.status_code == 204:
            success = True
            print('OK - Volume successfully unmapped')
        else:
            print('Error: Failed to unamp volume - %s' % resp.text)
        return success

    def expand(self, size: str) -> bool:
        """
        Perform API call to DSM that expands this volume by specified amount.
        :param size: Size by which this volume will be expanded (e.g.: 10GB or 1.2TB)
        :return: True if operation is successful, otherwise False
        """
        success = False
        payload = {"ExpandAmount": size}
        resp = self.session.post(self.expand_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("OK - Volume expanded by %s" % size)
        else:
            print("Error: Failed to expand volume - %s" % resp.json().get('result'))
        return success

    def expand_to_size(self, size: str) -> bool:
        """
        Perform API call to DSM that expands this volume to the specified size. This method can be used only to
        increase volume size, DSM is unable to shrink volumes
        :param size: Size to which this volume is expanded (e.g.: 500GB or 2.5TB)
        :return: True if operation is successful, otherwise False
        """
        success = False
        payload = {"NewSize": size}
        resp = self.session.post(self.expand_to_size_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("OK - Volume expanded to size %s" % size)
        else:
            print("Error: Failed to expand volume - %s" % resp.json().get('result'))
        return success

    def recycle(self) -> bool:
        """
        Perform API call to DSM that moves this volume to recycle bin. Volumes in recycle bin can be restored.
        :return: True if operation is successful, otherwise False
        """
        success = False
        resp = self.session.post(self.recycle_url)
        if resp.status_code == 204:
            success = True
            print('OK - Volume successfully moved to recycle bin')
        else:
            print('Error: Failed to recycle volume - %s' % resp.text)
        return success

    def delete(self) -> bool:
        """
        Perform API call to DSM that permanently deletes this volume.
        WARNING: This action can not be undone.
        :return: True if operation is successful, otherwise False
        """
        success = False
        resp = self.session.delete(self.delete_url)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume successfully deleted")
        else:
            print("Error: Failed to delete volume - %s" % resp.json().get('result'))
        return success

    def _modify_volume(self, payload: Dict[str, str]) -> bool:
        """
        Internal method that performs API call to DSM to modify volume properties. Only properties that are modifiable
        are 'Name' and 'VolumeFolder'.
        :param payload: Dictionary with modified properties and their new values (e.g.: {'Name': 'new_volume_name'})
        :return: True if operation is successful, otherwise False
        """
        # TODO: Move common functionality (like modify/rename/move) to base class
        success = False
        resp = self.session.put(self.modify_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume modified")
        else:
            print("Error: Failed to modify volume")
        return success

    def rename(self, new_name: str) -> bool:
        """
        Perform API call to DSM to change current volume name to the new value.
        Note: Volume names do not have to be unique even within same folder
        :param new_name: New name for this volume
        :return: True if operation is successful, otherwise False
        """
        if self._modify_volume({"Name": new_name}):
            self.name = new_name
            return True
        else:
            return False

    def move_to_folder(self, volume_folder_id: str) -> bool:
        """
        Perform API call to DSM to move this volume to the folder with instance ID specified by parameters
        volume_folder_id.
        :param volume_folder_id: Instance ID of a Volume folder to which this volume will be moved
        :return: True if operation is successful, otherwise False
        """
        if self._modify_volume({"VolumeFolder": volume_folder_id}):
            self.parent_folder_id = volume_folder_id
            return True
        else:
            return False

    def details(self) -> Dict[str, Any]:
        """
        Perform API call to DSM to get all information available about this volume and returns it in
        form of a dictionary
        :return: Dictionary containing details about this volume.
        """
        result: Dict[str, Any] = {}
        resp = self.session.put(self.details_url)
        if resp.status_code == 200:
            result = resp.json()
        else:
            print("Error: Failed to fetch volume details")
        return result


class VolumeCollection(StorageObjectCollection):
    """ Collection of volume folders"""

    def find_by_parent_folder(self, folder_id: str) -> 'VolumeCollection':
        """
        Return subset VolumeCollection that contains only volumes with specified parent folder.
        :param folder_id: Instance ID of a folder whose children should be in the result
        :return: Volumes with common parent specified by folder_id
        """
        result = VolumeCollection()
        for volume in self:
            if volume.parent_folder_id == folder_id:
                result.add(volume)
        return result


class VolumeFolder(StorageObjectFolder):
    """ Class representing Volume Folder"""
    ENDPOINT = '/StorageCenter/ScVolumeFolder'
    VOLUME_FOLDER_ENDPOINT = '/StorageCenter/ScVolumeFolder/%s'

    @classmethod
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) -> 'VolumeFolder':
        """
        Class method that creates instance of VolumeFolder class from supplied dictionary. Source dictionary is expected
        to contain at least 'instanceId' and 'name' keys. Optional key 'parent' can be present and its value is
        expected to be dict containing key 'instanceId'.
        :param req_session: requests.Session object with stored login cookies. (passed down
        from dell_storage_api.session.DsmSession)
        :param base_url: base URL of DSM
        :param source_dict: Dictionary containing data about VolumeFolder object
        :return: instance of a VolumeFolder class
        """
        return VolumeFolder(req_session=req_session,
                            base_url=base_url,
                            name=source_dict["name"],
                            instance_id=source_dict["instanceId"],
                            parent_id=source_dict.get('parent', {}).get('instanceId', None))

    @property
    def modify_url(self) -> str:
        """
        Return complete URL for modification of this volume folder
        :return: URL for folder modification
        """
        return self.build_url(self.VOLUME_FOLDER_ENDPOINT)

    @property
    def details_url(self) -> str:
        """
        Return complete URL for getting details about this volume folder
        :return: URL for details about this volume folder
        """
        return self.build_url(self.VOLUME_FOLDER_ENDPOINT)

    @property
    def delete_url(self) -> str:
        """
        Return complete URL to permanently remove this volume Folder.
        WARNING: this action can not be undone
        :return: URL for removing this volume folder
        """
        return self.build_url(self.VOLUME_FOLDER_ENDPOINT)

    def _modify_volume_folder(self, payload: Dict[str, str]) -> bool:
        """
        Internal method that performs call to DMS to modify this volume folder. Only modifiable properties are
        'Name' and 'Parent'.
        :param payload: Dictionary with modified properties and their new values (e.g.: {'Name': 'new_folder_name'})
        :return: True if operation is successful, otherwise False
        """
        success = False
        resp = self.session.put(self.modify_url, json=payload)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume folder modified")
        else:
            print("Error: Failed to modify volume folder")
        return success

    def rename(self, name: str) -> bool:
        """
        Perform API call to DSM to change this volume folder name to the new value.
        Note: Volume Folder names do not have to be unique even within same parent folder
        :param name: New name for this volume folder
        :return: True if operation is successful, otherwise False
        """
        if self._modify_volume_folder({"Name": name}):
            self.name = name
            return True
        else:
            return False

    def move_to_folder(self, parent_folder_id: str) -> bool:
        """
        Perform API call to DSM to move this folder to different parent folder specified by Instance ID in
        'parent_folder_id' parameter.
        :param parent_folder_id: Instance ID of the new parent folder
        :return: True if operation is successful, otherwise False
        """
        if self._modify_volume_folder({"VolumeFolder": parent_folder_id}):
            self.parent_id = parent_folder_id
            return True
        else:
            return False

    def details(self) -> Dict[str, Any]:
        """
        Perform API call to DSM to fetch details about this volume folder. Result is returned as dictionary
        :return: Dictionary containing details about this volume folder
        """
        result: Dict[str, Any] = {}
        resp = self.session.put(self.details_url)
        if resp.status_code == 200:
            result = resp.json()
        else:
            print("Error: Failed to fetch volume folder details")
        return result

    def delete(self) -> bool:
        """
        Perform API call to DSM to permanently delete this volume folder.
        Note: Volume folder can not be removed if it contains volumes.
        WARNING: This action can not be undone
        :return: True if operation is successful, otherwise False
        """
        success = False
        resp = self.session.delete(self.delete_url)
        if resp.status_code == 200:
            success = True
            print("Ok - Volume folder successfully deleted")
        else:
            print("Error: Failed to delete volume folder - %s" % resp.json().get('result'))
        return success
