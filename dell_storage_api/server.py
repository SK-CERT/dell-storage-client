""" This module contains classes representing servers connected to the Storage Center """
from typing import Dict, Any

from requests import Session

from dell_storage_api.storage_object import StorageObject, StorageObjectCollection


class Server(StorageObject):
    """
    Class representing server connected to Storage controller. Server can be of type 'Physical Server' or 'Cluster'
    where cluster is logical group of 0-N physical servers.
    Instance ID of a server can be used to map Volume to this particular server. This action makes volume accessible
    as a block device in this server. Mapping volume to 'Cluster' makes volume accessible to all physical servers in
    this cluster.
    """

    TYPE_PHYSICAL_SERVER = 'ScPhysicalServer'
    TYPE_SERVER_CLUSTER = 'ScServerCluster'

    def __init__(self, req_session: Session, base_url: str, name: str,
                 instance_id: str, object_type: str) -> None:
        super(Server, self).__init__(req_session=req_session, base_url=base_url, name=name, instance_id=instance_id)
        self.type = object_type

    @classmethod
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) -> 'Server':
        """
        Class method that creates instance of Server class from supplied dictionary. Source dictionary is expected
        to contain at least 'instanceId', 'name' and 'objectType' keys.
        :param req_session: requests.Session object with stored login cookies. (passed down
        from dell_storage_api.session.DsmSession)
        :param base_url: base URL of DSM
        :param source_dict: Dictionary containing data about Server object
        :return: instance of a Server class
        """
        return Server(req_session=req_session,
                      base_url=base_url,
                      name=source_dict['name'],
                      instance_id=source_dict['instanceId'],
                      object_type=source_dict['objectType'])

    def is_cluster(self) -> bool:
        """
        Is this Server object of type 'Cluster'
        :return: True if this Server is Cluster, otherwise False
        """
        return bool(self.type == self.TYPE_SERVER_CLUSTER)

    def is_physical_server(self) -> bool:
        """
        Is this Server object of type 'Physical Server'
        :return: True if this Server is Physical server, otherwise False
        """
        return bool(self.type == self.TYPE_PHYSICAL_SERVER)

    def pretty_type(self) -> str:
        """
        Pretty print type of this server
        :return:
        """
        if self.type == self.TYPE_SERVER_CLUSTER:
            result = 'Cluster'
        elif self.type == self.TYPE_PHYSICAL_SERVER:
            result = 'Physical Server'
        else:
            result = "Unknown Type"
        return result


class ServerCollection(StorageObjectCollection):
    """
    Collection of Server objects
    """

    def filter_physical_servers(self) -> 'ServerCollection':
        """
        Return subset collection containing only servers of type 'Physical Server'
        :return: Subset containing only physical servers
        """
        physical_servers = ServerCollection()
        for server in self:
            if server.is_physical_server():
                physical_servers.add(server)
        return physical_servers

    def filter_clusters(self) -> 'ServerCollection':
        """
        Return subset collection containing only servers of type 'Cluster'
        :return: Subset containing only clusters
        """
        clusters = ServerCollection()
        for server in self:
            if server.is_cluster():
                clusters.add(server)
        return clusters
