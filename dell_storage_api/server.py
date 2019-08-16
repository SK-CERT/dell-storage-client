from typing import Dict, Any

from requests import Session

from dell_storage_api.storage_object import StorageObject, StorageObjectCollection


class Server(StorageObject):

    TYPE_PHYSICAL_SERVER = 'ScPhysicalServer'
    TYPE_SERVER_CLUSTER = 'ScServerCluster'

    def __init__(self, req_session: Session, base_url: str, name: str,
                 instance_id: str, object_type: str) -> None:
        super(Server, self).__init__(req_session=req_session, base_url=base_url, name=name, instance_id=instance_id)
        self.type = object_type

    @classmethod
    def from_json(cls, req_session: Session, base_url: str, source_dict: Dict[Any, Any]) -> 'Server':
        return Server(req_session=req_session,
                      base_url=base_url,
                      name=source_dict['name'],
                      instance_id=source_dict['instanceId'],
                      object_type=source_dict['objectType'])

    def is_cluster(self) -> bool:
        return bool(self.type == self.TYPE_SERVER_CLUSTER)

    def is_physical_server(self) -> bool:
        return bool(self.type == self.TYPE_PHYSICAL_SERVER)

    def pretty_type(self) -> str:
        if self.type == self.TYPE_SERVER_CLUSTER:
            result = 'Cluster'
        elif self.type == self.TYPE_PHYSICAL_SERVER:
            result = 'Physical Server'
        else:
            result = "Unknown Type"
        return result


class ServerCollection(StorageObjectCollection):

    def filter_physical_servers(self) -> 'ServerCollection':
        physical_servers = ServerCollection()
        for server in self:
            if server.is_physical_server():
                physical_servers.add(server)
        return physical_servers

    def filter_clusters(self) -> 'ServerCollection':
        clusters = ServerCollection()
        for server in self:
            if server.is_cluster():
                clusters.add(server)
        return clusters
