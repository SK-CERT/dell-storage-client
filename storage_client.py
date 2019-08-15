from scm_api import ScmSession, StorageCenter
from typing import Optional
from texttable import Texttable
import argparse
import getpass

CMD_CONST_VOLUME = 'volume'
CMD_CONST_VOLUME_CREATE = 'create'
CMD_CONST_VOLUME_LIST = 'list'

CMD_CONST_VOLUME_FOLDER = 'volume_folder'
CMD_CONST_VOLUME_FOLDER_CREATE = 'create'
CMD_CONST_VOLUME_FOLDER_LIST = 'list'

CMD_CONST_STORAGE_CENTER = 'storage_center'
CMD_CONST_STORAGE_CENTER_LIST = 'list'


class ReturnCode:
    SUCCESS = 0
    FAILURE = 1


def volume_create(storage: StorageCenter, name: str, size: str,
                  unique_name: bool=True, folder_id: str='') ->int:
    if unique_name and storage.volume_list().find_by_name(name):
        print("Volume with name '%s' already exists" % name)
        return ReturnCode.FAILURE
    if storage.new_volume(name, size, folder_id):
        return ReturnCode.SUCCESS
    else:
        return ReturnCode.FAILURE


def volume_list(storage: StorageCenter, folder_id: '') ->int:
    table = Texttable(max_width=120)
    all_volumes = storage.volume_list()
    if folder_id:
        all_volumes = all_volumes.find_by_parent_folder(folder_id)
    table.header(['Volume', 'Instance ID', 'Parent Folder', 'WWID'])
    table.set_cols_dtype(['t', 't', 't', 't'])
    for volume in all_volumes:
        table.add_row([volume.name, volume.instance_id, volume.parent_folder_id, volume.wwid])
    print(table.draw())

    return ReturnCode.SUCCESS


def volume_folder_list(storage: StorageCenter, parent_id='') ->int:
    # TODO: Adaptable table width
    table = Texttable(max_width=120)
    table.header(['Folder', 'Instance ID', 'Parent Instance ID'])
    table.set_cols_dtype(['t', 't', 't'])
    folder_list = storage.volume_folder_list()
    if parent_id:
        folder_list = folder_list.find_by_parent_id(parent_id)
    for folder in folder_list:
        table.add_row([folder.name, folder.instance_id, folder.parent_id])
    print(table.draw())

    return ReturnCode.SUCCESS


def volume_folder_create(storage: StorageCenter, folder_name: str, folder_parent_id: str='',
                         unique_name: bool=True) ->int:
    if unique_name and storage.volume_folder_list().find_by_name(folder_name):
        print("Volume folder with name '%s' already exists" % folder_name)
        return ReturnCode.FAILURE
    if storage.new_volume_folder(folder_name, folder_parent_id):
        return ReturnCode.SUCCESS
    else:
        return ReturnCode.FAILURE


def storage_center_list(session: ScmSession) ->int:
    storage_centers = session.storage_centers()
    table = Texttable()
    table.header(["Name", "IP", "Instance ID", "Serial"])
    table.set_cols_dtype(['t', 't', 't', 't'])

    for sc in storage_centers:
        table.add_row([sc.name, sc.ip_addr, sc.instance_id, sc.serial_num])
    print(table.draw())

    return ReturnCode.SUCCESS


def _find_storage_center(session: ScmSession, instance_id: str) ->Optional[StorageCenter]:
    sc = session.storage_centers().find_by_instance_id(instance_id)
    if sc is None:
        print("Failed to find storage center with instance ID '%s'. Try listing all storage "
              "centers with command 'storage_center list'" % cli_args.storage_id)
        return None
    else:
        return sc


def exit_cli(session: ScmSession, return_code: int) ->None:
    session.logout(silent=True)
    exit(return_code)


def parse_arguments() ->argparse.Namespace:
    parser = argparse.ArgumentParser()

    # General options
    parser.add_argument('-H', '--host', required=True, help="Hostname or IP address of Dell Storage Center")
    parser.add_argument('-P', '--port', default=3033, help="Management port of Dell storage Center")
    parser.add_argument('-u', '--user', help='Login username')
    parser.add_argument('-p', '--password', help='Login password')

    # Top level subcommands
    command_parser = parser.add_subparsers(dest='command')

    # Storage Center subcommands
    storage_center_parser = command_parser.add_parser(CMD_CONST_STORAGE_CENTER)
    storage_center_parser_cmd = storage_center_parser.add_subparsers(dest='storage_center_commands')
    # List Storage centers
    storage_center_parser_cmd.add_parser(CMD_CONST_STORAGE_CENTER_LIST)

    # Volume subcommands
    volume_parser = command_parser.add_parser(CMD_CONST_VOLUME)
    volume_parser_cmd = volume_parser.add_subparsers(dest='volume_commands')
    # Create volume
    volume_create_args = volume_parser_cmd.add_parser(CMD_CONST_VOLUME_CREATE)
    volume_create_args.add_argument('-s', '--size', required=True, help='Size of the new volume. Example: "500GB"')
    volume_create_args.add_argument('-n', '--name', required=True, help='Name of the new volume')
    volume_create_args.add_argument('-f', '--folder-id', dest='folder_id', help='Instance ID of parent folder')
    volume_create_args.add_argument('-I', '--storage-id', required=True, dest='storage_id',
                                    help='Instance ID of storage center where the volume will be created')
    volume_create_args.add_argument('-Q', '--non-unique-name', default=False, action='store_true',
                                    help='If this flag is present, volume creation wont fail if there is another '
                                         'volume with the same name')
    # List Volumes
    volume_list_args = volume_parser_cmd.add_parser(CMD_CONST_VOLUME_LIST)
    volume_list_args.add_argument('-I', '--storage-id', required=True, dest='storage_id',
                                  help='Instance ID of storage center where the volume will be created')
    volume_list_args.add_argument('-f', '--folder-id', dest='folder_id', help='Instance ID of folder from '
                                                                              'which the volumes will be listed')

    # Volume Folder subcommands
    volume_folder_parser = command_parser.add_parser('volume_folder')
    volume_folder_parser_cmd = volume_folder_parser.add_subparsers(dest='volume_folder_commands')
    # List volume folders
    volume_folder_list_args = volume_folder_parser_cmd.add_parser(CMD_CONST_VOLUME_FOLDER_LIST)
    volume_folder_list_args.add_argument('-I' '--storage-id', required=True, dest='storage_id',
                                         help='Instance ID of storage center from which, volume folders will be listed')
    volume_folder_list_args.add_argument('-f', '--folder-id', dest='folder_id',
                                         help='Instance ID of folder from which the child volumes will be listed')

    # Create volume folder
    volume_folder_create_args = volume_folder_parser_cmd.add_parser(CMD_CONST_VOLUME_FOLDER_CREATE)
    volume_folder_create_args.add_argument('-n', '--name', help='Name of the new folder')
    volume_folder_create_args.add_argument('-f', '--folder-id', dest='folder_id', help='Instance ID of parent folder')
    volume_folder_create_args.add_argument('-I' '--storage-id', required=True, dest='storage_id',
                                           help='Instance ID of storage center from which,'
                                                'volume folders will be listed')
    volume_folder_create_args.add_argument('-Q', '--non-unique-name', default=False, action='store_true',
                                           help='If this flag is present, folder creation wont fail if there is another'
                                                ' folder with the same name in specified parent folder')
    return parser.parse_args()


def execute_command(args: argparse.Namespace, session: ScmSession) ->int:

    # Volume commands
    if args.command == CMD_CONST_VOLUME:
        # Create Volume
        if args.volume_commands == CMD_CONST_VOLUME_CREATE:
            unique_name = not args.non_unique_name
            sc = _find_storage_center(session, args.storage_id)
            if sc is None:
                rc = ReturnCode.FAILURE
            else:
                rc = volume_create(sc, args.name, args.size, unique_name)
        # List Volumes
        elif args.volume_commands == CMD_CONST_VOLUME_LIST:
            sc = _find_storage_center(session, args.storage_id)
            if sc is None:
                rc = ReturnCode.FAILURE
            else:
                parent_id = args.folder_id or ''
                rc = volume_list(sc, parent_id)
        # Default branch
        else:
            rc = ReturnCode.FAILURE

    # Storage Center commands
    elif args.command == CMD_CONST_STORAGE_CENTER:
        # List Storage Centers
        if args.storage_center_commands == CMD_CONST_STORAGE_CENTER_LIST:
            rc = storage_center_list(session)
        # Default branch
        else:
            rc = ReturnCode.FAILURE

    # Volume Folder commands
    elif args.command == CMD_CONST_VOLUME_FOLDER:
        # List Volume folders
        if args.volume_folder_commands == CMD_CONST_VOLUME_FOLDER_LIST:
            sc = _find_storage_center(session, args.storage_id)
            if sc is None:
                rc = ReturnCode.FAILURE
            else:
                parent_folder_id = args.folder_id or ''
                rc = volume_folder_list(sc, parent_folder_id)
        # Create volume folder
        elif args.volume_folder_commands == CMD_CONST_VOLUME_FOLDER_CREATE:
            unique_name = not args.non_unique_name
            sc = _find_storage_center(session, args.storage_id)
            if sc is None:
                rc = ReturnCode.FAILURE
            else:
                rc = volume_folder_create(sc, args.name, args.folder_id, unique_name)
        # Default branch
        else:
            rc = ReturnCode.FAILURE
    else:
        rc = ReturnCode.FAILURE

    return rc


if __name__ == '__main__':
    # parse CLI arguments
    cli_args = parse_arguments()

    # Request missing arguments via CLI dialog
    if not cli_args.user:
        cli_args.user = input("Username: ")

    if not cli_args.password:
        cli_args.password = getpass.getpass()

    # Initialize Session with Storage controller
    scm_session = ScmSession(cli_args.user, cli_args.password, cli_args.host, cli_args.port, verify_cert=False)
    if not scm_session.login():
        exit(ReturnCode.SUCCESS)

    ret_code = execute_command(cli_args, scm_session)
    exit_cli(scm_session, ret_code)
