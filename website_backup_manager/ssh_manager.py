import paramiko
import os

from helpers import Helpers as Utils

class SSHConnectionError(Exception):
    """Custom exception for SSH connection errors."""
    pass

class SSH:
    def __init__(self, host, user, password, local_base_url, host_base_url, port=22):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.local_base_url = local_base_url
        self.host_base_url = host_base_url
        self.ssh = None
        self.connected = False

    def connect(self):
        """Establish an SSH connection to the server."""
        if self.connected:
            Utils.log("Already connected to the server.")
            return

        try:
            Utils.log("Establishing SSH connection...")
            self.ssh = paramiko.SSHClient()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.host, username=self.user, password=self.password, port=self.port)
            self.connected = True
            Utils.log("SSH connection established.")
        except Exception as e:
            Utils.log(f"Error establishing SSH connection: {e}",level='error')
            raise SSHConnectionError(f"Failed to connect to {self.host}")

    def disconnect(self):
        """Close the SSH connection."""
        if not self.connected:
            Utils.log("No active SSH connection to close.")
            return

        try:
            Utils.log("Closing SSH connection...")
            if self.ssh:
                self.ssh.close()
            self.connected = False
            Utils.log("SSH connection closed.")
        except Exception as e:
            Utils.log(f"Error closing connection: {e}",level='error')

    def _execute_command(self, command):
        """Execute a command on the remote server and return the output."""
        if not self.connected:
            raise SSHConnectionError("Not connected to the server.")

        stdin, stdout, stderr = self.ssh.exec_command(command)
        exit_status = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error = stderr.read().decode()
        return exit_status, output, error

    def create_remote_archive(self, remote_dir_path, archive_name):
        """
        Create a tar.gz archive of the specified remote directory.
        :param remote_dir_path: Directory to be archived.
        :param archive_name: Name of the output archive file.
        """

        self._validate_remote_directory(remote_dir_path)
        
        archive_path = os.path.join(self.host_base_url, f"{archive_name}.tar.gz")
        command = f'tar -czf {archive_path} -C {remote_dir_path} . --ignore-failed-read --warning=no-file-changed'
        Utils.log(f"Creating archive: {archive_path}...")

        exit_status, _, error = self._execute_command(command)

        if exit_status == 0:
            Utils.log(f"Archive {archive_name}.tar.gz created successfully at {archive_path}.")
        else:
            Utils.log(f"Error creating archive: {error}",level='error')

    def _validate_remote_directory(self, remote_dir_path):
        """Ensure the specified remote directory exists."""
        command = f'ls {remote_dir_path}'
        exit_status, _, error = self._execute_command(command)
        if exit_status != 0:
            raise FileNotFoundError(f"{error} or SSH may be inactive on server.")
       

    def make_archive(self, remote_dir_name, archive_name = None):
        """Public method to create a remote archive with connection management."""
        try:
            self.connect()
            remote_path = os.path.join(self.host_base_url, remote_dir_name)
            if archive_name is None:
                archive_name = remote_dir_name.lower()
            self.create_remote_archive(remote_path, archive_name)
        except KeyboardInterrupt:
            Utils.log("Process interrupted by user.",level='error')
        except Exception as e:
            Utils.log(f"An error occurred: {e}",level='error')
        finally:
            self.disconnect()
