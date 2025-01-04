from helpers import Helpers as Utils
import os
import threading
from ftplib import FTP_TLS, error_perm


class FTPConnectionError(Exception):
    """Custom exception for FTP connection errors."""    

class FTP:

    def __init__(self, host, username, password, local_base_path, host_base_path):
        self.host = host
        self.username = username
        self.password = password
        self.local_base_path = local_base_path
        self.host_base_path = host_base_path
        self.connected = False
        self.ftp = None

    @property
    def className(self):
        return self.__class__.__name__

    def connect(self):
        """Establish an FTPS connection with retries."""
        try:
            Utils.log(f"Connecting to {self.className} host {self.host}...")
            self.ftp = FTP_TLS()
            Utils.retry(lambda: self.ftp.connect(self.host))
            self.ftp.login(self.username, self.password)
            self.ftp.prot_p()
            self.ftp.set_pasv(True)
            self.connected = True
            self.__keepSessionAlive()
            Utils.log("FTPS connection established.")
        except Exception as e:
            raise FTPConnectionError(f"Connection to {self.className} host {self.host} failed\n\n{e}\n")

    def disconnect(self):
        """Close the FTPS connection safely."""
        if self.connected:
            Utils.log(f"Disconnecting to {self.className} host {self.host}...")
            try:
                self.ftp.quit()
                self.ftp = None
            except Exception as e:
                Utils.log(f"Error disconnecting to {self.className} host {self.host}",level='error')
        else:
            Utils.log(f"No active {self.className} connection to close.")
        
        self.connected = False

    def __keepSessionAlive(self, interval=60):
        """Start a new thread to send periodic NOOP commands to keep the connection alive."""

        import time
        def send_noop():
            while self.connected:
                time.sleep(interval)
                self.ftp.voidcmd("NOOP")

        threading.Thread(target=send_noop, daemon=True).start()

    def __verifyFileSize(self, remote_path, local_path):
        """Ensure the local file size matches the remote file size."""
        
        remote_size = self.ftp.size(remote_path)
        local_size = os.path.getsize(local_path)

        if remote_size != local_size:
            raise ValueError(f"Size mismatch: {remote_path} (expected {remote_size}, got {local_size})")
        
        Utils.log(f"Verified {remote_path}: {local_size} bytes")

    def __downloadWithProgress(self, remote_file, output_file_path):
        
        """Handle downloading a file with progress updates."""
        
        self.ftp.voidcmd("TYPE I")  # Set binary mode

        total_size = self.ftp.size(remote_file)
        downloaded = 0

        Utils.log(f"Starting downloading {os.path.basename(remote_file)} to {output_file_path}")

        with open(output_file_path, 'wb') as f:
            def progressing(data):
                nonlocal downloaded
                f.write(data)
                downloaded += len(data)
                percent = (downloaded / total_size) * 100
                Utils.log(f"{os.path.basename(remote_file)} | {downloaded} of {total_size} bytes downloaded. ({percent:.2f}%)")

            self.ftp.retrbinary(f"RETR {remote_file}", progressing, blocksize=1024 * 1024)  # 1 MB chunks

        Utils.log(f"[{os.path.basename(remote_file)}] downloaded successfully to {output_file_path}")


    def isDir(self, remote_path):
        """Check if the remote path is a directory."""
        current = self.ftp.pwd()
        try:
            self.ftp.cwd(remote_path)
            self.ftp.cwd(current)  # Revert to original directory
            return True
        except:
            return False
        

    def downloadFile(self, remote_path, local_path, max_retries=3):
        """Download a single file with progress tracking and retry logic."""
        
        """ Creating a directories in local path if it doesn't exist """
        Utils._makeDirs(os.path.dirname(local_path))

        try:
            Utils.retry(lambda: self.__downloadWithProgress(remote_path, local_path), retries=max_retries)

            self.__verifyFileSize(remote_path, local_path)
        except Exception as e:
            Utils.log(f"Unable to download File {remote_path}.",level='error')

    def downloadDir(self, remote_dir, local_dir, max_retries=3):
        """Recursively download all files and subdirectories."""

        """ Creating a directories in local path if it doesn't exist """
        Utils._makeDirs(local_dir)

        try:
            self.ftp.cwd(remote_dir)
            for item in self.ftp.nlst():
                if item not in ('.', '..'):
                    item_name = os.path.basename(item)
                    local_path = os.path.join(local_dir, item_name)

                    if self.isDir(item):
                        self.downloadDir(item, local_path, max_retries)
                    else:
                        self.downloadFile(item, local_path, max_retries)
            self.ftp.cwd('..')
        except Exception as e:
            raise


    def download(self, remote_dir_name):
        """Main entry point for downloading files or directories."""
        local_path = os.path.join(self.local_base_path, os.path.basename(remote_dir_name))


        remote_path = os.path.join(self.host_base_path, remote_dir_name)

        error_message = None

        try:
            self.connect()
            if self.isDir(remote_path):
                self.downloadDir(remote_path, local_path)
            else:
                self.downloadFile(remote_path, local_path)

        except KeyboardInterrupt:
            error_message = f"Downloading {remote_path} from FTP host {self.host} was interrupted by user."
        except FTPConnectionError as e:
            error_message = str(e)
        except Exception as e:
            error_message = f"Unexpected error while downloading {remote_path} from FTP host {self.host} :\n\n{e}\n"
        finally:
            if error_message is not None:
                Utils.log(error_message,level='error')
            else:
                Utils.log(f"Downloading {remote_path} from FTP host {self.host} completed successfully.")

            self.disconnect()
