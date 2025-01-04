from ftp_manager import FTP
from ssh_manager import SSH
from database_manager import MySQLDatabase
import os
import datetime

from helpers import Helpers as Utils

class remote_backup_manager:
    def __init__(self, ftp_config = None, ssh_config = None, db_config = None):

        self.ftp_downloader = None
        self.ssh_manager = None
        self.database_downloader = None

        if ftp_config is not None:
            self.ftp_downloader = FTP(**ftp_config)
        if ssh_config is not None:
            self.ssh_manager = SSH(**ssh_config)
        if db_config is not None:
            self.database_downloader = MySQLDatabase(**db_config)

    
    def download_from_ftp(self, remote_path):
        """Download files or directories from the FTP server."""
        if not self.ftp_downloader:
            Utils.log("No FTP downloader configured. Skipping FTP download.")
            return
        try:
            Utils.log(f"Starting FTP download for: {remote_path}")
            self.ftp_downloader.download(remote_path)
        except Exception as e:
            Utils.log(f"Error during FTP download: {e}",level='error')

    def create_remote_archive(self, remote_dir_name, archive_name = None):
        """Create a remote archive using SSH."""
        if not self.ssh_manager:
            Utils.log("No SSH manager configured. Skipping remote archiving.")
            return
        try:
            Utils.log(f"Creating archive for {remote_dir_name}...")
            self.ssh_manager.make_archive(remote_dir_name, archive_name)
        except Exception as e:
            Utils.log(f"Error during remote archiving: {e}",level='error')

    def dump_database(self, dump_name):
        """Take a MySQL database dump."""

        if not self.database_downloader:
            Utils.log("No database downloader configured. Skipping database dump.")
            return
        
        try:
            Utils.log(f"Initiating database dump: {dump_name}")
            self.database_downloader.dump(dump_name)
        except Exception as e:
            Utils.log(f"Error during database dump: {e}",level='error')

    def full_backup(self, website_dir_name, website_database_name, website_archive_name = None):
        """Perform a complete backup: FTP download, archive, and database dump."""
        try:
            Utils.log("**** Full backup process started ****")
            
            start_time = datetime.datetime.now()


            # Step 1: Create an archive of remote files
            self.create_remote_archive(website_dir_name, website_archive_name)


            if website_archive_name is None:
                website_archive_name = website_dir_name

            # Step 2: Download files from FTP server
            self.download_website_archive(website_archive_name)

            # Step 3: Take a database dump
            self.download_database_dump(website_database_name)

            Utils.log(f"**** Backup completed in {Utils.timeTaken(start_time)} ****")

        except Exception as e:
            Utils.log(f"Backup Interrupted with this error:\n{e}\n",level='error')


    def create_website_archive_on_server(self,remote_dir, archive_name):    
        # Create an archive of remote files
        self.create_remote_archive(remote_dir, archive_name)


    def download_website_archive(self,archive_name = 'backup'):    
        # Download files from FTP server
        self.download_from_ftp(f"{archive_name}.tar.gz")

    def download_database_dump(self,db_dump_name = 'database'):    
        # Take a database dump
        self.dump_database(db_dump_name)
        


if __name__ == "__main__":
    import os
    from dotenv import load_dotenv
    load_dotenv()


    # Website directory on server (name only)
    website_dir_name = 'themes'

    # Website database (name only)
    database_name = os.getenv('DB_NAME', 'test_db')

    # Local path where website backup will be downloaded
    local_base_path = f"/Users/hassan/Sites/BACKUPS/REMOTE_SERVER/{website_dir_name}"


    ssh_config = {
        'host': os.getenv('SSH_HOST', 'ssh.example.com'),
        'user': os.getenv('SSH_USER', 'sshuser'),
        'password': os.getenv('SSH_PASS', 'sshpassword'),
        'port': os.getenv('SSH_PORT', 22),
        'local_base_url': local_base_path,
        'host_base_url': '/home/u775440477/domains/hassandev.com/public_html/wp-content'  
    }

    ftp_config = {
        'host': os.getenv('FTP_HOST', 'ftp.example.com'),
        'username': os.getenv('FTP_USER', 'ftpuser'),
        'password': os.getenv('FTP_PASS', 'ftppassword'),
        'local_base_path': local_base_path,
        'host_base_path': './wp-content/'
    }

    db_config = {
        'db_name': database_name,
        'username': os.getenv('DB_USER', 'dbuser'),
        'password': os.getenv('DB_PASS', 'dbpassword'),
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': os.getenv('DB_PORT', 3306),
        'local_base_path': local_base_path
    }

    
    manager = remote_backup_manager(db_config=db_config)

    manager.full_backup(website_dir_name, database_name)