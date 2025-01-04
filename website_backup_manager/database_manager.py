from abc import ABC, abstractmethod
from helpers import Helpers as Utils
import subprocess
import shlex
import os
import threading

class DatabaseConnectionError(Exception):
    """Custom exception for database connection errors."""
    

class Database(ABC) :

    def __init__(self, db_name, username, password, host='localhost', port=3306, local_base_path='.'):
        self.db_name = db_name
        self.username = username
        self.password = password
        self.host = host
        self.port = port
        self.local_base_path = local_base_path

    def dump_path(self,output_dump_file_name=None):

        if output_dump_file_name is None:
            output_dump_file_name = self.db_name
        else:
            if '.' in output_dump_file_name:
                output_dump_file_name = output_dump_file_name.split('.')[0]
        
        return os.path.join(self.local_base_path, f"{output_dump_file_name}.sql")
    
    @abstractmethod
    def dump(self, output_dump_name='db_backup.sql'):
        pass

    def testConnection(self, command = None):
        
        if command is None:
            return Utils.log("Please provide command to test connection.")
        
        Utils.log(f"Testing connection to {self.__class__.__name__} '{self.db_name}'...")

        try:
            Utils.retry(lambda: subprocess.run(shlex.split(command), check=True), retries=1)
            Utils.log(f"Testing connection to {self.__class__.__name__} '{self.db_name}' was successful.")

        except subprocess.CalledProcessError as e:
             raise DatabaseConnectionError(f"Connection to {self.__class__.__name__} '{self.db_name}' failed\n\n{e}\n")
        
    def getDatabaseSize(self,command = None) -> int:

        if command is None:
            return Utils.log("Please provide command to get database size.")

        try:
            # Estimate structure-only dump size
            structure_cmd = f"mysqldump -u {self.username} -p{self.password} -h {self.host} -P {self.port} --no-data {self.db_name}"
            structure_result = subprocess.run(shlex.split(structure_cmd), capture_output=True, text=True, check=True)
            structure_size = len(structure_result.stdout.encode())  # Convert to bytes



            result = Utils.retry(lambda: subprocess.run(shlex.split(command), capture_output=True, text=True, check=True), retries=1)
            total_size = 0
            for line in result.stdout.splitlines()[1:]:  # Skip the header line
                columns = line.split("\t")
                data_length = int(columns[6])  # Data_length column
                # index_length = int(columns[8])  # Index_length column
                total_size += data_length 

            return total_size + structure_size
        except:
            return 0

    def monitorProgress(self, file_path=None, interval=0.5):
        """Monitor the progress of the dump file size periodically."""
        
        if file_path is None:
            file_path = self.dump_path()

        monitorThread = threading.Thread(target=Utils.monitorDownload,args=(file_path,), daemon=True)

        monitorThread.start()
        
        return monitorThread


class MySQLDatabase(Database):

    def __init__(self, db_name, username, password, host='localhost', port=3306, local_base_path='.'):
        super().__init__(db_name, username, password, host, port, local_base_path)

    def testConnection(self, command = None):
        """Test Mysql connection."""

        try:
            command = f"mysql -u {self.username} -p{self.password} -h {self.host} -P {self.port} -e 'SHOW DATABASES;'"
            super().testConnection(command)
        
        except DatabaseConnectionError as e:
            Utils.log(e, level='error')
            raise


    def getDatabaseSize(self, command = None) -> int:
        """Estimate the database size by summing up table data and index lengths using subprocess."""

        command = f"mysql -u {self.username} -p{self.password} -h {self.host} -P {self.port} -D {self.db_name} -e 'SHOW TABLE STATUS'"

        return super().getDatabaseSize(command)
        
    def dump(self, output_file_name=None):
        """Take a MySQL dump with robust handling for large databases."""
        
        Utils._makeDirs(self.local_base_path)

        dump_path = self.dump_path(output_file_name)

        command = [
            "mysqldump",
            "-u", self.username,
            f"-p{self.password}",
            "-h", self.host,
            "-P", str(self.port),
            "--single-transaction",
            "--quick",
            "--max-allowed-packet=512M",
            self.db_name
        ]

        error_message = None

        try:
            Utils.log(f"Starting database dump to {dump_path}...")

            """ Start Monitoring the progress of the dump file size periodically."""
            self.monitorProgress(dump_path)

            with open(dump_path, 'wb', buffering=16 * 1024 * 1024) as dump_output:
                process = subprocess.Popen(command, stdout=dump_output, stderr=subprocess.PIPE)
                _, stderr = process.communicate()

            if process.returncode != 0:
                raise subprocess.CalledProcessError(process.returncode, command, stderr=stderr)
            
        except KeyboardInterrupt:
            error_message = "Database dump process interrupted by user."
        except subprocess.CalledProcessError as e:
            error_message = f"Database dump process failed: {e.stderr.decode()}"
        except Exception as e:
            error_message = f"Unexpected error during dump: {e}"
        finally:
            if error_message is not None:
                Utils.log(error_message,level='error')
            else:
                Utils.log(f"Database dump completed successfully: {dump_path}")
