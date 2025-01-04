import os
import logging
import datetime
import time

class Helpers:

    logging.basicConfig(
        level=logging.INFO,
        format='%(levelname)s - %(message)s'
    )

    @staticmethod
    def log(message, level= 'info'):
        
        if level == 'info':
            logging.info(message)
        elif level == 'warning':
            logging.warning(message)
        elif level == 'error':
            logging.error(message)
        elif level == 'critical':
            logging.critical(message)
        else:
            logging.info(message)
    @staticmethod
    def timeTaken(start_time: datetime.datetime) -> str:
        """Calculate and format the time difference since start_time."""
        now = datetime.datetime.now()
        diff = datetime.timedelta(seconds=(now - start_time).total_seconds())
        return str(diff)
    
    @staticmethod
    def getSizeIn(filePath:str, unit = None) -> str:

        size = os.path.getsize(filePath)

        if unit == 'MB':
            return size / (1024 * 1024)
        elif unit == 'KB':
            return size / 1024
        else:
            return size

    @staticmethod
    def __makeDirs(path):
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            raise FileExistsError(f"Directory already exists: {path}")
    
    @staticmethod
    def _makeDirs(path):
        """Ensure that a directory exists, creating it if necessary."""
        try:
            Helpers.__makeDirs(path)
            Helpers.log(f"Directory created: {path}")
        except FileExistsError as e:
            Helpers.log(f"Directory already exists: {path}")

    @staticmethod
    def retry(operation, retries=3, delay=1):
        """
            Retry an operation in case of failure.

            Args:
                operation (function): The operation to be retried.
                retries (int, optional): The maximum number of retries. Defaults to 3.
                delay (int, optional): The delay between retries in seconds. Defaults to 1.

            Returns:
                The result of the successful operation.

            Raises:
                RuntimeError: If the operation fails after multiple retries.
        """
        for attempt in range(retries):
            try:
                return operation()
            except Exception as e:
                Helpers.log(f"Attempt {attempt + 1} failed: {e}", level='error')
                time.sleep(delay)
        
        raise RuntimeError("Operation failed after {retries} retries.")
    
    @staticmethod
    def monitorDownload(filePath: str, interval=0.5, max_stable_checks=10):
        """Monitor progress of a file being downloaded or created."""
        
        last_size = 0
        stable_checks = 0  # Tracks how many times size remains unchanged

        while True:
            if os.path.exists(filePath):
                current_size = os.path.getsize(filePath)
                # {percent:.2f}
                Helpers.log(f"{os.path.basename(filePath)} | {current_size:.2f} bytes downloaded. (__%)")

                if current_size == last_size:
                    stable_checks += 1
                    if stable_checks >= max_stable_checks:
                        Helpers.log(f"Assuming {os.path.basename(filePath)} downloaded successfully. Final size {current_size:.2f} bytes")
                        break  # Stop monitoring once size is stable
                else:
                    stable_checks = 0  # Reset if size changes

                last_size = current_size  # Update last size
            
            time.sleep(interval)