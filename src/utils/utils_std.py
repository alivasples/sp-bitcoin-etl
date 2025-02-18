"""
Author: Alexis Aspauza
Date: 2025-02-16
Description: Standard python utils for the project
"""

import pytz
import os
from datetime import datetime

class UtilsSTD():
    region = "UTC"

    @staticmethod
    def get_now_str():
        """
        Returns the now datetime in format %Y-%m-%d %H:%M:%S
        """
        tz = pytz.timezone(UtilsSTD.region)
        dateTimeObj = datetime.now(tz)
        return dateTimeObj.strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def get_now_datetime():
        """
        Creates a datetime object in region time.
        """
        tz = pytz.timezone(UtilsSTD.region)
        dateTimeObj = datetime.now(tz)
        return dateTimeObj

    @staticmethod
    def get_filenames(folder_path: str, suffix: str=""):
        """
        This method returns a list of sorted filenames in a path.

        Args:
            folder_path (str): folder where to find for files
            suffix (str): specific suffix for the filenames to retrieve in the folder

        Return:
            sorted filenames list
        """
        filenames = os.listdir(folder_path)
        filenames = [filename for filename in filenames if filename.endswith(suffix)]
        return sorted(filenames)