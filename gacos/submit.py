import datetime as dt
import poplib
import re
import time
from email.parser import Parser
from email.utils import parseaddr
from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
import rasterio
import requests
from tqdm import tqdm

from .dataset_info import SarDatasetInfo


class GACOS:
    def __init__(
        self,
        data_info: SarDatasetInfo,
        download_dir: Union[Path, str],
        gacos_url="http://www.gacos.net/M/action_page.php",
    ) -> None:
        self.data_info = data_info
        self.download_dir = Path(download_dir)
        self.gacos_url = gacos_url

    def __post_data(self, data):
        """Post data to gacos website."""
        r = requests.post(self.gacos_url, data=data)
        return "Thanks for using GACOS!" in r.text

    def post_request(self):
        self.data_info.post_data
        failed = []
        succeed = []

        # post gacos info to website
        for dates in self.data_info.date_patches:
            try:
                post_data = self.data_info.get_post_data(dates)
                status_ok = self.__post_data(post_data)
                if status_ok:
                    succeed.append(post_data)
                    tqdm.write(f">>> succeed post: {post_data}")
                else:
                    failed.append(post_data)
                    tqdm.write(f">>> failed post: {post_data}")
            except:
                failed.append(post_data)
                tqdm.write(f">>> failed post: {post_data}")

            # wait to avoid be rejected
            time.sleep(np.random.randint(60, 60 * 20))

        return failed, succeed

    def download_from_email(self, save_dir):
        """Download gacos data from email."""
        failed = []
        succeed = []
        for dates in self.data_info.date_patches:
            try:
                post_data = self.data_info.get_post_data(dates)
                status_ok = self.__post_data(post_data)
                if status_ok:
                    succeed.append(post_data)
                    tqdm.write(f">>> succeed post: {post_data}")
                else:
                    failed.append(post_data)
                    tqdm.write(f">>> failed post: {post_data}")
            except:
                failed.append(post_data)
                tqdm.write(f">>> failed post: {post_data}")

            # wait to avoid be rejected
            time.sleep(np.random.randint(60, 60 * 20))

        return failed, succeed


