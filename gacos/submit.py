import time
from pathlib import Path
from typing import Union

import numpy as np
import requests
from tqdm.auto import tqdm

from .datasets import SarDataset


class Submitter:
    def __init__(
        self,
        dataset: SarDataset,
        download_dir: Union[Path, str],
        email: str,
        gacos_url="http://www.gacos.net/M/action_page.php",
    ) -> None:
        self.dataset = dataset
        self.download_dir = Path(download_dir)
        self.email = email
        self.gacos_url = gacos_url

    def __post_data(self, data):
        """Post data to gacos website."""
        r = requests.post(self.gacos_url, data=data)
        return "Thanks for using GACOS!" in r.text

    def post_request(self):
        failed = []
        succeed = []

        # post gacos info to website
        for dates in self.dataset.date_patches:
            try:
                post_data = self.dataset.get_post_data(dates, self.email)
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
        for dates in self.dataset.date_patches:
            try:
                post_data = self.dataset.get_post_data(dates)
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
