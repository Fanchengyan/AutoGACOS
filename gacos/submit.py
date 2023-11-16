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
        # download_dir: Union[Path, str],
        email: str,
        sleep_time_range: tuple[int, int] = (60, 60 * 5),
        gacos_url="http://www.gacos.net/M/action_page.php",
    ) -> None:
        """Initialize Submitter class

        Parameters
        ----------
        dataset : SarDataset
            The SarDataset object.
        email : str
            The email address to submit to gacos.
        sleep_time_range : tuple[int, int], optional
            The range of sleep time in seconds. Default is (60, 60 * 5).
        gacos_url : str, optional
            The url of gacos website. Default is "http://www.gacos.net/M/action_page.php".
        """
        self.dataset = dataset
        # self.download_dir = Path(download_dir) #TODO: check if this is needed
        self.email = email
        self.sleep_time_range = sleep_time_range
        self.gacos_url = gacos_url

    def _post_data(self, data):
        """Post data to gacos website."""
        r = requests.post(self.gacos_url, data=data)
        return "Thanks for using GACOS!" in r.text

    def post_requests(self):
        # TODO:  check how to handle failed and succeed
        self.failed = []
        self.succeed = []

        # post gacos info to website
        for _key, _dates in tqdm(self.dataset.datetime_patches.items()):
            try:
                for _dt in tqdm(_dates):
                    post_data = self.dataset.gen_post_data(
                        _dt, _key.split(":"), self.email
                    )
                    status_ok = self._post_data(post_data)
                    if status_ok:
                        self.succeed.append(post_data)
                        tqdm.write(f">>> succeed post: {post_data}")
                    else:
                        self.failed.append(post_data)
                        tqdm.write(f">>> failed post: {post_data}")

                # wait to avoid be rejected
                sleep_time = np.random.randint(*self.sleep_time_range)
                tqdm.write(f"    sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)
            except:
                self.failed.append(post_data)
                tqdm.write(f">>> failed post: {post_data}")
