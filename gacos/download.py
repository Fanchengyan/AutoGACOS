import tarfile
from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd
from data_downloader import downloader
from faninsar.query import BoundingBox
from tqdm.auto import tqdm

from .parse_email import GACOSEmail


class Downloader:
    def __init__(
        self,
        url_file: Union[Path, str],
        output_dir: Union[Path, str],
        tar_gz_dir: Optional[Union[Path, str]] = None,
        keep_original: bool = False,
        time: Optional[float] = None,
        bounds: Optional[tuple[float, float, float, float]] = None,
    ) -> None:
        """Initialize Downloader class

        Parameters
        ----------
        url_file : Union[Path, str]
            Path to file containing URLs that created by :meth:`GACOSEmail.retrieve_gacos_urls`
        output_dir : Union[Path, str]
            directory to output gacos files
        tar_gz_dir : Optional[Union[Path, str]], optional
            directory to store downloaded *.tar.gz files. If None, then
            `output_dir` is used. Default is None.
        keep_original : bool, optional
            Whether to keep original files (*.tar.gz). Default is False.
        time : Optional[float], optional
            time of acquisition, used to filter out files that are not needed.
            Default is None.
        bounds : Optional[tuple[float, float, float, float]], optional
            bounds of area of interest with order (W, S, E, N), used to filter
            out files that are not needed. Default is None.
        """
        self.url_file = Path(url_file)
        self.output_dir = Path(output_dir)
        if tar_gz_dir is None:
            self.tar_gz_dir = self.output_dir
        self.keep_original = keep_original

        if not self.url_file.exists():
            raise FileNotFoundError(f"{self.url_file} does not exist")
        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True)
        if not self.tar_gz_dir.exists():
            self.tar_gz_dir.mkdir(parents=True)

        self.df_urls = pd.read_csv(self.url_file, header=0)

        # only keep urls that intersect with bounds
        if bounds is not None:
            mask_bbox = self._bbox_mask(BoundingBox(*bounds))

        # only keep urls that acquisition time is within 10 minutes of `time`
        if time is not None:
            mask_time = self._time_mask(time)

        if bounds is not None and time is not None:
            self.mask = mask_bbox & mask_time
        elif bounds is not None:
            self.mask = mask_bbox
        elif time is not None:
            self.mask = mask_time
        else:
            self.mask = np.ones(self.df_urls.shape[0], dtype=bool)

    def _bbox_mask(self, bounds) -> np.ndarray:
        intersection_bbox = np.array(
            [
                BoundingBox(*b).intersects(bounds)
                for b in zip(
                    self.df_urls["south"].astype(float),
                    self.df_urls["west"].astype(float),
                    self.df_urls["north"].astype(float),
                    self.df_urls["east"].astype(float),
                )
            ]
        )
        return intersection_bbox

    def _time_mask(self, time) -> np.ndarray:
        """Only keep urls that acquisition time is within 10 minutes of `time`"""
        intersection_time = np.array(
            np.abs(self.df_urls["time"].astype(float) - time)
            <= 1 / 60 * 10  # 10 minutes
        )
        return intersection_time

    def download(self) -> None:
        """Download GACOS files from URLs in file created by :meth:`GACOSEmail.retrieve_gacos_urls`"""
        urls_used = self.df_urls[self.mask]["url"].values

        for url in tqdm(urls_used):
            gz_file = self.tar_gz_dir / Path(url).name
            downloader.download_data(url, file_name=gz_file)
            self._extract_tar_gz(gz_file)
            if not self.keep_original:
                self._delete_file(gz_file)

    def _extract_tar_gz(self, gz_file) -> None:
        """Unzip/extract downloaded GACOS files

        Parameters
        ----------
        gz_file : Path
            path to downloaded GACOS file (*.tar.gz)
        """
        with tarfile.open(gz_file, "r:gz") as tar:
            tar.extractall(path=self.output_dir)

    def _delete_file(self, gz_file) -> None:
        """Delete original GACOS files

        Parameters
        ----------
        gz_file : Path
            path to downloaded GACOS file (*.tar.gz)
        """
        gz_file.unlink()
