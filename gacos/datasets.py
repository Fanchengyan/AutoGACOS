from pathlib import Path
from typing import Union

import numpy as np
import pandas as pd
from faninsar.datasets import HyP3, LiCSAR
import warnings

warnings.filterwarnings("ignore")

class SarDataset:
    def __init__(
        self,
        bounds: tuple[float, float, float, float],
        date_time: pd.DatetimeIndex,
    ) -> None:
        self.bounds = bounds
        self._times = date_time.strftime("%H:%M")
        self._dates = date_time.strftime("%Y%m%d")
        self.datetime_patches = self._gen_datetime_patches(date_time)

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}(\n"
            f"    bounds={self.bounds}, \n"
            f"    times={len(set(self.times))}, \n"
            f"    dates={len(self.dates)}\n"
            ")"
        )

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def dates(self):
        """The dates ('%Y%m%d') of the acquisitions parsed from dataset. """
        return self._dates

    @property
    def times(self):
        """The times ('%H:%M') of the acquisitions parsed from dataset."""
        return self._times

    def _gen_datetime_patches(self, date_time: pd.DatetimeIndex) -> dict:
        """Generate datetime patches.

        Parameters
        ----------
        date_time : pd.DatetimeIndex
            The datetime index.

        Returns
        -------
        datetime_patches : dict
            The datetime patches. The key is the time (HH:MM) and the value is
            the datetime patches.
        """
        nums = 20
        datetime_patches = {}

        for _dt in np.unique(self.times):
            _dts = self.dates[self.times == _dt]
            n_patch = np.ceil(len(_dts) / nums)
            dates_patch = np.array_split(_dts, n_patch)
            datetime_patches[_dt] = dates_patch

        return datetime_patches

    def gen_post_data(
        self,
        dates: Union[list, np.ndarray],
        time: Union[tuple[int, int], tuple[str, str]],
        email: str,
    ):
        """Generate post data for gacos website.

        Parameters
        ----------
        date : list or np.ndarray
            The list of dates.
        time : tuple[int, int]
            The time of the acquisition (hour, minute).
        email : str
            The email address to receive the gacos data.

        Returns
        -------
        post_data : dict
            The post data.
        """
        if isinstance(dates, np.ndarray):
            dates = dates.tolist()
        time = [int(t) for t in time]

        post_data = {
            "N": self.bounds[3],
            "W": self.bounds[0],
            "S": self.bounds[1],
            "E": self.bounds[2],
            "H": time[0],
            "M": time[1],
            "date": "\n".join(dates),
            "type": "2",
            "email": email,
        }
        return post_data


class LiCSARDataset(SarDataset):
    def __init__(self, home_dir: Union[Path, str]) -> None:
        self.home_dir = Path(home_dir)
        self.dataset = LiCSAR(home_dir)
        bounds = self.dataset.bounds
        time = self._get_time()
        dates = self.dataset.pairs.dates
        date_time = pd.to_datetime([f"{d} {time[0]}:{time[1]}:00" for d in dates])
        super().__init__(bounds, date_time)

    def _get_time(self):
        """Get the acquisition time of acquisitions.

        Returns
        -------
        time: tuple[int, int]
            A tuple of hour and minute representing the acquisition time.

        Raises
        ------
        ValueError
            If no center_time found in metadata.txt.
        """
        meta_file = sorted(self.home_dir.rglob("metadata.txt"))[0]

        with open(meta_file) as f:
            lines = f.readlines()
            time = None
            for line in lines:
                line_split = line.split("=")
                key, value = (line_split[0].strip(), line_split[1])
                if "center_time" == key:
                    center_time = value.strip()
                    hour, minute, second = center_time.split(":")
                    hour, minute, second = int(hour), int(minute), float(second)
                    minute = minute + int(np.round(second / 60, 0))
                    return hour, minute
                else:
                    continue
            if time is None:
                raise ValueError(f"No center_time found in {meta_file}")


class HyPe3Dataset(SarDataset):
    def __init__(self, home_dir: Union[Path, str]) -> None:
        self.dataset = HyP3(home_dir)
        bounds = self.dataset.bounds.to_crs("epsg:4326")
        date_time = self.dataset.datetime

        super().__init__(bounds, date_time)
