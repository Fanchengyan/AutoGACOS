from pathlib import Path
from typing import Optional, Union

import numpy as np
import pandas as pd
import rasterio
from tqdm.auto import tqdm


class SarDatasetInfo:
    def __init__(self, bounds, time, dates, email) -> None:
        self.bounds = bounds
        self.time = time
        self.dates = dates
        self.date_patches = self.get_date_patches()
        self.email = email

    def __str__(self) -> str:
        return f"{self.post_data}"

    def __repr__(self) -> str:
        return f"{self.post_data}"

    def get_date_patches(self):
        """Get the dates patch.

        Returns
        -------
        dates_patch : list
            The dates patch. Each element is a list of dates.
        """
        nums = 20
        n_patch = np.ceil(len(self.dates) / nums)
        dates_patch = np.array_split(self.dates, n_patch)

        return dates_patch

    def get_post_data(self, dates):
        """Get the post data.
        
        Parameters
        ----------
        date : list
            The list of dates.

        Returns
        -------
        post_data : dict
            The post data.
        """
        if isinstance(dates, np.ndarray):
            dates = dates.tolist()
            
        post_data = {
            "N": self.bounds[3],
            "W": self.bounds[0],
            "S": self.bounds[1],
            "E": self.bounds[2],
            "H": self.time[0],
            "M": self.time[1],
            "date": "\n".join(dates),
            "type": "2",
            "email": self.email,
        }

        return post_data


class LiCSARDatasetInfo(SarDatasetInfo):
    def __init__(
        self,
        home_dir: Union[Path, str],
        email: str,
    ) -> None:
        self.home_dir = Path(home_dir)
        self.bounds = self.get_bounds()
        self.time = self.get_time()
        super().__init__(self.bounds, self.time, self.get_dates(), email)

    def get_bounds(
        self, tif_pattern: str = "*.geo.hgt.tif"
    ) -> tuple[float, float, float, float]:
        """Get the bounds of the frame.
        Parameters
        ----------
        tif_pattern: str, optional
            The pattern of the tif file. Default is "*.geo.hgt.tif".

        Returns
        -------
        bounds: tuple
            The bounds of the frame.
        """
        tif_file = sorted(self.home_dir.rglob(tif_pattern))
        if len(tif_file) == 0:
            raise ValueError(f"No {tif_pattern} file found in {self.home_dir}")
        else:
            tif_file = tif_file[0]

        with rasterio.open(tif_file) as ds:
            bounds = ds.bounds
        return bounds

    def get_time(self):
        """Get the acquisition time of the frame.

        Returns
        -------
        time: float
            The acquisition time of the frame.

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

    def get_dates(self):
        baseline_file = list(self.home_dir.rglob("baselines"))
        if len(baseline_file) == 0:
            raise ValueError(f"No baselines file found in {self.home_dir}")

        baseline_file = baseline_file[0]
        df = pd.read_csv(
            baseline_file,
            sep="\s",
            engine="python",
            header=None,
            dtype=str,
            names=["primary", "secondary", "b1", "b2"],
        )
        dates = np.unique([df["secondary"], df["primary"]])

        return dates


if __name__ == "__main__":
    home_dir = Path("/Volumes/新加卷/jinping/055A_06018_131313")
    dataset = LiCSARDatasetInfo(home_dir, email="fanchy14@lzu.edu.cn")
