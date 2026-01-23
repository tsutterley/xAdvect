#!/usr/bin/env python
"""
netcdf.py
Written by Tyler Sutterley (01/2026)

Reads netCDF4 files as xarray Datasets with variable mapping

PYTHON DEPENDENCIES:
    h5netcdf: Python interface to HDF5 and netCDF4
        https://pypi.org/project/h5netcdf/
    pyproj: Python interface to PROJ library
        https://pypi.org/project/pyproj/
        https://pyproj4.github.io/pyproj/
    xarray: N-D labeled arrays and datasets in Python
        https://docs.xarray.dev/en/stable/

UPDATE HISTORY:
    Written 01/2026
"""

from __future__ import division, annotations

import os
import pyproj
import pathlib
import warnings
import xarray as xr
import xAdvect.utilities

# attempt imports
dask = xAdvect.utilities.import_dependency("dask")
dask_available = xAdvect.utilities.dependency_available("dask")

# set environmental variable for anonymous s3 access
os.environ["AWS_NO_SIGN_REQUEST"] = "YES"
# suppress warnings
warnings.filterwarnings("ignore", category=UserWarning)


# PURPOSE: read a list of files
def open_mfdataset(filename: list[str] | list[pathlib.Path], **kwargs):
    """
    Open multiple netCDF4 files

    Parameters
    ----------
    filename: list of str or pathlib.Path
        list of files
    parallel: bool, default False
        Open files in parallel using ``dask.delayed``
    **kwargs: dict
        additional keyword arguments for opening files
    Returns
    -------
    ds: xarray.Dataset
        xarray Dataset
    """
    # set default keyword arguments
    kwargs.setdefault("parallel", False)
    parallel = kwargs.get("parallel") and dask_available
    # read each file as xarray dataset and append to list
    if parallel:
        opener = dask.delayed(open_dataset)
        (d,) = dask.compute([opener(f, **kwargs) for f in filename])
    else:
        d = [open_dataset(f, **kwargs) for f in filename]
    # merge datasets
    ds = xr.merge(d, compat="override")
    # return xarray dataset
    return ds


def open_dataset(
    filename: str,
    mapping: dict | None = None,
    chunks: int | dict | str | None = None,
    **kwargs,
) -> xr.Dataset:
    """Open a netCDF4 file as an xarray Dataset and remap variables

    Parameters
    ----------
    filename: str
        Path to netCDF4 file
    mapping: dict or None, default None
        Dictionary mapping standard variable names to those in the file
    chunks: int, dict, str, or None, default None
        variable chunk sizes for dask (see ``xarray.open_dataset``)

    Returns
    -------
    ds: xr.Dataset
        xarray Dataset
    """
    # get coordinate reference system (CRS) information from kwargs
    crs = kwargs.get("crs", None)
    # open the NetCDF file using xarray
    tmp = xr.open_dataset(filename, mask_and_scale=True, chunks=chunks)
    # apply variable mapping if provided
    if mapping is not None:
        # create xarray dataset
        ds = xr.Dataset()
        for key, value in mapping.items():
            ds[key] = tmp[value]
        # copy attributes
        ds.attrs = tmp.attrs.copy()
    else:
        ds = tmp.copy()
    # attach coordinate reference system (CRS) information
    if crs is not None:
        ds.attrs["crs"] = pyproj.CRS.from_user_input(crs).to_dict()
    # return the xarray dataset
    return ds
