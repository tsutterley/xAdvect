#!/usr/bin/env python
"""
interpolate.py
Written by Tyler Sutterley (01/2026)
Interpolators for spatial data

PYTHON DEPENDENCIES:
    numpy: Scientific Computing Tools For Python
        https://numpy.org
        https://numpy.org/doc/stable/user/numpy-for-matlab-users.html
    scipy: Scientific Tools for Python
        https://docs.scipy.org/doc/
    xarray: N-D labeled arrays and datasets in Python
        https://docs.xarray.dev/en/stable/

UPDATE HISTORY:
    Written 01/2026
"""

from __future__ import annotations

import numpy as np
import scipy.fftpack
import scipy.spatial

__all__ = ["inpaint"]


def inpaint(
    xs: np.ndarray,
    ys: np.ndarray,
    zs: np.ndarray,
    N: int = 0,
    s0: int = 3,
    power: int = 2,
    epsilon: float = 2.0,
    **kwargs,
):
    """
    Inpaint over missing data in a two-dimensional array using a
    penalized least square method based on discrete cosine transforms
    :cite:p:`Garcia:2010hn,Wang:2012ei`

    Parameters
    ----------
    xs: np.ndarray
        input x-coordinates
    ys: np.ndarray
        input y-coordinates
    zs: np.ndarray
        input data
    N: int, default 0
        Number of iterations (0 for nearest neighbors)
    s0: int, default 3
        Smoothing
    power: int, default 2
        power for lambda function
    epsilon: float, default 2.0
        relaxation factor
    """
    # find masked values
    if isinstance(zs, np.ma.MaskedArray):
        W = np.logical_not(zs.mask)
    else:
        W = np.isfinite(zs)
    # no valid values can be found
    if not np.any(W):
        raise ValueError("No valid values found")

    # dimensions of input grid
    ny, nx = np.shape(zs)

    # calculate initial values using nearest neighbors
    # computation of distance Matrix
    # use scipy spatial KDTree routines
    xgrid, ygrid = np.meshgrid(xs, ys)
    tree = scipy.spatial.cKDTree(np.c_[xgrid[W], ygrid[W]])
    # find nearest neighbors
    masked = np.logical_not(W)
    _, ii = tree.query(np.c_[xgrid[masked], ygrid[masked]], k=1)
    # copy valid original values
    z0 = np.zeros((ny, nx), dtype=zs.dtype)
    z0[W] = np.copy(zs[W])
    # copy nearest neighbors
    z0[masked] = zs[W][ii]
    # return nearest neighbors interpolation
    if N == 0:
        return z0

    # copy data to new array with 0 values for mask
    ZI = np.zeros((ny, nx), dtype=zs.dtype)
    ZI[W] = np.copy(z0[W])

    # calculate lambda function
    L = np.zeros((ny, nx))
    L += np.broadcast_to(np.cos(np.pi * np.arange(ny) / ny)[:, None], (ny, nx))
    L += np.broadcast_to(np.cos(np.pi * np.arange(nx) / nx)[None, :], (ny, nx))
    LAMBDA = np.power(2.0 * (2.0 - L), power)

    # smoothness parameters
    s = np.logspace(s0, -6, N)
    for i in range(N):
        # calculate discrete cosine transform
        GAMMA = 1.0 / (1.0 + s[i] * LAMBDA)
        DISCOS = GAMMA * scipy.fftpack.dctn(W * (ZI - z0) + z0, norm="ortho")
        # update interpolated grid
        z0 = (
            epsilon * scipy.fftpack.idctn(DISCOS, norm="ortho")
            + (1.0 - epsilon) * z0
        )

    # reset original values
    z0[W] = np.copy(zs[W])
    # return the inpainted grid
    return z0
