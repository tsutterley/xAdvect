#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
xAdvect
=======

Python tools for advecting point data for use in a
Lagrangian reference frame powered by xarray

Documentation is available at https://xAdvect.readthedocs.io
"""

# base modules
import xAdvect.interpolate
import xAdvect.spatial
import xAdvect.tools
import xAdvect.utilities
from xAdvect.advect import Advect
from xAdvect import io, datasets

import xAdvect.version

# get version number
__version__ = xAdvect.version.version
