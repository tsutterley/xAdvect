"""
Input/output functions for reading velocity data
"""

import os
from .dataset import *

# set environmental variable for anonymous s3 access
os.environ["AWS_NO_SIGN_REQUEST"] = "YES"
