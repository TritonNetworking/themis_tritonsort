#!/usr/bin/python

import os

PARENT_PATH = os.path.abspath(os.path.dirname(__file__))

# Make every directory within this directory a sub-package of this package
__all__ = [x for x in os.listdir(PARENT_PATH) if
           os.path.isdir(os.path.join(PARENT_PATH, x))]
