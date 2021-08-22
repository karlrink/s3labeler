
# -*- coding: utf-8 -*-

"""setup.py: setuptools control."""

from setuptools import setup

setup(
    name = "s3labeler",
    packages = ["s3labeler"],
    entry_points = {
        "console_scripts": ['s3labeler = s3labeler.s3labeler:main']
        },
    version = '1.0.3',
    description = "label s3 objects",
    long_description = "label s3 objects cli and REST API",
    author = "Karl Rink",
    author_email = "karl@rink.us",
    url = "https://gitlab.com/krink/s3labeler",
    install_requires = [ ]
    )

