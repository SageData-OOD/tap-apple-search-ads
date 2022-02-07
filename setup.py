#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-apple-search-ads",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_apple_search_ads"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        "singer-python",
        "requests",
        "PyJWT",
        "cryptography"
    ],
    entry_points="""
    [console_scripts]
    tap-apple-search-ads=tap_apple_search_ads:main
    """,
    packages=["tap_apple_search_ads"],
    package_data = {
        "schemas": ["tap_apple_search_ads/schemas/*.json"]
    },
    include_package_data=True,
)
