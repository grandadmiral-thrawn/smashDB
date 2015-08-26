#!/usr/bin/env python
try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name="smasher",
    version="0.1.0",
    description="Smashing things since 2015",
    author="Fox Peterson",
    author_email="<fox@tinybike.net>",
    maintainer="Fox Peterson",
    maintainer_email="<fox@tinybike.net>",
    license="MIT",
    url="https://github.com/dataRonin/smasher",
    download_url = "https://github.com/dataRonin/smasher/tarball/0.1.0",
    packages=["smasher"],
    install_requires=["matplotlib", "numpy", "pandas"],
    keywords = ["smash", "smashdb", "smashery"]
)
