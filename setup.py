from setuptools import setup, find_packages

setup(
    name="gcolab_pkg",
    version="0.1",
    packages=find_packages(),
    install_requires=["google-cloud-bigquery", "pandas", "ipython"],
)
