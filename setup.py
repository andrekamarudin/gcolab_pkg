from setuptools import find_packages, setup

setup(
    name="gcolab_pkg",
    version="0.1",
    description="A package for Google Colab functionalities.",
    packages=find_packages(),
    install_requires=[
        "google-cloud-bigquery",
        "pandas",
        "ipython",
        "colorama",
        "pytz",
    ],
)
