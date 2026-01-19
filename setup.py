
from setuptools import setup, find_packages

setup(
    name="gov_credit",
    version="0.1.0",
    packages=find_packages(),
    install_requires=["pyspark", "delta-spark"]
)
