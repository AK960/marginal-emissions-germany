# see pyproject.toml for explanation

from setuptools import setup, find_packages

setup(
    name="marginal_emissions",
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src")
)