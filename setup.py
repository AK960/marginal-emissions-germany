"""
.SYNOPSIS
    Creating installable src package.

.DESCRIPTION
    This file makes it possible to install the package in the virtual environment and make it available globally for
    scripts and notebooks to use.

.PARAMETER
    Beschreibt einen spezifischen Parameter des Moduls.

.EXAMPLE
    Zeigt ein Beispiel für die Verwendung des Moduls.
"""
from setuptools import setup, find_packages

setup(
    name="marginal_emissions",
    version="0.1",
    package_dir={"": "src"},
    packages=find_packages(where="src")
)