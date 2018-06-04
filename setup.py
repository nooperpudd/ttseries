# encoding:utf-8
import codecs
import os
import re

from setuptools import setup, find_packages


def find_version(*file_paths):
    """
    Don't pull version by importing package as it will be broken due to as-yet uninstalled
    dependencies, following recommendations at  https://packaging.python.org/single_source_version,
    extract directly from the init file
    """
    here = os.path.abspath(os.path.dirname(__file__))
    with codecs.open(os.path.join(here, *file_paths), 'r', encoding="utf-8") as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


setup(
    name="ttseries",
    platforms="any",
    version=find_version("ttseries", "__init__.py"),
    author="Winton Wang",
    author_email="365504029@qq.com",
    url="https://github.com/nooperpudd/ttseries",
    license="LGPLv3",
    description="Time series data store in Redis",
    keywords="Time Series, Redis Time Series",
    long_description=codecs.open("README.rst", encoding="utf-8").read(),
    zip_safe=False,
    python_requires=">=3.6",
    packages=find_packages(exclude=["tests*", "docs"]),
    install_requires=codecs.open("requirements.txt", encoding="utf-8").readlines(),
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved",
        "License :: OSI Approved :: GNU Lesser General Public License v3 (LGPLv3)",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.6",
        "Topic :: Software Development",
        "Topic :: Software Development :: Libraries"
    ],

)
