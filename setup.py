# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
# https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
# <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
# option. This file may not be copied, modified, or distributed
# except according to those terms.

from setuptools import setup, find_packages
import setuptools

setuptools.setup(
    name="stock-market-recommendation",
    version="1.0",
    python_requires='==3.9.17',
    py_modules=['utilities'],
    description="generating stock market buy,sell,hold recommendations",
    author="Kfir Naftali",
    author_email="kfirnaftali@google.com",
    install_requires=['google-cloud-discoveryengine==0.11.6',
                      'google-api-core==2.11.0',
                      'google-cloud-aiplatform==1.38.0',
                      'google-auth==2.23.0',
                      'transformers==4.33.2'],
    packages=setuptools.find_packages()
)
