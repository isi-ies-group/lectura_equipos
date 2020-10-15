# -*- coding: utf-8 -*-
"""
Created on Fri Mar  6 11:30:00 2020

@author: Rubén Núñez
"""

from setuptools import setup

setup_args = dict(
    name="lectura_equipos",
    version="0.1",
    url='http://github.com/rubennj/lectura_equipos',
    author="Rubén Núñez",
    author_email="ruben.nunez@upm.es",
    description="Pequeña librería que facilita la lectura de ficheros de datos de equipos del ISI IES-UPM",
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: Windows",
    ],
    python_requires='>=3.6',
    packages=['lectura_equipos'],
    zip_safe=False,
    package_data={'': ['*.txt','*.yaml']},
    include_package_data=True,
)

install_requires = [
    'pandas',
    'numpy',
    'pytz',
]

if __name__ == '__main__':
    setup(**setup_args, install_requires=install_requires)

