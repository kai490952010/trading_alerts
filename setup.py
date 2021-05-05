#!/usr/bin/env python

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [ ]

setup_requirements = ['pytest-runner', ]

test_requirements = ['pytest>=3', ]

setup(
    author="Kaishi Zhou",
    author_email='kai490952010@gmail.com',
    python_requires='>=3.6',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    description="OHLC data pipeline boilerplate",
    entry_points={
        'console_scripts': [
            'ohlc_data_pipeline=ohlc_data_pipeline.cli:main',
        ],
    },
    install_requires=requirements,
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='ohlc_data_pipeline',
    name='ohlc_data_pipeline',
    packages=find_packages(include=['ohlc_data_pipeline', 'ohlc_data_pipeline.*']),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/kai490952010/ohlc_data_pipeline',
    version='0.1.0',
    zip_safe=False,
)
