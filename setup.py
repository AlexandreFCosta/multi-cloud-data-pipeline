"""
Setup configuration for Multi-Cloud Data Pipeline Framework
"""

from setuptools import setup, find_packages
import os

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
with open(os.path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

# Read requirements
with open('requirements.txt') as f:
    requirements = [line.strip() for line in f if line.strip() and not line.startswith('#')]

setup(
    name='multi-cloud-data-pipeline',
    version='1.0.0',
    author='Alexandre',
    author_email='yalexandrefcosta.dev@gmail.com',
    description='A production-ready, cloud-agnostic data pipeline framework for Azure and GCP',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url='https://github.com/AlexandreFCosta/multi-cloud-data-pipeline',
    project_urls={
        'Bug Reports': 'https://github.com/AlexandreFCosta/multi-cloud-data-pipeline/issues',
        'Source': 'https://github.com/AlexandreFCosta/multi-cloud-data-pipeline',
        'Documentation': 'https://multi-cloud-data-pipeline.readthedocs.io/',
    },
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: Database',
        'Topic :: System :: Distributed Computing',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
        'Operating System :: OS Independent',
    ],
    python_requires='>=3.8',
    install_requires=requirements,
    extras_require={
        'dev': [
            'pytest>=7.4.3',
            'pytest-cov>=4.1.0',
            'pytest-mock>=3.12.0',
            'black>=23.12.1',
            'flake8>=7.0.0',
            'pylint>=3.0.3',
            'mypy>=1.8.0',
        ],
        'docs': [
            'sphinx>=7.2.6',
            'sphinx-rtd-theme>=2.0.0',
            'sphinxcontrib-napoleon>=0.7',
        ],
    },
    entry_points={
        'console_scripts': [
            'multicloud-pipeline=multicloud_pipeline.cli:main',
        ],
    },
    keywords='data-engineering azure gcp pyspark databricks bigquery etl data-pipeline cloud spark',
    include_package_data=True,
    zip_safe=False,
)
