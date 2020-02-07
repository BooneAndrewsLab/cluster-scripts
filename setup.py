import io
import os
import sys

from setuptools import setup

ROOT = os.path.dirname(__file__)


# noinspection PyUnresolvedReferences,PyPackageRequirements
def get_version():
    sys.path.insert(0, "cluster")
    import version
    return version.__version__


with io.open(os.path.join(ROOT, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name="ccbr-cluster-scripts",
    packages=["cluster"],
    version=get_version(),
    description='User scripts for CCBR cluster',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Matej Usaj',
    author_email='m.usaj@utoronto.ca',
    url='https://github.com/BooneAndrewsLab/cluster-scripts',
    download_url='https://github.com/BooneAndrewsLab/cluster-scripts/archive/master.zip',

    classifiers=[
        'Development Status :: 3 - Beta',

        'Intended Audience :: Science/Research',
        'Topic :: Scientific/Engineering :: Bio-Informatics',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],

    keywords='hpc cluster',

    entry_points={
        'console_scripts': [
            'deleteallmyjobs=cluster.deleteallmyjobs:main',
            'nodes=cluster.nodes:main',
            'submitjob=cluster.submitjob:main'
        ]
    },
)
