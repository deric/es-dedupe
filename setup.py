# Always prefer setuptools over distutils
from setuptools import setup, find_packages
import pathlib

import esdedupe

here = pathlib.Path(__file__).parent.resolve()

# Get the long description from the README file
long_description = (here / 'README.md').read_text(encoding='utf-8')


setup(
    author='Tomas Barton',
    author_email='barton.tomas@gmail.com',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    description="A tool for duplicate removal from Elasticsearch",
    entry_points={
        'console_scripts': 'esdedupe=esdedupe:main',
    },
    install_requires=[
        'ujson',
        'tqdm',
        'psutil',
        'elasticsearch>5.0'
    ],
    license='Apache License 2.0',
    keywords='elasticsearch',
    long_description=long_description,
    long_description_content_type='text/markdown',
    name='esdedupe',
    packages=find_packages(),
    url='https://github.com/deric/es-dedupe',
    version=esdedupe.__VERSION__,
)