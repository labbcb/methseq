from distutils.core import setup

from setuptools import find_packages

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='methseq',
    version='1.0.1',
    packages=find_packages(),
    url='https://github.com/labbcb/methseq',
    license='GPLv3',
    author='Welliton de Souza, Danielle Brunno, Jaqueline Geraldis',
    author_email='well309@gmail.com',
    description='Automation tool for high-throughput sequencing DNA methylation data',
    long_description=long_description,
    long_description_content_type="text/markdown",
    requires=['click', 'requests'],
    include_package_data=True,
    entry_points='''
        [console_scripts]
        methseq=methseq.scripts.methseq:cli
    ''',
)
