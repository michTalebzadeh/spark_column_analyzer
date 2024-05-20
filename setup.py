from setuptools import setup, find_packages

setup(
    name='spark-column-analyzer',
    version='0.2.1',
    packages=find_packages(where='src'),
    package_dir={'': 'src'},
    author='Mich Talebzadeh',
    author_email='mich.talebzadeh@gmail.com',
    description='A package for analyzing PySpark DataFrame columns',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    url='https://github.com/michTalebzadeh/spark_column_analyzer',
    install_requires=[
        'pyspark',
    ],
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
    ],
)
