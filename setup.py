from setuptools import setup, find_packages

setup(
    name='spark-column-analyzer',
    version='0.2.3',
    description='A package for analyzing Spark DataFrame columns',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author='Mich Talebzadeh',
    author_email='mich.talebzadeh@gmail.com',
    url='https://github.com/michTalebzadeh/spark_column_analyzer',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'matplotlib',
        'numpy',
        'seaborn',
    ],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
