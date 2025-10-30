from setuptools import setup, find_packages

setup(
    name="svd",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "urllib3",
    ],
    py_modules=['svd'],
    entry_points={
        'console_scripts':[
            'svd=svd:main',
            ]
        }
)
