import setuptools

setuptools.setup(
    name='ipystate',
    version='0.0.1',
    package_dir={'': 'src/main/python'},
    packages=['ipystate', 'ipystate/impl', 'ipystate/impl/dispatch'],
    install_requires=[
        'ipython==7.13.0',
        'cloudpickle==1.6.0',
        'pyarrow>=0.17.1',
        'pybase64==1.0.0',
    ],
    tests_require=[
        'numpy',
    ],
    python_requires='>=3.7',
    classifiers=[
        'Framework :: IPython',
        'Framework :: Jupyter',
    ],
    include_package_data=True,
    zip_safe=False,
)