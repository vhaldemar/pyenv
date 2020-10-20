import setuptools

setuptools.setup(
    name='ipystate',
    version='0.0.1',
    package_dir={'': 'src/main/python'},
    packages=['ipystate', 'ipystate/impl'],
    install_requires=[
        'ipython==7.13.0',
        'cloudpickle==1.2.2',
        'numpy==1.19.1',
        'pandas==0.25.3'
    ],
    python_requires='>=3.7',
    classifiers=[
        'Framework :: IPython',
        'Framework :: Jupyter',
    ],
    include_package_data=True,
    zip_safe=False
)
