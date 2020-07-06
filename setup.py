import setuptools

setuptools.setup(
    name='pyenv',
    version='0.0.1',
    package_dir={'': 'src/main/python'},
    packages=['pyenv'],
    install_requires=[
        'ipython==7.13.0',
    ],
    python_requires='>=3.7',
    classifiers=[
        'Framework :: IPython',
        'Framework :: Jupyter',
    ],
    include_package_data=True,
    zip_safe=False
)
