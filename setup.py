import setuptools
from Cython.Build import cythonize

import Cython.Compiler.Options
Cython.Compiler.Options.annotate = True

# compile_files = ["src/main/python/ipystate/impl/cython_walker.pyx"]
# ext_module = Extension("extension", compile_files,
#                        language='c++',
#                        extra_link_args=["-stdlib=libc++"])

setuptools.setup(
    name='ipystate',
    version='0.0.1',
    package_dir={'': 'src/main/python'},
    packages=['ipystate', 'ipystate/impl', 'ipystate/impl/dispatch'],
    install_requires=[
        'ipython>=7.13.0,<=7.19.0',
        'cloudpickle>=1.6.0,<=1.6.0',
        'pyarrow>=0.17.1,<=2.0.0',
        'pybase64>=1.0.0,<=1.0.2',
        'cython==0.29.5',
        'pympler==0.9',
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
    ext_modules=cythonize(["src/main/python/ipystate/impl/cython_walker.pyx"], annotate=True),
    zip_safe=False,
)
