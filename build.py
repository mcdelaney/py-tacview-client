import os

from distutils.core import Extension
import numpy

try:
    from Cython.Build import cythonize
except ImportError:
    def build(setup_kwargs):
        pass

else:
    from setuptools import Extension
    from setuptools.dist import Distribution
    from distutils.command.build_ext import build_ext

    def build(setup_kwargs):
        # The file you want to compile
        extensions = [
            "tacview_client/cython_funs.pyx"
        ]

        # gcc arguments hack: enable optimizations
        os.environ['CFLAGS'] = '-O3'

        # Build
        setup_kwargs.update({
            'ext_modules': cythonize(
                extensions,
                language_level=3,
                compiler_directives={'linetrace': True},
            ),
            'include_dirs': [numpy.get_include()],
            'cmdclass': {'build_ext': build_ext}
        })