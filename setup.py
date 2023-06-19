from setuptools import setup

with open("README.md") as f:
    long_description = f.read()

# Get the current package version.
version_ns = {}
with open("version.py") as f:
    exec(f.read(), {}, version_ns)

setup(
    name="batchspawner",
    entry_points={
        "console_scripts": ["batchspawner-singleuser=batchspawner.singleuser:main"],
    },
    packages=["batchspawner"],
    version=version_ns["__version__"],
    description="""Batchspawner: A spawner for Jupyterhub to spawn notebooks using batch resource managers.""",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Michael Milligan, Andrea Zonca, Mike Gilbert",
    author_email="milligan@umn.edu",
    url="http://jupyter.org",
    license="BSD",
    platforms="Linux, Mac OS X",
    keywords=["Interactive", "Interpreter", "Shell", "Web", "Jupyter"],
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: BSD License",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
    ],
    project_urls={
        "Bug Reports": "https://github.com/jupyterhub/batchspawner/issues",
        "Source": "https://github.com/jupyterhub/batchspawner/",
        "About Jupyterhub": "http://jupyterhub.readthedocs.io/en/latest/",
        "Jupyter Project": "http://jupyter.org",
    },
    python_requires=">=3.7",
    install_require={
        "async_generator>=1.8",
        "jinja2",
        "jupyterhub>=1.5.1",
    },
    extras_require={
        "test": [
            "pytest",
            "pytest-cov",
        ],
    },
)
