import setuptools

VERSION = "1.0.0"
PACKAGE_NAME = "pygeoapi_ingestor_plugin"
AUTHOR = "Valerio Luzzi, Marco Renzi, EHJ, JSL"
EMAIL = "valerio.luzzi@gecosistema.com, marco.renzi@gecosistema.com"
GITHUB = "https://github.com/icisk/pygeoapi_ingestor"
DESCRIPTION = "An utils functions package"

setuptools.setup(
    name=PACKAGE_NAME,
    version=VERSION,
    license='MIT',
    author=AUTHOR,
    author_email=EMAIL,
    description=DESCRIPTION,
    long_description=DESCRIPTION,
    url=GITHUB,
    packages=setuptools.find_packages("src"),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    install_requires=[
        #
        #   sync with requirements.txt
        #
        "cdsapi",
        "filelock",
        "Flask-Cors==5.0.0",
        "geojson",
        "geopandas",
        "h5netcdf",
        "netcdf4",
        "numpy==1.24.0",
        "pygeoapi",
        "python-dotenv",
        "s3fs",
        "shyaml",
        "tifffile",
        "xarray==2024.1.0",
        "zarr"
    ]
)
