import setuptools

VERSION = "1.0.0"
PACKAGE_NAME = "pygeoapi_ingestor_plugin"
AUTHOR = "Valerio Luzzi, Marco Renzi, JSL, EHJ"
EMAIL = "valerio.luzzi@gecosistema.com, marco.renzi@gecosistema.com, j.schnell@52north.org, e.h.juerrens@52north.org"
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
        "Flask-Cors==5.0.0",
        "cdsapi",
        "dask==2024.11.2",
        "filelock",
        "geojson",
        "geopandas",
        "h5netcdf",
        "netcdf4",
        "numpy==1.24.0",
        "openpyxl",
        "pygeoapi",
        "pygrib",
        "python-dotenv",
        "rasterio",
        "rioxarray==0.18.2",
        "s3fs",
        "scipy",
        "shyaml",
        "tifffile",
        "xarray==2024.7.0",
        "zarr",
    ]
)
