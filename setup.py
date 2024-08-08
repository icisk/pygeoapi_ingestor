import setuptools

VERSION = "0.1.0"
PACKAGE_NAME = "pygeoapi_ingestor_plugin"
AUTHOR = "Valerio Luzzi, Marco Renzi"
EMAIL = "valerio.luzzi@gecosistema.com, marco.renzi@gecosistema.com"
GITHUB = "https://github.com/SaferPlaces2023/pygeoapi_ingestor_plugin"
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
        "pygeoapi",
        "cdsapi",
        "h5netcdf",
        "netcdf4",
        "python-dotenv",
        "shyaml",
        "geojson",
        "geopandas",
        "numpy==1.24.0",
        "xarray==2024.1.0",
        "tifffile",
        "s3fs",
        "zarr"]
)
