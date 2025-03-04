import os
import hashlib
from logging import getLogger
from tempfile import tempdir

import boto3
from botocore.exceptions import ClientError, NoCredentialsError

import pygeoapi_ingestor_plugin.utils as utils

Logger = getLogger(__name__)



def iss3(filename):
    """
    iss3
    """
    return filename and isinstance(filename, str) and \
        (filename.startswith("s3:/") or filename.startswith("/vsis3/"))


def etag(filename, client=None, chunk_size=8 * 1024 * 1024):
    """
    calculates a multipart upload etag for amazon s3
    Arguments:
    filename   -- The file to calculate the etag for
    """
    if filename and os.path.isfile(filename):
        md5 = []
        with open(filename, 'rb') as fp:
            while True:
                data = fp.read(chunk_size)
                if not data:
                    break
                md5.append(hashlib.md5(data))
        if len(md5) == 1:
            return f"{md5[0].hexdigest()}"
        digests = b''.join(m.digest() for m in md5)
        digests_md5 = hashlib.md5(digests)
        return f"{digests_md5.hexdigest()}-{len(md5)}"

    elif filename and iss3(filename):
        uri = filename
        ETag = ""
        try:
            bucket_name, key_name = get_bucket_name_key(uri)
            if bucket_name and key_name:
                client = get_client(client)
                ETag = client.head_object(Bucket=bucket_name, Key=key_name)[
                    'ETag'][1:-1]
        except ClientError as ex:
            Logger.debug(f"ETAG:{ex}")
            ETag = ""
        except NoCredentialsError as ex:
            Logger.error(ex)
            ETag = ""
        return ETag
    else:
        return ""


def s3_equals(file1, file2, client=None):
    """
    s3_equals - check if s3 object is equals to local file
    """
    etag1 = etag(file1, client)
    etag2 = etag(file2, client)
    if etag1 and etag2:
        return etag1 == etag2
    return False


def tempname4S3(uri):
    """
    tempname4S3
    """
    dest_folder = tempdir("s3")
    uri = uri if uri else ""
    if uri.startswith("s3://"):
        tmp = uri.replace("s3://", dest_folder + "/")
    if uri.startswith("s3:/"):
        tmp = uri.replace("s3:/", dest_folder + "/")
    elif uri.startswith("/vsis3/"):
        tmp = uri.replace("/vsis3/", dest_folder + "/")
    else:
        _, path = os.path.splitdrive(uri)
        tmp = utils.normpath(dest_folder + "/" + path)
    
    os.makedirs(utils.justpath(tmp), exist_ok=True)
    return tmp


def get_client(client=None):
    """
    get_client
    """
    return client if client else boto3.client('s3', region_name='us-east-1')


def get_bucket_name_key(uri):
    """
    get_bucket_name_key - get bucket name and key name from uri
    """
    bucket_name, key_name = None, None
    if not uri:
        pass
    elif uri.startswith("s3://"):
        # s3://saferplaces.co/tests/rimini/dem.tif
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("s3:/"):
        # s3:/saferplaces.co/tests/rimini/dem.tif
        _, bucket_name, key_name = uri.split("/", 2)
    elif uri.startswith("/vsis3/"):
        # /vsis3/saferplaces.co/tests/rimini/dem.tif
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("https://s3.amazonaws.com/"):
        _, _, bucket_name, key_name = uri.split("/", 3)
    elif uri.startswith("https://s3."):
        _, _, bucket_name, key_name = uri.split("/", 3)
    else:
        bucket_name, key_name = None, uri
    return bucket_name, key_name


def s3_download(uri, fileout=None, remove_src=False, client=None):
    """
    Download a file from an S3 bucket
    """
    bucket_name, key = get_bucket_name_key(uri)
    if bucket_name:
        try:
            # check the cache
            client = get_client(client)

            if key and not key.endswith("/"):

                if not fileout:
                    fileout = tempname4S3(uri)

                if os.path.isdir(fileout):
                    fileout = f"{fileout}/{utils.justfname(key)}"

                if os.path.isfile(fileout) and s3_equals(uri, fileout, client):
                    Logger.debug(f"using cached file {fileout}")
                else:
                    # Download the file
                    Logger.debug(f"downloading {uri} into {fileout}...")
                    os.makedirs(utils.justpath(fileout), exist_ok=True)
                    client.download_file(
                        Filename=fileout, Bucket=bucket_name, Key=key)
                    if remove_src:
                        client.delete_object(Bucket=bucket_name, Key=key)
            else:
                objects = client.list_objects_v2(
                    Bucket=bucket_name, Prefix=key)['Contents']
                for obj in objects:
                    pathname = obj['Key']
                    if not pathname.endswith("/"):
                        dst = fileout
                        pathname = pathname.replace(key, "")
                        s3_download(f"{uri.rstrip('/')}/{pathname}",
                                    f"{dst}/{pathname}", client)

        except ClientError as ex:
            Logger.error(ex)
            return None
        except NoCredentialsError as ex:
            Logger.error(ex)
            return None

    return fileout if os.path.isfile(fileout) else None


def s3_upload(filename, uri, remove_src=False, client=None):
    """
    Upload a file to an S3 bucket
    Examples: s3_upload(filename, "s3://saferplaces.co/a/rimini/lidar_rimini_building_2.tif")
    """

    # Upload the file
    try:
        bucket_name, key = get_bucket_name_key(uri)
        if bucket_name and key and filename and os.path.isfile(filename):
            client = get_client(client)
            if s3_equals(uri, filename, client):
                print(f"file {filename} already uploaded")
            else:
                print(f"uploading {filename} into {bucket_name}/{key}...")

                extra_args = {}

                client.upload_file(Filename=filename,
                                   Bucket=bucket_name, Key=key,
                                   ExtraArgs=extra_args)

            if remove_src:
                print(f"removing {filename}")
                os.unlink(filename)  # unlink and not ogr_remove!!!
            return True

    except ClientError as ex:
        print(ex)
    except NoCredentialsError as ex:
        print(ex)

    return False