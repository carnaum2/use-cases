from google.cloud import storage
import yaml
import os


def get_bucket_prefix(file_path):

    bucket=file_path.split("/")[2]
    split=bucket+'/'
    prefix=file_path.partition(split)[2] #get path without bucket

    return bucket, prefix


def load_yml_from_gcp(file_path):
    # Bucket configs
    
    bucket, prefix= get_bucket_prefix(file_path)

    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(prefix)

    if "/" in prefix:
        file_name = prefix.split("/")[-1]

    # Check if file exists:
    print("{} exists: ".format(prefix), blob.exists())
    blob.download_to_filename(file_name)

    # Load with yaml:
    with open(file_name, "r") as ymlfile:
        configs = yaml.load(ymlfile)
    return configs




def check_file_exists_gcp(file_path):

    
    bucket, prefix= get_bucket_prefix(file_path)

    # Bucket configs
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(prefix)

    return blob.exists()


def check_folder_exists_gcp(file_path, delimiter = None):

    num_files_inside_folder = list_blobs_with_prefix(file_path, delimiter)

    if num_files_inside_folder == 0:
        return False
    else:
        return True




def list_blobs_with_prefix(file_path, delimiter=None):
    """Lists all the blobs in the bucket that begin with the prefix.
    This can be used to list all blobs in a "folder", e.g. "public/".
    The delimiter argument can be used to restrict the results to only the
    "files" in the given "folder". Without the delimiter, the entire tree under
    the prefix is returned. For example, given these blobs:
        a/1.txt
        a/b/2.txt
    If you just specify prefix = 'a', you'll get back:
        a/1.txt
        a/b/2.txt
    However, if you specify prefix='a' and delimiter='/', you'll get back:
        a/1.txt
    Additionally, the same request will return blobs.prefixes populated with:
        a/b/
    """

    # Bucket configs:

    bucket, prefix= get_bucket_prefix(file_path)

    client = storage.Client()
    bucket = client.get_bucket(bucket)

    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

    return len(list(blobs))


def upload_file_to_gcp(file_name, file_path):

    bucket, prefix= get_bucket_prefix(file_path)
    
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(prefix)
    blob.upload_from_filename(file_name)


