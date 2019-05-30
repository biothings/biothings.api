import os
import logging
from urllib.parse import urlparse

from boto import connect_s3

try:
    from biothings import config
except ImportError:
    # assuming key, secret and bucket will be passed
    # to all functions
    pass



def send_s3_file(localfile, s3key, overwrite=False, permissions=None, metadata=None,
                 content=None, content_type=None, aws_key=None, aws_secret=None, s3_bucket=None):
    '''save a localfile to s3 bucket with the given key.
       bucket is set via S3_BUCKET
       it also save localfile's lastmodified time in s3 file's metadata
    '''
    metadata = metadata or {}
    try:
        aws_key = aws_key or getattr(config, "AWS_SECRET")
        aws_secret = aws_secret or getattr(config, "AWS_SECRET")
        s3_bucket = s3_bucket or getattr(config, "S3_BUCKET")
    except AttributeError:
        logging.info("Skip sending file to S3, missing information in config file: AWS_KEY, AWS_SECRET or S3_BUCKET")
        return
    s3 = connect_s3(aws_key, aws_secret)
    bucket = s3.get_bucket(s3_bucket)
    k = bucket.new_key(s3key)
    if not overwrite:
        assert not k.exists(), 's3key "{}" already exists.'.format(s3key)
    for header in metadata:
        k.set_metadata(header, metadata[header])
    if content_type:
        k.content_type = content_type
    if content:
        k.set_contents_from_string(content)
    else:
        assert os.path.exists(localfile), 'localfile "{}" does not exist.'.format(localfile)
        lastmodified = os.stat(localfile)[-2]
        k.set_metadata('lastmodified', lastmodified)
        k.set_contents_from_filename(localfile)
    if permissions:
        k.set_acl(permissions)


def get_s3_file(s3key, localfile=None, return_what=False,
                aws_key=None, aws_secret=None, s3_bucket=None):
    aws_key = aws_key or getattr(config, "AWS_SECRET")
    aws_secret = aws_secret or getattr(config, "AWS_SECRET")
    s3_bucket = s3_bucket or getattr(config, "S3_BUCKET")
    localfile = localfile or os.path.basename(s3key)
    s3 = connect_s3(aws_key, aws_secret)
    bucket = s3.get_bucket(s3_bucket)
    k = bucket.get_key(s3key)
    if not k:
        raise FileNotFoundError(s3key)
    if return_what == "content":
        return k.get_contents_as_string()
    elif return_what == "key":
        return k
    else:
        k.get_contents_to_filename(localfile)


def get_s3_folder(s3folder, basedir=None, aws_key=None, aws_secret=None, s3_bucket=None):
    aws_key = aws_key or getattr(config, "AWS_SECRET")
    aws_secret = aws_secret or getattr(config, "AWS_SECRET")
    s3_bucket = s3_bucket or getattr(config, "S3_BUCKET")
    s3 = connect_s3(aws_key, aws_secret)
    bucket = s3.get_bucket(s3_bucket)
    cwd = os.getcwd()
    try:
        if basedir:
            os.chdir(basedir)
        if not os.path.exists(s3folder):
            os.makedirs(s3folder)
        for k in bucket.list(s3folder):
            get_s3_file(k.key, localfile=k.key, aws_key=aws_key, aws_secret=aws_secret, s3_bucket=s3_bucket)
    finally:
        os.chdir(cwd)


def send_s3_folder(folder, s3basedir=None, permissions=None, overwrite=False,
                   aws_key=None, aws_secret=None, s3_bucket=None):
    aws_key = aws_key or getattr(config, "AWS_SECRET")
    aws_secret = aws_secret or getattr(config, "AWS_SECRET")
    s3_bucket = s3_bucket or getattr(config, "S3_BUCKET")
    s3 = connect_s3(aws_key, aws_secret)
    s3.get_bucket(s3_bucket)    # check if s3_bucket exists
    cwd = os.getcwd()
    if not s3basedir:
        s3basedir = os.path.basename(cwd)
    for localf in [f for f in os.listdir(folder) if not f.startswith(".")]:
        send_s3_file(os.path.join(folder, localf), os.path.join(s3basedir, localf),
                     overwrite=overwrite, permissions=permissions,
                     aws_key=aws_key, aws_secret=aws_secret, s3_bucket=s3_bucket)


def get_s3_url(s3key, aws_key=None, aws_secret=None, s3_bucket=None):
    try:
        k = get_s3_file(s3key, return_what="key",
                        aws_key=aws_key, aws_secret=aws_secret, s3_bucket=s3_bucket)
    except FileNotFoundError:
        return None
    # generate_url will include some acdesskey, signature, etc... we want to remove this
    # as the bucket is public anyway and want "clean" url
    url = k.generate_url(expires_in=0) # never (and whatever, we
    return urlparse(url)._replace(query="").geturl()
