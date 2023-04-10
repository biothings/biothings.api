import logging
import mimetypes
import os
import tempfile
import warnings
from urllib.parse import quote

import boto3
import botocore.exceptions

try:
    from biothings import config
except ImportError:
    # assuming key, secret and bucket will be passed
    # to all functions
    pass


def key_exists(bucket, s3key, aws_key=None, aws_secret=None):
    client = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    try:
        client.head_object(Bucket=bucket, Key=s3key)
        return True
    except Exception as e:
        if e.response["Error"]["Code"] == "404":
            return False
        else:
            raise


def send_s3_file(
    localfile,
    s3key,
    overwrite=False,
    permissions=None,
    metadata=None,
    content=None,
    content_type=None,
    aws_key=None,
    aws_secret=None,
    s3_bucket=None,
    redirect=None,
):
    """save a localfile to s3 bucket with the given key.
    bucket is set via S3_BUCKET
    it also save localfile's lastmodified time in s3 file's metadata

    Args:
        redirect (str): if not None, set the redirect property
            of the object so it produces a 301 when accessed
    """
    metadata = metadata or {}
    try:
        aws_key = aws_key or config.AWS_SECRET
        aws_secret = aws_secret or config.AWS_SECRET
        s3_bucket = s3_bucket or config.S3_BUCKET
    except AttributeError:
        logging.info("Skip sending file to S3, missing information in config file: AWS_KEY, AWS_SECRET or S3_BUCKET")
        return
    s3 = boto3.resource("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    target_object = s3.Object(s3_bucket, s3key)
    if not overwrite:
        if key_exists(bucket=s3_bucket, s3key=s3key, aws_key=aws_key, aws_secret=aws_secret):
            # NOTE: change to assert/AssertionError if something relies on
            #  the assert statement
            raise FileExistsError('s3key "{}" already exists.'.format(s3key))
    # assuming metadata is a Mapping type
    put_request = {"Metadata": metadata}
    if redirect:
        put_request["WebsiteRedirectLocation"] = redirect
    if content_type:
        put_request["ContentType"] = content_type
    if content is not None:
        put_request["Body"] = content
    else:
        assert os.path.exists(localfile), 'localfile "{}" does not exist.'.format(localfile)
        lastmodified = os.stat(localfile)[-2]
        put_request["Body"] = open(localfile, "rb")
        put_request["Metadata"]["lastmodified"] = str(lastmodified)
    target_object.put(**put_request)

    if permissions:
        target_object.Acl().put(ACL=permissions)


def send_s3_big_file(
    localfile,
    s3key,
    overwrite=False,
    acl=None,
    aws_key=None,
    aws_secret=None,
    s3_bucket=None,
    storage_class=None,
):
    """
    Multiparts upload for file bigger than 5GiB
    """
    # TODO: maybe merge with send_s3_file() based in file size ? It would need boto3 migration
    try:
        aws_key = aws_key or config.AWS_SECRET
        aws_secret = aws_secret or config.AWS_SECRET
        s3_bucket = s3_bucket or config.S3_BUCKET
    except AttributeError:
        logging.info("Skip sending file to S3, missing information in config file: AWS_KEY, AWS_SECRET or S3_BUCKET")
        return
    client = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    if not overwrite and key_exists(s3_bucket, s3key, aws_key, aws_secret):
        raise Exception("Key '%s' already exist" % s3key)
    tfr_config = boto3.s3.transfer.TransferConfig(
        multipart_threshold=1024 * 25,
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True,
    )
    extra = {
        "ACL": acl or "private",
        "ContentType": mimetypes.MimeTypes().guess_type(localfile)[0] or "binary/octet-stream",
        "StorageClass": storage_class or "REDUCED_REDUNDANCY",
    }
    client.upload_file(Filename=localfile, Bucket=s3_bucket, Key=s3key, ExtraArgs=extra, Config=tfr_config)


def get_s3_file(s3key, localfile=None, return_what=False, aws_key=None, aws_secret=None, s3_bucket=None):
    # get_s3_file is planned to be deprecated in 0.11 and removed in 0.13
    warnings.warn(
        DeprecationWarning("get_s3_file is deprecated, use download_s3_file or get_s3_file_contents instead"),
        stacklevel=2,
    )

    if return_what == "content":
        return get_s3_file_contents(s3key, aws_key, aws_secret, s3_bucket)
    elif return_what == "key":
        warnings.warn(
            DeprecationWarning("get_s3_file: return_what=key is deprecated, use other ways instead"), stacklevel=2
        )
        try:
            # pylint:disable=import-outside-toplevel
            # this is so that only those who need return_what="key"
            # will depend on boto
            from boto import connect_s3

            # pylint:enable=import-outside-toplevel
            s3 = connect_s3(aws_key, aws_secret)
            bucket = s3.get_bucket(s3_bucket)
            k = bucket.get_key(s3key)
            return k
        except ImportError:
            raise RuntimeError("get_s3_file: return_what=key needs package boto to be installed")
    else:
        download_s3_file(s3key, localfile, aws_key, aws_secret, s3_bucket, overwrite=True)


def _populate_s3_info(aws_key, aws_secret, s3_bucket):
    aws_key = aws_key or getattr(config, "AWS_SECRET", None)
    aws_secret = aws_secret or getattr(config, "AWS_SECRET", None)
    s3_bucket = s3_bucket or getattr(config, "S3_BUCKET", None)
    return aws_key, aws_secret, s3_bucket


def _get_s3_object(aws_key, aws_secret, s3_bucket, s3key):
    aws_key, aws_secret, s3_bucket = _populate_s3_info(aws_key, aws_secret, s3_bucket)
    if not key_exists(s3_bucket, s3key, aws_key, aws_secret):
        raise FileNotFoundError(s3key)
    s3 = boto3.resource("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    target_object = s3.Object(s3_bucket, s3key)
    return target_object


# pylint:disable=too-many-arguments
# at the moment we do not intend to merge parameters (to sth. like S3Config)
def download_s3_file(s3key, localfile=None, aws_key=None, aws_secret=None, s3_bucket=None, overwrite=False):
    localfile = localfile or os.path.basename(s3key)
    if not overwrite and os.path.exists(localfile):
        raise FileExistsError(f"download_s3_file: {localfile} already exists and not overwriting")
    target_object = _get_s3_object(aws_key, aws_secret, s3_bucket, s3key)
    with tempfile.NamedTemporaryFile("xb", delete=False) as tmp:
        body = target_object.get()["Body"]
        for chunk in body.iter_chunks():
            tmp.write(chunk)
        if overwrite:
            os.replace(tmp.name, localfile)
        else:
            os.rename(tmp.name, localfile)


# pylint:enable=too-many-arguments
def get_s3_file_contents(s3key, aws_key=None, aws_secret=None, s3_bucket=None) -> bytes:
    target_object = _get_s3_object(aws_key, aws_secret, s3_bucket, s3key)
    return target_object.get()["Body"].read()


def get_s3_folder(s3folder, basedir=None, aws_key=None, aws_secret=None, s3_bucket=None):
    aws_key = aws_key or config.AWS_SECRET
    aws_secret = aws_secret or config.AWS_SECRET
    s3_bucket = s3_bucket or config.S3_BUCKET
    s3 = boto3.resource("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    bucket = s3.Bucket(s3_bucket)
    cwd = os.getcwd()
    try:
        if basedir:
            os.chdir(basedir)
        if not os.path.exists(s3folder):
            os.makedirs(s3folder)
        for k in bucket.objects.filter(Prefix=s3folder):
            download_s3_file(
                k.key, localfile=k.key, aws_key=aws_key, aws_secret=aws_secret, s3_bucket=s3_bucket, overwrite=True
            )
    finally:
        os.chdir(cwd)


def send_s3_folder(
    folder,
    s3basedir=None,
    acl=None,
    overwrite=False,
    aws_key=None,
    aws_secret=None,
    s3_bucket=None,
):
    aws_key = aws_key or config.AWS_SECRET
    aws_secret = aws_secret or config.AWS_SECRET
    s3_bucket = s3_bucket or config.S3_BUCKET
    s3 = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    s3.head_bucket(Bucket=s3_bucket)  # will raise when not 200
    cwd = os.getcwd()
    if not s3basedir:
        s3basedir = os.path.basename(cwd)
    for localf in [f for f in os.listdir(folder) if not f.startswith(".")]:
        fullpath = os.path.join(folder, localf)
        if os.path.isdir(fullpath):
            send_s3_folder(
                fullpath,
                os.path.join(s3basedir, localf),
                overwrite=overwrite,
                acl=acl,
                aws_key=aws_key,
                aws_secret=aws_secret,
                s3_bucket=s3_bucket,
            )
        else:
            send_s3_big_file(
                fullpath,
                os.path.join(s3basedir, localf),
                overwrite=overwrite,
                acl=acl,
                aws_key=aws_key,
                aws_secret=aws_secret,
                s3_bucket=s3_bucket,
            )


def get_s3_url(s3key, aws_key=None, aws_secret=None, s3_bucket=None):
    if key_exists(s3_bucket, s3key, aws_key, aws_secret):
        return f"https://{s3_bucket}.s3.amazonaws.com/{quote(s3key)}"
    return None


def get_s3_static_website_url(s3key, aws_key=None, aws_secret=None, s3_bucket=None):
    aws_key, aws_secret, s3_bucket = _populate_s3_info(aws_key, aws_secret, s3_bucket)
    s3 = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    location_resp = s3.get_bucket_location(Bucket=s3_bucket)
    region = location_resp.get("LocationConstraint", "us-east-1")
    return f"http://{s3_bucket}.s3-website.{region}.amazonaws.com/{quote(s3key)}"


def create_bucket(name, region=None, aws_key=None, aws_secret=None, acl=None, ignore_already_exists=False):
    """Create a S3 bucket "name" in optional "region". If aws_key and aws_secret
    are set, S3 client will these, otherwise it'll use default system-wide setting.
    "acl" defines permissions on the bucket: "private" (default), "public-read",
    "public-read-write" and "authenticated-read"
    """
    client = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    acl = acl or "private"
    kwargs = {"ACL": acl, "Bucket": name}
    if region:
        kwargs["CreateBucketConfiguration"] = {"LocationConstraint": region}
    try:
        client.create_bucket(**kwargs)
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "BucketAlreadyOwnedByYou" and not ignore_already_exists:
            raise


def set_static_website(name, aws_key=None, aws_secret=None, index="index.html", error="error.html"):
    client = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)
    conf = {"IndexDocument": {"Suffix": index}, "ErrorDocument": {"Key": error}}
    client.put_bucket_website(Bucket=name, WebsiteConfiguration=conf)
    location = client.get_bucket_location(Bucket=name)
    region = location["LocationConstraint"]
    # generate website URL
    return "http://%(name)s.s3-website-%(region)s.amazonaws.com" % {"name": name, "region": region}
