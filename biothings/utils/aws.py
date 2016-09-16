import os, logging
from boto import connect_s3

try:
    from biothings import config
except ImportError:
    raise Exception("call biothings.config_for_app() first")

def send_s3_file(localfile, s3key, overwrite=False):
    '''save a localfile to s3 bucket with the given key.
       bucket is set via S3_BUCKET
       it also save localfile's lastmodified time in s3 file's metadata
    '''
    try:

        assert os.path.exists(localfile), 'localfile "{}" does not exist.'.format(localfile)
        s3 = connect_s3(config.AWS_KEY, config.AWS_SECRET)
        bucket = s3.get_bucket(config.S3_BUCKET)
        k = bucket.new_key(s3key)
        if not overwrite:
            assert not k.exists(), 's3key "{}" already exists.'.format(s3key)
        lastmodified = os.stat(localfile)[-2]
        k.set_metadata('lastmodified', lastmodified)
        k.set_contents_from_filename(localfile)
    except ImportError:
        logging.info("Skip sending file to S3, missing information in config file: AWS_KEY, AWS_SECRET or S3_BUCKET")
