import os

import boto3
import pytest

from biothings.utils.aws import *


class TestAWS:
    access_key = "AWS_ACCESS_ID"
    secret_key = "AWS_SECRET_KEY"
    b_name = "TEST_BUCKET_NAME"

    contents = os.urandom(10 * 1024 + 17)  # 10KB and something

    def send_s3_file(self, *args, **kwargs):
        kwargs.update(
            {
                "aws_key": self.access_key,
                "aws_secret": self.secret_key,
                "s3_bucket": self.b_name,
            }
        )
        return send_s3_file(*args, **kwargs)

    @pytest.fixture(scope="class", autouse=True)
    def _rm_objects(self):
        s3 = boto3.resource("s3", aws_access_key_id=self.access_key, aws_secret_access_key=self.secret_key)
        del_dict = {"Objects": []}
        for fn in [
            "test.txt",
            "test-overwrite",
            "test-aws2.txt",
            "test.json",
            "test-perm.txt",
            r"un#safe\name?.txt",
            "test-content.bin",
            "test-dl.bin",
        ]:
            del_dict["Objects"].append({"Key": fn})
        s3.Bucket(self.b_name).delete_objects(Delete=del_dict)
        yield  # cleanup
        s3.Bucket(self.b_name).delete_objects(Delete=del_dict)

    @pytest.fixture(scope="class", autouse=True)
    def _create_dir(self):
        self.send_s3_file(None, "test-dir/test1.txt", overwrite=True, content="1")
        self.send_s3_file(None, "test-dir/test2.txt", overwrite=True, content="2")

    def test_overwrite(self):
        self.send_s3_file(None, "test-overwrite", content="c")
        self.send_s3_file(None, "test-overwrite", overwrite=True, content="c2")
        with pytest.raises(FileExistsError):
            self.send_s3_file(None, "test-overwrite", content="c3")

    def test_send_normal(self):
        self.send_s3_file("test-aws2.txt", "test-aws2.txt")

    def test_send_string(self):
        self.send_s3_file(None, "test.txt", content="test-text")

    def test_content_type(self):
        self.send_s3_file(None, "test.json", content="{}", content_type="application/json")

    def test_permission(self):
        self.send_s3_file(None, "test-perm.txt", content="open", permissions="public-read")

    def test_get_folder(self):
        get_s3_folder("test-dir/", aws_key=self.access_key, aws_secret=self.secret_key, s3_bucket=self.b_name)

    def test_get_url(self):
        url = get_s3_url(
            "test-dir/test2.txt", aws_key=self.access_key, aws_secret=self.secret_key, s3_bucket=self.b_name
        )
        assert url == f"https://{self.b_name}.s3.amazonaws.com/test-dir/test2.txt"

    def test_url_404(self):
        url = get_s3_url("not-exist", aws_key=self.access_key, aws_secret=self.secret_key, s3_bucket=self.b_name)
        assert url is None

    def test_unsafe_key_url(self):
        self.send_s3_file(None, r"un#safe\name?.txt", content="bad_name", permissions="public-read")
        url = get_s3_url(
            r"un#safe\name?.txt", aws_key=self.access_key, aws_secret=self.secret_key, s3_bucket=self.b_name
        )
        assert url == f"https://{self.b_name}.s3.amazonaws.com/un%23safe%5Cname%3F.txt"

    def test_get_contents(self):
        contents = os.urandom(10)
        k = "test-content.bin"
        self.send_s3_file(None, k, content=contents)
        recv = get_s3_file_contents(k, self.access_key, self.secret_key, self.b_name)
        assert recv == contents

    def test_dl(self):
        k = "test-dl.bin"
        self.send_s3_file(None, k, content=self.contents)
        download_s3_file(k, k, self.access_key, self.secret_key, self.b_name)
        with open(k, "rb") as f:
            assert f.read() == self.contents
        os.remove(k)

    def test_dl_raise(self):
        open("test-dl.bin", "xb").close()
        with pytest.raises(OSError):
            download_s3_file("test-dl.bin", "test-dl.bin", self.access_key, self.secret_key, self.b_name)
        os.remove("test-dl.bin")

    def test_dl_overwrite(self):
        k = "test-dl.bin"
        open(k, "xb").close()
        download_s3_file(k, k, self.access_key, self.secret_key, self.b_name, overwrite=True)
        with open(k, "rb") as f:
            assert f.read() == self.contents
        os.remove(k)
