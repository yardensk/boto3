# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the 'License'). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
# http://aws.amazon.com/apache2.0/
#
# or in the 'license' file accompanying this file. This file is
# distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.
import os
import tempfile
import shutil
from tests import unittest

import mock

from boto3.s3.transfer import ReadFileChunk, StreamReaderProgress
from boto3.s3.transfer import S3Transfer
from boto3.s3.transfer import OSUtils
from botocore.vendored import six


class InMemoryOSLayer(OSUtils):
    def __init__(self, filemap):
        self._filemap = filemap

    def get_file_size(self, filename):
        return len(self._filemap[filename])

    def open_file_chunk_reader(self, filename, start_byte, size, callback):
        # TODO: handle the case for partial reads.
        return six.BytesIO(self._filemap[filename])

    def open(self, filename, mode):
        return open(filename, mode)


class TestReadFileChunk(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.tempdir)

    def test_read_entire_chunk(self):
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb')
        f.write(b'onetwothreefourfivesixseveneightnineten')
        f.flush()
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=0, chunk_size=3)
        self.assertEqual(chunk.read(), b'one')
        self.assertEqual(chunk.read(), b'')

    def test_read_with_amount_size(self):
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb')
        f.write(b'onetwothreefourfivesixseveneightnineten')
        f.flush()
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=11, chunk_size=4)
        self.assertEqual(chunk.read(1), b'f')
        self.assertEqual(chunk.read(1), b'o')
        self.assertEqual(chunk.read(1), b'u')
        self.assertEqual(chunk.read(1), b'r')
        self.assertEqual(chunk.read(1), b'')

    def test_reset_stream_emulation(self):
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb')
        f.write(b'onetwothreefourfivesixseveneightnineten')
        f.flush()
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=11, chunk_size=4)
        self.assertEqual(chunk.read(), b'four')
        chunk.seek(0)
        self.assertEqual(chunk.read(), b'four')

    def test_read_past_end_of_file(self):
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb')
        f.write(b'onetwothreefourfivesixseveneightnineten')
        f.flush()
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=36, chunk_size=100000)
        self.assertEqual(chunk.read(), b'ten')
        self.assertEqual(chunk.read(), b'')
        self.assertEqual(len(chunk), 3)

    def test_tell_and_seek(self):
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb')
        f.write(b'onetwothreefourfivesixseveneightnineten')
        f.flush()
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=36, chunk_size=100000)
        self.assertEqual(chunk.tell(), 0)
        self.assertEqual(chunk.read(), b'ten')
        self.assertEqual(chunk.tell(), 3)
        chunk.seek(0)
        self.assertEqual(chunk.tell(), 0)

    def test_callback_is_invoked_on_read(self):
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb')
        f.write(b'abc')
        f.flush()
        amounts_seen = []
        def callback(amount):
            amounts_seen.append(amount)
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=0, chunk_size=3, callback=callback)
        chunk.read(1)
        chunk.read(1)
        chunk.read(1)

        self.assertEqual(amounts_seen, [1, 1, 1])

    def test_file_chunk_supports_context_manager(self):
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb')
        f.write(b'abc')
        f.flush()
        with ReadFileChunk.from_filename(filename,
                                         start_byte=0,
                                         chunk_size=2) as chunk:
            val = chunk.read()
            self.assertEqual(val, b'ab')

    def test_iter_is_always_empty(self):
        # This tests the workaround for the httplib bug (see
        # the source for more info).
        filename = os.path.join(self.tempdir, 'foo')
        f = open(filename, 'wb').close()
        chunk = ReadFileChunk.from_filename(
            filename, start_byte=0, chunk_size=10)
        self.assertEqual(list(chunk), [])


class TestStreamReaderProgress(unittest.TestCase):

    def test_proxies_to_wrapped_stream(self):
        original_stream = six.StringIO('foobarbaz')
        wrapped = StreamReaderProgress(original_stream)
        self.assertEqual(wrapped.read(), 'foobarbaz')

    def test_callback_invoked(self):
        amounts_seen = []
        def callback(amount):
            amounts_seen.append(amount)
        original_stream = six.StringIO('foobarbaz')
        wrapped = StreamReaderProgress(original_stream, callback)
        self.assertEqual(wrapped.read(), 'foobarbaz')
        self.assertEqual(amounts_seen, [9])


class TestMultipartUploader(unittest.TestCase):
    def test_multipart_upload_uses_correct_client_calls(self):
        pass


class TestS3Transfer(unittest.TestCase):
    def setUp(self):
        self.client = mock.Mock()

    def test_upload_below_multipart_threshold_uses_put_object(self):
        below_multipart_threshold = 10
        fake_files = {
            'smallfile': b'foobar',
        }
        osutil = InMemoryOSLayer(fake_files)
        transfer = S3Transfer(self.client, osutil=osutil)
        transfer.upload_file('smallfile', 'bucket', 'key')
        self.client.put_object.assert_called_with(
            Bucket='bucket', Key='key', Body=mock.ANY
        )

    def test_extra_args_on_uploaded_passed_to_api_call(self):
        below_multipart_threshold = 10
        extra_args = {'ACL': 'public-read'}
        fake_files = {
            'smallfile': b'hello world'
        }
        osutil = InMemoryOSLayer(fake_files)
        transfer = S3Transfer(self.client, osutil=osutil)
        transfer.upload_file('smallfile', 'bucket', 'key',
                             extra_args=extra_args)
        self.client.put_object.assert_called_with(
            Bucket='bucket', Key='key', Body=mock.ANY,
            ACL='public-read'
        )

    def test_callback_integration(self):
        pass

    def test_download_below_multipart_threshold(self):
        # TODO: implement me
        return
        client = mock.Mock()
        transfer = S3Transfer(client)
        below_multipart_threshold = 10
        with mock.patch('os.path.getsize') as getsize:
            getsize.return_value = below_multipart_threshold
            transfer.download_file('bucket', 'key', '/tmp/smallfile')
