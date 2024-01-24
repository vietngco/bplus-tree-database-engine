import os
from unittest import mock

import pytest

filename = '/tmp/bplustree-testfile.index'
schema_name = 'test_schema'
tree_schema_filename = "/tmp/" + schema_name + ".db",

@pytest.fixture(autouse=True)
def clean_file():
    if os.path.isfile(filename):
        os.unlink(filename)
    if os.path.isfile(filename + '-wal'):
        os.unlink(filename + '-wal')
    yield # return/resume point 
    if os.path.isfile(filename):
        os.unlink(filename)
    if os.path.isfile(filename + '-wal'):
        os.unlink(filename + '-wal')

tree_schema_filename = "/tmp/" + schema_name + ".db"  # Remove the extra comma at the end

@pytest.fixture(autouse=True)
def clean_file_schema():
    if os.path.isfile(tree_schema_filename):
        os.unlink(tree_schema_filename)
    if os.path.isfile(tree_schema_filename + '-wal'):
        os.unlink(tree_schema_filename + '-wal')
    yield  # return/resume point 
    if os.path.isfile(tree_schema_filename):
        os.unlink(tree_schema_filename)
    if os.path.isfile(tree_schema_filename + '-wal'):
        os.unlink(tree_schema_filename + '-wal')


@pytest.fixture(autouse=True)
def patch_fsync():
    mock_fsync = mock.patch('os.fsync')
    mock_fsync.start()
    yield
    mock_fsync.stop()
