import io
import os
from unittest import mock

import pytest

from bplustree.node import LeafNode
from bplustree.memory import (
    Memory, FileMemory, open_file_in_dir, WAL, ReachedEndOfFile
)
from bplustree.const import TreeConf
from .conftest import filename
from bplustree.serializer import IntSerializer

tree_conf = TreeConf(4096, 4, 16, 16, IntSerializer())
node = LeafNode(tree_conf, page=3)


def test_memory_node():
    mem = Memory()

    with pytest.raises(ValueError):
        mem.get_node(3)

    mem.set_node(node)
    assert mem.get_node(3) == node

    mem.close()


def test_memory_metadata():
    mem = Memory()
    with pytest.raises(ValueError):
        mem.get_metadata()
    mem.set_metadata(6, tree_conf)
    assert mem.get_metadata() == (6, tree_conf)


def test_memory_next_available_page():
    mem = Memory()
    for i in range(1, 100):
        assert mem.next_available_page == i


def test_file_memory_node():
    mem = FileMemory(filename, tree_conf)

    with pytest.raises(ReachedEndOfFile):
        mem.get_node(3)

    mem.set_node(node)
    assert node == mem.get_node(3)

    mem.close()


def test_file_memory_metadata(clean_file):
    mem = FileMemory(filename, tree_conf)
    with pytest.raises(ValueError):
        mem.get_metadata()
    mem.set_metadata(6, tree_conf)
    assert mem.get_metadata() == (6, tree_conf)


def test_file_memory_next_available_page(clean_file):
    mem = FileMemory(filename, tree_conf)
    for i in range(1, 100):
        assert mem.next_available_page == i


def test_open_file_in_dir(clean_file):
    with pytest.raises(ValueError):
        open_file_in_dir('/foo/bar/does/not/exist')

    # Create file and re-open
    for _ in range(2):
        file_fd, dir_fd = open_file_in_dir(filename)
        assert isinstance(file_fd, io.FileIO)
        assert isinstance(dir_fd, int)
        file_fd.close()
        os.close(dir_fd)


def test_wal_create_reopen_empty():
    WAL(filename, 64)

    wal = WAL(filename, 64)
    assert wal._page_size == 64


def test_wal_create_reopen_uncommitted():
    wal = WAL(filename, 64)
    wal.set_page(1, b'1' * 64)
    wal.commit()
    wal.set_page(2, b'2' * 64)
    assert wal.get_page(1) == b'1' * 64
    assert wal.get_page(2) == b'2' * 64

    wal = WAL(filename, 64)
    assert wal.get_page(1) == b'1' * 64
    assert wal.get_page(2) is None


def test_wal_rollback():
    wal = WAL(filename, 64)
    wal.set_page(1, b'1' * 64)
    wal.commit()
    wal.set_page(2, b'2' * 64)
    assert wal.get_page(1) == b'1' * 64
    assert wal.get_page(2) == b'2' * 64

    wal.rollback()
    assert wal.get_page(1) == b'1' * 64
    assert wal.get_page(2) is None


def test_wal_checkpoint():
    wal = WAL(filename, 64)
    wal.set_page(1, b'1' * 64)
    wal.commit()
    wal.set_page(2, b'2' * 64)

    rv = wal.checkpoint()
    assert list(rv) == [(1, b'1' * 64)]

    with pytest.raises(ValueError):
        wal.set_page(3, b'3' * 64)

    assert os.path.isfile(filename + '-wal') is False
