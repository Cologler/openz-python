# -*- coding: utf-8 -*-
# 
# Copyright (c) 2023~2999 - Cologler <skyoflw@gmail.com>
# ----------
#
# ----------

from pathlib import Path
import os
import tempfile
import atexit

import pytest
import nanoid

from openz import open_for_write, try_rollback

tmpdir = tempfile.TemporaryDirectory(prefix='openz-tests-')
atexit.register(lambda: tmpdir.cleanup())
tmpdir_path = Path(tmpdir.name)

def _assert_lockfile(file_name: str, with_lockfile: bool):
    lockfile_path = tmpdir_path / (file_name + '.lock')
    assert lockfile_path.is_file() == with_lockfile

def _assert_no_lockfile(file_name: str):
    # lockfile wont remove if system is linux
    if os.name == 'nt':
        lockfile_path = tmpdir_path / (file_name + '.lock')
        assert not lockfile_path.is_file()


@pytest.mark.parametrize("text_mode", [True, False])
@pytest.mark.parametrize("with_atomicwrite", [True, False])
@pytest.mark.parametrize("with_lockfile", [True, False])
@pytest.mark.parametrize("with_exclusive", [True, False])
@pytest.mark.parametrize("with_backup", [True, False])
def test_write(
        text_mode: bool,
        with_atomicwrite: bool,
        with_lockfile: bool,
        with_exclusive: bool,
        with_backup: bool,
    ):

    should_raises_error = with_atomicwrite and with_exclusive

    file_name = f'{nanoid.generate()}.txt'
    file_path = tmpdir_path / file_name

    def content():
        return file_path.read_text() if text_mode else file_path.read_bytes()

    data_str = nanoid.generate(size=10)
    data = data_str if text_mode else data_str.encode('utf-8')

    for step in range(2):
        try:
            with open_for_write(file_path,
                    text_mode=text_mode,
                    overwrite=False,
                    with_atomicwrite=with_atomicwrite,
                    with_lockfile=with_lockfile,
                    with_exclusive=with_exclusive,
                    with_backup=with_backup) as fp:

                _assert_lockfile(file_name, with_lockfile)
                fp.write(data)

            assert not should_raises_error
            assert step == 0
            _assert_no_lockfile(file_name)
        except ValueError:
            assert should_raises_error
            return
        except FileExistsError:
            assert step > 0

        assert content() == data


@pytest.mark.parametrize("text_mode", [True, False])
@pytest.mark.parametrize("with_atomicwrite", [True, False])
@pytest.mark.parametrize("with_lockfile", [True, False])
@pytest.mark.parametrize("with_exclusive", [True, False])
@pytest.mark.parametrize("with_backup", [True, False])
def test_overwrite(
        text_mode: bool,
        with_atomicwrite: bool,
        with_lockfile: bool,
        with_exclusive: bool,
        with_backup: bool,
    ):

    should_raises_error = with_atomicwrite and with_exclusive

    file_name = f'{nanoid.generate()}.txt'
    file_path = tmpdir_path / file_name

    def content():
        return file_path.read_text() if text_mode else file_path.read_bytes()

    sizes = (10, 20, 5)
    data_strs = [nanoid.generate(size=s) for s in sizes]

    for data_str in data_strs:
        data = data_str if text_mode else data_str.encode('utf-8')

        try:
            with open_for_write(file_path,
                    text_mode=text_mode,
                    overwrite=True,
                    with_atomicwrite=with_atomicwrite,
                    with_lockfile=with_lockfile,
                    with_exclusive=with_exclusive,
                    with_backup=with_backup) as fp:

                _assert_lockfile(file_name, with_lockfile)
                fp.write(data)

            assert not should_raises_error
            _assert_no_lockfile(file_name)
        except ValueError:
            assert should_raises_error
            continue

        assert content() == data


@pytest.mark.parametrize("text_mode", [True, False])
@pytest.mark.parametrize("with_atomicwrite", [True, False])
@pytest.mark.parametrize("with_lockfile", [True, False])
@pytest.mark.parametrize("with_exclusive", [True, False])
@pytest.mark.parametrize("backup_for_fault", [True, False])
def test_backup(
        text_mode: bool,
        with_atomicwrite: bool,
        with_lockfile: bool,
        with_exclusive: bool,
        backup_for_fault: bool,
    ):

    should_raises_error = with_atomicwrite and with_exclusive

    file_name = f'{nanoid.generate()}.txt'
    file_path = tmpdir_path / file_name

    def content():
        return file_path.read_text() if text_mode else file_path.read_bytes()

    sizes = (10, 20, 5)
    data_strs = [nanoid.generate(size=s) for s in sizes]

    for data_str in data_strs:
        data = data_str if text_mode else data_str.encode('utf-8')

        try:
            with open_for_write(file_path,
                    text_mode=text_mode,
                    overwrite=True,
                    with_atomicwrite=with_atomicwrite,
                    with_lockfile=with_lockfile,
                    with_exclusive=with_exclusive,
                    with_backup=True,
                    backup_for_fault=backup_for_fault,
                    ) as fp:

                _assert_lockfile(file_name, with_lockfile)
                fp.write(data)

            assert not should_raises_error
            _assert_no_lockfile(file_name)
        except ValueError:
            assert should_raises_error
            continue

        assert content() == data

        if backup_for_fault:
            assert not try_rollback(file_path)
        else:
            assert len(data_str) in sizes

            if len(data_str) in sizes[1:]:
                assert try_rollback(file_path)
                assert content() == data_strs[0] if text_mode else data_strs[0].encode('utf-8')
