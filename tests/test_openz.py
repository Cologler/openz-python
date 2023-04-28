# -*- coding: utf-8 -*-
# 
# Copyright (c) 2023~2999 - Cologler <skyoflw@gmail.com>
# ----------
#
# ----------

from pathlib import Path
import tempfile
import atexit

import pytest
import nanoid

from openz import open_for_write, try_rollback

tmpdir = tempfile.TemporaryDirectory(prefix='openz-tests-')
atexit.register(lambda: tmpdir.cleanup())
tmpdir_path = Path(tmpdir.name)

@pytest.mark.parametrize("text_mode", [True, False])
@pytest.mark.parametrize("atomicwrite", [True, False])
@pytest.mark.parametrize("lockfile", [True, False])
@pytest.mark.parametrize("exclusive", [True, False])
@pytest.mark.parametrize("backup", [True, False])
def test_write(
        text_mode: bool,
        atomicwrite: bool,
        lockfile: bool,
        exclusive: bool,
        backup: bool,
    ):

    should_raises_error = atomicwrite and exclusive

    f = tmpdir_path / f'{nanoid.generate()}.txt'

    def content():
        return f.read_text() if text_mode else f.read_bytes()

    data_str = nanoid.generate(size=10)
    data = data_str if text_mode else data_str.encode('utf-8')

    for step in range(2):
        try:
            with open_for_write(f,
                    text_mode=text_mode,
                    overwrite=False,
                    with_atomicwrite=atomicwrite,
                    with_lockfile=lockfile,
                    with_exclusive=exclusive,
                    with_backup=backup) as fp:
                fp.write(data)

            assert not should_raises_error
            assert step == 0
        except ValueError:
            assert should_raises_error
            return
        except FileExistsError:
            assert step > 0

        assert content() == data


@pytest.mark.parametrize("text_mode", [True, False])
@pytest.mark.parametrize("atomicwrite", [True, False])
@pytest.mark.parametrize("lockfile", [True, False])
@pytest.mark.parametrize("exclusive", [True, False])
@pytest.mark.parametrize("backup", [True, False])
def test_overwrite(
        text_mode: bool,
        atomicwrite: bool,
        lockfile: bool,
        exclusive: bool,
        backup: bool,
    ):

    should_raises_error = atomicwrite and exclusive

    f = tmpdir_path / f'{nanoid.generate()}.txt'

    def content():
        return f.read_text() if text_mode else f.read_bytes()

    sizes = (10, 20, 5)
    data_strs = [nanoid.generate(size=s) for s in sizes]

    for data_str in data_strs:
        data = data_str if text_mode else data_str.encode('utf-8')

        try:
            with open_for_write(f,
                    text_mode=text_mode,
                    overwrite=True,
                    with_atomicwrite=atomicwrite,
                    with_lockfile=lockfile,
                    with_exclusive=exclusive,
                    with_backup=backup) as fp:
                fp.write(data)

            assert not should_raises_error
        except ValueError:
            assert should_raises_error
            continue

        assert content() == data


@pytest.mark.parametrize("text_mode", [True, False])
@pytest.mark.parametrize("atomicwrite", [True, False])
@pytest.mark.parametrize("lockfile", [True, False])
@pytest.mark.parametrize("exclusive", [True, False])
@pytest.mark.parametrize("backup_for_fault", [True, False])
def test_backup(
        text_mode: bool,
        atomicwrite: bool,
        lockfile: bool,
        exclusive: bool,
        backup_for_fault: bool,
    ):

    should_raises_error = atomicwrite and exclusive

    if atomicwrite and exclusive:
        return

    f = tmpdir_path / f'{nanoid.generate()}.txt'

    def content():
        return f.read_text() if text_mode else f.read_bytes()

    sizes = (10, 20, 5)
    data_strs = [nanoid.generate(size=s) for s in sizes]

    for data_str in data_strs:
        data = data_str if text_mode else data_str.encode('utf-8')

        try:
            with open_for_write(f,
                    text_mode=text_mode,
                    overwrite=True,
                    with_atomicwrite=atomicwrite,
                    with_lockfile=lockfile,
                    with_exclusive=exclusive,
                    with_backup=True,
                    backup_for_fault=backup_for_fault,
                    ) as fp:
                fp.write(data)

            assert not should_raises_error
        except ValueError:
            assert should_raises_error
            continue

        assert content() == data

        if backup_for_fault:
            assert not try_rollback(f)
        else:
            assert len(data_str) in sizes

            if len(data_str) in sizes[1:]:
                assert try_rollback(f)
                assert content() == data_strs[0] if text_mode else data_strs[0].encode('utf-8')
