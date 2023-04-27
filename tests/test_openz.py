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

from openz import open_for_write

tmpdir = tempfile.TemporaryDirectory(prefix='openz-tests-')
atexit.register(lambda: tmpdir.cleanup())
tmpdir_path = Path(tmpdir.name)

@pytest.mark.parametrize("backup", [True, False])
@pytest.mark.parametrize("exclusive", [True, False])
@pytest.mark.parametrize("lockfile", [True, False])
@pytest.mark.parametrize("atomicwrite", [True, False])
@pytest.mark.parametrize("text_mode", [True, False])
def test_write(
        text_mode: bool,
        atomicwrite: bool,
        lockfile: bool,
        exclusive: bool,
        backup: bool,
    ):

    should_raises_error = atomicwrite and exclusive

    if atomicwrite and exclusive:
        return

    f = tmpdir_path / f'{nanoid.generate()}.txt'


    data_str = nanoid.generate(size=10)
    data = data_str if text_mode else data_str.encode('utf-8')

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
    except ValueError:
        assert should_raises_error
        return

    content = f.read_text() if text_mode else f.read_bytes()
    assert content == data

@pytest.mark.parametrize("backup", [True, False])
@pytest.mark.parametrize("exclusive", [True, False])
@pytest.mark.parametrize("lockfile", [True, False])
@pytest.mark.parametrize("atomicwrite", [True, False])
@pytest.mark.parametrize("text_mode", [True, False])
def test_overwrite(
        text_mode: bool,
        atomicwrite: bool,
        lockfile: bool,
        exclusive: bool,
        backup: bool,
    ):

    should_raises_error = atomicwrite and exclusive

    if atomicwrite and exclusive:
        return

    f = tmpdir_path / f'{nanoid.generate()}.txt'

    for data_str in [nanoid.generate(size=10), nanoid.generate(size=20), nanoid.generate(size=5)]:
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

        content = f.read_text() if text_mode else f.read_bytes()
        assert content == data
