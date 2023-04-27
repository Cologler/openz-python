# -*- coding: utf-8 -*-
# 
# Copyright (c) 2023~2999 - Cologler <skyoflw@gmail.com>
# ----------
# 
# ----------

from typing import Dict
from pathlib import Path
from contextlib import ExitStack, contextmanager
import os
import shutil

import portalocker
import filelock
import nanoid

BACKUP_FORMAT = '{name}.bak'

def create_path_dict(path: os.PathLike, include_random: bool) -> Dict[str, str]:
    path_obj = Path(path)
    path_dict = dict(
        name=path_obj.name,
        stem=path_obj.stem,
        suffix=path_obj.suffix
    )
    if include_random:
        path_dict['random'] = nanoid.generate()
    return path_dict

@contextmanager
def _atomic_open_for_write(dst: os.PathLike, parent: Path):
    tmp = parent / nanoid.generate()
    with open(tmp, 'wb') as fp:
        yield fp
    os.replace(tmp, dst)

@contextmanager
def open_for_write(path: os.PathLike, *,
        text_mode: bool = False,
        overwrite: bool = False,

        with_atomicwrite: bool = False,
        atomicwrite_format: str = '{stem}{random}{suffix}',

        with_lockfile: bool = False,
        lockfile_format: str = '{name}.lock',

        with_exclusive: bool = False,

        with_backup: bool = False,
        backup_format: str = BACKUP_FORMAT,
    ):

    if with_atomicwrite and with_exclusive:
        raise ValueError('`with_atomicwrite` and `with_exclusive` cannot be set at the same time.')

    if not overwrite and os.path.isfile(path):
        raise FileExistsError(path)

    path_dict = create_path_dict(path, with_atomicwrite)

    with ExitStack() as open_stack:

        parent = Path(path).parent

        if with_lockfile:
            lockfile_path = lockfile_format.format_map(path_dict)
            open_stack.enter_context(filelock.FileLock(lockfile_path))

        if with_backup and os.path.exists(path):
            backup_path = parent / backup_format.format_map(path_dict)
            with open(path, 'rb') as fp_src:
                with _atomic_open_for_write(backup_path, parent) as fp_dst:
                    shutil.copyfileobj(fp_src, fp_dst)

        if with_atomicwrite:
            open_mode = 'x'
            open_mode += '' if text_mode else 'b'
            atomicwrite_temp_path = parent / atomicwrite_format.format_map(path_dict)
            fp = open_stack.enter_context(open(atomicwrite_temp_path, open_mode))
        else:
            open_mode = 'w' if overwrite else 'x'
            open_mode += '' if text_mode else 'b'
            fp = open_stack.enter_context(open(path, open_mode))

        if with_exclusive:
            portalocker.lock(fp, portalocker.LOCK_EX)

        yield fp

    if with_atomicwrite:
        if overwrite:
            os.replace(atomicwrite_temp_path, path)
        else:
            os.rename(atomicwrite_temp_path, path)

def try_rollback(path: os.PathLike, *,
        backup_format: str = BACKUP_FORMAT,
    ):

    path_dict = create_path_dict(path, False)
    backup_path = backup_format.format_map(path_dict)
    if os.path.isfile(backup_path):
        os.replace(backup_path, path)
        return True

    return False
