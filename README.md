# openz

A strange file opener.

## Usage

``` python
from openz import open_for_write, try_rollback

with open_for_write(
            path,
            text_mode=True,
            overwrite=True,
            with_atomicwrite=True,
            with_lockfile=True,
            with_exclusive=False, # unable work with `with_atomicwrite`
            with_backup=True
        ) as fp:

    fp.write('content')
```

### Restore data from crash

Make sure set `with_backup=True` and `backup_for_fault=True`:

``` python
with open_for_write(path, with_backup=True, backup_for_fault=True) as fp:
    ...
```

Then you can restore it from the backup:

``` python
try_rollback(path)
```
