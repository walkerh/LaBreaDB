"""Use tar to store key/value pairs where the key is the member name and the
value is the member contents.

    from %s import TarDbWriter, TarDbReader, load_dict, save_map

    with TarDbWriter('foo.tar.gz', 'w:gz') as tw:
        tw.put('spam', 'green eggs')
        tw.put('foo', 'bar of candy')

    with TarDbReader('foo.tar.gz') as tr:
        db1 = dict(tr)  # tr is an item iterator

    db2 = load_dict('foo*.tar.gz')  # Slurp all tarballs
    save_map(db2, 'consolidated-results.tar')

    Additionally there is support for sets and keys that are generated
    from the values.

""" % __name__

# TODO: Python 3 implications for str vs bytes.

import contextlib
from cStringIO import StringIO
import glob
import grp
import os
import pwd
import re
import tarfile
import time


FILE_PAT = re.compile(r'(.+)\.((tar(\.(gz|bz2))?)|(tgz|tbz|tbz2|tb2))')


class TarDb(object):
    """Wraps a tar file representing a single directory with files."""
    def __init__(self, file_name, mode='r', dir_name=None):
        """file_name = file_name of file (without extension) and directory"""
        super(TarDb, self).__init__()
        self.file_name = file_name
        if dir_name:
            self.dir_name = dir_name
        else:
            m = FILE_PAT.match(self.file_name)
            assert m
            self.dir_name = m.group(1)
        self.mode = mode
        self.tar = tarfile.open(self.file_name, self.mode)

    def close(self):
        self.tar.close()

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()


class TarDbWriter(TarDb):
    """Used for writing.
    mode is the same as specified within the tarfile library.
    dir_name is the name of the inner directory inside the tar file, which
    defaults to file_name with the extension removed.
    keygen = a callable that extracts a key from a value when
    executing add_value(value)."""
    def __init__(self, file_name, mode='a', dir_name=None, keygen=None):
        assert mode[0] in 'aw'
        new_file = not os.path.isfile(file_name) or mode[0] == 'w'
        TarDb.__init__(self, file_name, mode, dir_name)
        self.keygen = keygen
        # TODO: Consider getpass.getuser() for better portability.
        self.uname = pwd.getpwuid(os.geteuid()).pw_name
        self.gname = grp.getgrgid(os.getegid()).gr_name
        if new_file:
            ti = self._make_tarinfo(self.dir_name, 0, True)
            self.tar.addfile(ti)

    def add(self, value):
        """Add a value, computing the key using the keygen callable.
        This introduces set semantics."""
        assert self.keygen
        key = self.keygen(value)
        self.put(key, value)

    def put(self, key, value):
        assert isinstance(key, str)
        assert isinstance(value, str)
        path = '/'.join((self.dir_name, key))
        ti = self._make_tarinfo(path, len(value))
        fo = StringIO(value)
        self.tar.addfile(ti, fo)

    def _make_tarinfo(self, name, size, is_dir=False):
        ti = tarfile.TarInfo(name)
        if is_dir:
            assert size == 0
            ti.type = tarfile.DIRTYPE
            ti.mode = 0777
        else:
            assert ti.type == tarfile.REGTYPE
            ti.mode = 0666
            ti.size = size
        ti.mtime = time.time()
        ti.uname = self.uname
        ti.gname = self.gname
        return ti


class TarDbReader(TarDb):
    """Used for Reading. Implements iterator protocol"""
    def __init__(self, file_name):
        assert os.path.isfile(file_name)
        TarDb.__init__(self, file_name)

    def __iter__(self):
        return self

    def next(self):
        """Iterator"""
        while True:  # Will skip over any directories or specials.
            ti = self.tar.next()
            if not ti:
                raise StopIteration
            if ti.isfile():
                break
        i = ti.name.find('/')
        if i < 0:
            key = ti.name
        else:
            key = ti.name[i+1:]
        with contextlib.closing(self.tar.extractfile(ti)) as fin:
            value = fin.read()
        return key, value


def save_map(mapping, file_name, mode='a', dir_name=None):
    """Save every key-value item in mapping to the tar file in key order.
    The keys and values must all be of type str (bytes).
    mode is the same as specified within the tarfile library.
    dir_name is the name of the inner directory inside the tar file, which
    defaults to file_name with the extension removed."""
    with TarDbWriter(file_name, mode, dir_name) as tw:
        keys = mapping.keys()
        keys.sort()
        for key in keys:
            tw.put(key, mapping[key])


def save_set(sequence, file_name, keygen, mode='a', dir_name=None):
    """Save value in sequence to the tar file in sequence order.
    The must all be of type str (bytes), and keygen must be a callable that
    takes a value and returns a str key.
    mode is the same as specified within the tarfile library.
    dir_name is the name of the inner directory inside the tar file, which
    defaults to file_name with the extension removed."""
    with TarDbWriter(file_name, mode, dir_name, keygen) as tw:
        for value in sequence:
            tw.add(value)


def load_dict(*glob_patterns):
    """For each glob pattern provided in order, expand to a list of files
    sorted lexically, and load the corresponding tar data into a dict. The
    order of loading is predictable, but this only matters if the same key
    appears more than once within the collection of tar files."""
    result = {}
    for glob_pattern in glob_patterns:
        paths = glob.glob(glob_pattern)
        paths.sort()
        for path in paths:
            with TarDbReader(path) as tr:
                for key, value in tr:
                    result[key] = value
    return result


def load_set(*glob_patterns):
    """For each glob pattern provided in order, expand to a list of files
    sorted lexically, and load the corresponding tar data into a dict. The
    order of loading is predictable, but this only matters if the same key
    appears more than once within the collection of tar files."""
    result = set()
    for glob_pattern in glob_patterns:
        paths = glob.glob(glob_pattern)
        paths.sort()
        for path in paths:
            with TarDbReader(path) as tr:
                for key, value in tr:
                    result.add(value)
    return result
