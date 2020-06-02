import io
import os
import tarfile
import contextlib


class SpoutManager:
    """
    A convenience context manager for creating spouts, allowing you to create
    spout code like:

    ```
    while True:
        with SpoutManager() as spout:
            spout.add_from_bytes("foo", b"#")
        time.sleep(1.0)
    ```
    """

    def __init__(self, marker_filename=None, pfs_directory="/pfs"):
        """
        Creates a new spout manager.

        Params:

        * `marker_filename`: The name of the file for storing markers. If
        unspecified, marker-related operations will fail.
        * `pfs_directory`: The directory for PFS content. Usually this
        shouldn't be explicitly specified, unless the spout manager is being
        tested outside of a real Pachyderm pipeline.
        """

        self.f = None
        self.marker_filename = marker_filename
        self.pfs_directory = pfs_directory

    def __enter__(self):
        f = open(os.path.join(self.pfs_directory, "out"), "wb")
        self.f = tarfile.open(fileobj=f, mode="w|", encoding="utf-8")
        return self

    def __exit__(self, type, value, traceback):
        self.f.close()

    @contextlib.contextmanager
    def marker(self):
        """
        Gets the marker file as a context manager.
        """

        if self.marker_filename is None:
            raise Exception("no marker filename set")
        with open(os.path.join(self.pfs_directory, self.marker_filename), "r") as f:
            yield f

    def add_from_fileobj(self, path, size, fileobj):
        """
        Adds a file to the spout from a file-like object.

        Params:

        * `path`: The path to the file in the spout.
        * `size`: The size of the file.
        * `fileobj`: The file-like object to add.
        """

        tar_info = tarfile.TarInfo(path)
        tar_info.size = size
        tar_info.mode = 0o600
        self.f.addfile(tarinfo=tar_info, fileobj=fileobj)

    def add_from_bytes(self, path, bytes):
        """
        Adds a file to the spout from a bytestring.

        Params:

        * `path`: The path to the file in the spout.
        * `bytes`: The bytestring representing the file contents.
        """

        self.add_from_fileobj(path, len(bytes), io.BytesIO(bytes))

    def add_marker_from_fileobj(self, size, fileobj):
        """
        Writes to the marker file from a file-like object.

        Params:

        * `size`: The size of the file.
        * `fileobj`: The file-like object to add.
        """

        if self.marker_filename is None:
            raise Exception("no marker filename set")
        self.add_from_fileobj(self.marker_filename, size, fileobj)

    def add_marker_from_bytes(self, bytes):
        """
        Adds to the marker from a bytestring.

        Params:

        * `bytes`: The bytestring representing the file contents.
        """

        if self.marker_filename is None:
            raise Exception("no marker filename set")
        self.add_from_fileobj(self.marker_filename, len(bytes), io.BytesIO(bytes))
