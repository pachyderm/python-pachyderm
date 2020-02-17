import io
import tarfile


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

    def __init__(self):
        """
        Creates a new spout manager.
        """

        self.f = None

    def __enter__(self):
        self.f = tarfile.open(fileobj=open("/pfs/out", "wb"), mode="w|", encoding="utf-8")
        return self

    def __exit__(self, type, value, traceback):
        self.f.close()

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
