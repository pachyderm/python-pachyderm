import os
import tarfile
import time
import io
import stat


class SpoutManager:
    """
    A convenience context manager for creating spouts, allowing you to create spout code like:

    ```
    while True:
        with SpoutManager() as spout:
            spout.add_from_bytes("foo", b"#")
        time.sleep(1.0)
    ```
    """

    def __init__(self, max_attempts=10, sleep_time=1):
        """
        Creates a new spout manager.

        Params:

        * `max_attempts`: An optional int specifying the maximum number of
        times the manager should attempt to open the spout tarpipe.
        * `sleep_time`: An optional int specifying the sleep time between
        attempting to open the spout tarpipe.
        """

        self.f = None
        self.max_attempts = max_attempts
        self.sleep_time = 1

    def __enter__(self):
        attempts = 0
        umask_original = os.umask(0o777 ^ stat.S_IWUSR)

        try:
            while True:
                try:
                    f1 = os.open("/pfs/out", os.O_WRONLY, stat.S_IWUSR)
                    f2 = os.fdopen(f1, "wb")
                    self.f = tarfile.open(fileobj=f2, mode="w|", encoding="utf-8")
                    return self
                except OSError:
                    attempts += 1
                    if attempts == self.max_attempts:
                        raise
                    time.sleep(self.sleep_time)
        finally:
            os.umask(umask_original)

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
