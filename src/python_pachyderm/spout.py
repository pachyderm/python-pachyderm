import os
import tarfile
import time
import io
import stat

class SpoutProducer:
    def __init__(self, max_attempts=10, sleep_time=1):
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
                except OSError as e:
                    attempts += 1
                    if attempts == self.max_attempts:
                        raise
                    time.sleep(self.sleep_time)
        finally:
            os.umask(umask_original)

    def __exit__(self):
        self.f.close()

    def add_from_fileobj(self, path, size, fileobj):
        tar_info = tarfile.TarInfo(path)
        tar_info.size = size
        tar_info.mode = 0o600
        self.f.addfile(tarinfo=tar_info, fileobj=fileobj)

    def add_from_bytes(self, path, bytes):
        self.add_from_fileobj(path, len(bytes), io.BytesIO(bytes))

    def add_from_path(self, local_path, spout_path=None):
        with open(local_path, "rb") as f:
            self.add_from_fileobj(spout_path or local_path, os.path.getsize(local_path), f)
