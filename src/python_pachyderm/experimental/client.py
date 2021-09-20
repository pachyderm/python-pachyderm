import subprocess
from pathlib import Path
from typing import List
import time

from python_pachyderm import Client as _Client


class Mount:
    def __init__(self, mount_dir: str, repos: List[str] = [], debug: bool = False):
        Path(mount_dir).mkdir(parents=True, exist_ok=True)
        self.path = mount_dir

        cmd = ["pachctl", "mount", mount_dir]

        for r in repos:
            cmd.append("-r")
            cmd.append(r)

        if debug:
            cmd.append("-d")

        self.ps = subprocess.Popen(cmd)
        time.sleep(0.1)

    def unmount(self):
        self.ps.terminate()
        subprocess.Popen(["sudo", "pachctl", "unmount", self.path])

    def __del__(self):
        self.unmount()


class ExperimentalMixin:
    def mount(self, mount_dir: str, repos: List[str] = [], debug: bool = False):
        return Mount(mount_dir, repos, debug)


class EClient(_Client, ExperimentalMixin):
    def __init__(
        self,
        host: str = None,
        port: int = None,
        auth_token: str = None,
        root_certs: bytes = None,
        transaction_id: str = None,
        tls: bool = None,
        use_default_host: bool = True,
    ):
        _Client.__init__(
            self,
            host,
            port,
            auth_token,
            root_certs,
            transaction_id,
            tls,
            use_default_host,
        )
