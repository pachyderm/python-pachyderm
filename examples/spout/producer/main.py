#!/usr/bin/env python3

import time
import random
import string

import python_pachyderm

for i in range(10):
    with python_pachyderm.SpoutManager() as spout:
        content = ''.join(random.choice(string.ascii_lowercase) for _ in range(2048))
        spout.add_from_bytes(str(i), content.encode())
    time.sleep(5)
