#!/usr/bin/env python3

import time
import python_pachyderm

def main():
    while True:
        with python_pachyderm.SpoutProducer() as spout:
            spout.add_from_bytes("foo", b"#")
        time.sleep(1.0)

if __name__ == "__main__":
    main()
