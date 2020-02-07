#!/usr/bin/env python3

import time
import python_pachyderm

MAX_ATTEMPTS = 10

def main():
    while True:
        attempts = 0

        while True:
            try:
                with python_pachyderm.SpoutManager() as spout:
                    spout.add_from_bytes("foo", b"#")
                break
            except:
                attempts += 1
                if attempts == MAX_ATTEMPTS:
                    raise
            
            time.sleep(1.0)

        time.sleep(1.0)

if __name__ == "__main__":
    main()
