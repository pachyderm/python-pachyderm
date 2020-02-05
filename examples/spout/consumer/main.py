#!/usr/bin/env python3

import datetime

def main():
    with open("/pfs/printer/foo", "r") as f:
        print(datetime.datetime.now(), f.read())

if __name__ == "__main__":
    main()
