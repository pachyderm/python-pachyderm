#!/usr/bin/env python3

import datetime

def main():
    with open("/pfs/producer/content", "r") as f:
        print(datetime.datetime.now(), f.read())

if __name__ == "__main__":
    main()
