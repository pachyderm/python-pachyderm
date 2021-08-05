#!/usr/bin/env python3
import datetime


def main():
    with open("/pfs/producer/content", "r") as f:
        with open("/pfs/out/results.txt", "w") as r:
            r.write(str(datetime.datetime.now()) + "\n")
            r.write(f.read())


if __name__ == "__main__":
    main()
