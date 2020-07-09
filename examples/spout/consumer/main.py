#!/usr/bin/env python3

import os

def main():
    for filename in os.listdir("/pfs/producer"):
        with open(os.path.join("/pfs/producer", filename), "r") as f:
            print(filename, f.read())

if __name__ == "__main__":
    main()
