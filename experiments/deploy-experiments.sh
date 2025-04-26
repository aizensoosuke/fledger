#!/bin/bash

if test "$#" -ne 0; then
  echo "usage: $0"
  exit 1
fi

echo "Rsyncing experiment files..."
rsync -avh --delete --info=progress2 ./ sphere-fledger:~/experiments/
