#!/bin/bash

mkdir -p env.systemd

message="$(openssl rand -hex 16)"

{
  echo "CENTRAL_HOST=10.0.0.128"
  echo "NODE_NAME=fledger-n0-0"
  echo "NODE_CMD=simulation send-chat '$message'"
  echo "WAIT=false"
} >"env.systemd/n0/fledger-n0-0"

{
  echo "CENTRAL_HOST=10.0.0.128"
  echo "NODE_NAME=fledger-n1-0"
  echo "NODE_CMD=simulation recv-chat '$message' --print-new-messages"
  echo "WAIT=true"
  echo ""
} >"env.systemd/n1/fledger-n1-0"
