#!/bin/bash

mkdir -p env.systemd

#amount=100
amount=10

messages=()

for i in $(seq $amount); do
  message="$(openssl rand -hex 16)"
  messages+=("$message")
done

for i in $(seq 0 $((amount - 1))); do
  j=$((("$i" + 2) % amount))
  nodename="n${i}"
  envfile="env.systemd/${nodename}.env"
  send_msg="${messages[$j]}"
  recv_msg="${messages[$i]}"

  if test "$i" -lt 5; then
    signalhost="10.0.0.128"
  else
    signalhost="10.0.1.128"
  fi

  echo "FLEDGER_FLSIGNAL_HOST=${signalhost}" >"$envfile"
  echo "FLEDGER_SEND_MSG=${send_msg}" >>"$envfile"
  echo "FLEDGER_RECV_MSG=${recv_msg}" >>"$envfile"

  echo "[node $nodename]"
  echo "    <- ${recv_msg} [$i]"
  echo "    -> ${send_msg} [$j]"
done
