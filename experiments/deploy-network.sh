#!/bin/bash

# CHANGE THESE
project="fledger"
username="abehsser"
# END CHANGE THESE

network="$1"
path="$HOME/fledger/experiments/$1"
repo="$HOME/fledger/sphere/"
model="$path/model.py"

base=$(basename "$network")
realizeas="$base.$project.$username"
materializeas="$realizeas"

token=$(mrg whoami -t)
remote="https://$token:@git.sphere-testbed.net/$username/$project"

echo "Deploying experiment network: [$network]"

if ! test -d "$repo"; then
  echo "FATAL: repo not found at [$repo]."
  echo "You must clone the experiments repo (ex https://git.sphere-testbed.net/abehsser/fledger)."
  exit 1
fi

if ! test -f "$model"; then
  echo "FATAL: Model file [$model] not found."
  exit 1
fi

if test -z "$token"; then
  echo "FATAL: Mergetb token not found, make sure you're logged in."
  exit 1
else
  echo "Found token: $token"
fi

echo "Copying model to repo..."
cp "$model" "$repo"

cd "$repo" || exit 1

echo "[MODEL]"
echo "    Compiling model..."
mrg compile "model.py" -q || exit 1

echo "    Pushing model to $remote..."
git add model.py >/dev/null
git commit -m "commit from deploy tool" >/dev/null
git push "$remote" >/dev/null || exit 1

revision=$(git rev-parse HEAD)
echo "    SUCCESS: revision $revision created."

echo "[REALIZATION]"
echo "    Checking if $realizeas exists..."
mrg show realization "$realizeas" >/dev/null 2>&1
if test $? -eq 0; then
  echo "    Realization exists, we must RELINQUISH it to continue."
  read -r -p "    Continue (y/n)? " choice
  case "$choice" in
  y | Y) echo "yes" ;;
  n | N)
    echo "no"
    exit 0
    ;;
  *)
    echo "invalid"
    exit 1
    ;;
  esac

  echo "    RELINQUISHING $realizeas"
  sleep 2
  mrg relinquish "$realizeas"
fi

echo "    Realize model as $realizeas..."
mrg realize "$realizeas" revision "$revision" || exit 1
echo "    SUCCESS: realization created"

echo "[MATERIALIZATION]"
echo "    Materialize model as $materializeas..."
mrg mat "$materializeas" --sync || exit 1

count=0
while test -z "$success"; do
  success=$(mrg show mat "$realizeas" -S | grep Success)
  if test -z "$success"; then
    echo "    Finished but API says unsuccessful, checking again ($count/100)..."
    sleep 2
  fi
  ((count++)) && ((count == 100)) && break
done

if test -z "$success"; then
  mrg show mat "$materializeas"
  echo "FATAL: materialization failed."
  exit 1
else
  echo "    SUCCESS: materialization created"
fi

echo "[XDC ATTACH]"
echo "    Detaching XDC..."
mrg xdc detach "$project.$username" || exit 1

echo "    Attaching XDC..."
mrg xdc attach "$project.$username" "$realizeas" || exit 1
