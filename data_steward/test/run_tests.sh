#!/bin/bash -e
# Runs unit tests (no server interaction) and client tests (hit a local dev
# server). Fails if any test fails.


BASE_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && cd .. && pwd )"
. ${BASE_DIR}/tools/set_path.sh

subset="all"


function usage() {
  echo "Usage: run_test.sh -g /path/to/google/cloud/sdk_dir" \
      "[-s all|unit|client]" \
      "[-r <file name match glob, e.g. 'extraction_*'>]" >& 2
  exit 1
}

while getopts "s:g:h:r:" opt; do
  case $opt in
    g)
      sdk_dir=$OPTARG
      echo "Using SDK dir: $OPTARG" >&2
      ;;
    s)
      subset=$OPTARG
      ;;
    r)
      substring=$OPTARG
      ;;
    \?)
      echo "Invalid option: -$OPTARG" >&2
      exit 1
      ;;
    :)
      echo "Option -$OPTARG requires an argument." >&2
      exit 1
      ;;
    h|*)
      usage
      ;;
  esac
done

if [ -z "$sdk_dir" ];
then
  sdk_dir="$GAE_SDK_ROOT"
fi

if [[ $subset ]];
then
  echo Executing subset $subset
fi

if [[ $substring ]];
then
   echo Executing tests that match glob $substring
fi

if [[ "$subset" == "all" || "$subset" == "unit" ]];
then
  if [[ -z $substring ]]
  then
    cmd="test/runner.py --test-path test/unit_test/ ${sdk_dir}"
  else
    cmd="test/runner.py --test-path test/unit_test/ ${sdk_dir} --test-pattern $substring"
  fi
  (cd ${BASE_DIR}; python $cmd)
fi
