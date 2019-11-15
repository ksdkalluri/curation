#!/usr/bin/env bash

# This Script automates the process of de-identification of the combined_dataset
# This script expects you are using the venv in curation directory

USAGE="
Usage: deid_runner.sh
  --key_file <path to key file>
  --cdr_id <combined_dataset name>
  --vocab_dataset <vocabulary dataset name>
  --identifier <version identifier>
"

while true; do
  case "$1" in
  --cdr_id)
    cdr_id=$2
    shift 2
    ;;
  --key_file)
    key_file=$2
    shift 2
    ;;
  --vocab_dataset)
    vocab_dataset=$2
    shift 2
    ;;
  --identifier)
    identifier=$2
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *) break ;;
  esac
done

if [[ -z "${key_file}" ]] || [[ -z "${cdr_id}" ]] || [[ -z "${vocab_dataset}" ]]; then
  echo "Specify the key file location and input dataset name application id and vocab dataset name. $USAGE"
  exit 1
fi

echo "key_file --> ${key_file}"
echo "cdr_id --> ${cdr_id}"
echo "vocab_dataset --> ${vocab_dataset}"

APP_ID=$(cat "${key_file}" | python -c 'import json,sys;obj=json.load(sys.stdin);print(obj["project_id"]);')
export GOOGLE_APPLICATION_CREDENTIALS="${key_file}"
export GOOGLE_CLOUD_PROJECT="${APP_ID}"

#set application environment (ie dev, test, prod)
gcloud auth activate-service-account --key-file="${key_file}"
gcloud config set project "${APP_ID}"

cdr_deid="${cdr_id}_deid"
cdr_deid_clean="${cdr_deid}_clean"

#------Create de-id virtual environment----------
set -e

# create a new environment in directory deid_env
TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
DATA_STEWARD_DIR="$(cd ${TOOLS_DIR} && cd .. && pwd )"
DEID_DIR="${DATA_STEWARD_DIR}/deid"
VENV_DIR="${DATA_STEWARD_DIR}/deid_venv"

virtualenv  -p $(which python3.7) "${VENV_DIR}"

source ${VENV_DIR}/bin/activate

# install the requirements in the virtualenv
pip install -r "${DATA_STEWARD_DIR}/requirements.txt"
pip install -r "${DEID_DIR}/requirements.txt"

export BIGQUERY_DATASET_ID="${cdr_deid}"
export PYTHONPATH="${PYTHONPATH}:${DEID_DIR}:${DATA_STEWARD_DIR}"

# Version is the most recent tag accessible from the current branch
version=$(git describe --abbrev=0 --tags)

# create empty de-id dataset
bq mk --dataset --description "${version} deidentified version of ${cdr_id}" "${APP_ID}":"${cdr_deid}"

# create the clinical tables
python "${DATA_STEWARD_DIR}/cdm.py" "${cdr_deid}"

# copy OMOP vocabulary
python "${DATA_STEWARD_DIR}/cdm.py" --component vocabulary "${cdr_deid}"
"${DATA_STEWARD_DIR}"/tools/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${vocab_dataset}" --target_dataset "${cdr_deid}"

# apply deidentification on combined dataset
python "${DATA_STEWARD_DIR}/tools/run_deid.py" --idataset "${cdr_id}" -p "${key_file}" -a submit --interactive  -c

# generate ext tables in deid dataset
python "${DATA_STEWARD_DIR}/tools/generate_ext_tables.py" -p "${APP_ID}" -d "${cdr_deid}" -c "${cdr_id}" -s

cdr_deid_base_staging="${identifier}_base_staging"
cdr_deid_base="${identifier}_base"
cdr_deid_clean_staging="${identifier}_clean_staging"
cdr_deid_clean="${identifier}_clean"

# create empty de-id_clean dataset to apply cleaning rules
bq mk --dataset --description "Intermediary dataset to apply cleaning rules on ${cdr_deid}" ${APP_ID}:${cdr_deid_base_staging}

# copy de_id dataset to a clean version
"${DATA_STEWARD_DIR}"/tools/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${cdr_deid}" --target_dataset "${cdr_deid_base_staging}"

export BIGQUERY_DATASET_ID="${cdr_deid_base_staging}"
export EHR_RDR_DEID_DATASET_ID="${cdr_deid_base_staging}"
data_stage='deid_base'

cd "${DATA_STEWARD_DIR}/cdr_cleaner/"

# run cleaning_rules on a dataset
python clean_cdr.py --data_stage ${data_stage} -s 2>&1 | tee deid_base_cleaning_log.txt

cd "${DATA_STEWARD_DIR}/tools/"

# Create a snapshot dataset with the result
python snapshot_by_query.py -p "${APP_ID}" -d "${cdr_deid_base_staging}" -n "${cdr_deid_base}"

bq update --description "${version} De-identified Base version of ${cdr_id}" ${APP_ID}:${cdr_deid_base}

#copy sandbox dataset
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${cdr_deid_base_staging}_sandbox" --target_dataset "${cdr_deid_base}_sandbox"


# create empty de-id_clean dataset to apply cleaning rules
bq mk --dataset --description "Intermediary dataset to apply cleaning rules on ${cdr_deid_base}" ${APP_ID}:${cdr_deid_clean_staging}

# copy de_id dataset to a clean version
"${DATA_STEWARD_DIR}"/tools/table_copy.sh --source_app_id "${APP_ID}" --target_app_id "${APP_ID}" --source_dataset "${cdr_deid_base}" --target_dataset "${cdr_deid_clean_staging}"

export BIGQUERY_DATASET_ID="${cdr_deid_clean_staging}"
export EHR_RDR_DEID_CLEAN_DATASET_ID="${cdr_deid_clean_staging}"
data_stage='deid_clean'

cd "${DATA_STEWARD_DIR}/cdr_cleaner/"

# run cleaning_rules on a dataset
python clean_cdr.py --data_stage ${data_stage} -s 2>&1 | tee deid_clean_cleaning_log.txt

cd "${DATA_STEWARD_DIR}/tools/"

# Create a snapshot dataset with the result
python snapshot_by_query.py -p "${APP_ID}" -d "${cdr_deid_clean_staging}" -n "${cdr_deid_clean}"

bq update --description "${version} De-identified Clean version of ${cdr_deid_base}" ${APP_ID}:${cdr_deid_clean}

#copy sandbox dataset
./table_copy.sh --source_app_id ${app_id} --target_app_id ${app_id} --source_dataset "${cdr_deid_clean_staging}_sandbox" --target_dataset "${cdr_deid_clean}_sandbox"

# deactivate virtual environment
deactivate
