"""
Deletes duplicate cope responses from a given {dataset}.observation table.

Uses service account impersonation.
"""
import logging

from google.auth import impersonated_credentials
from google.cloud import bigquery
from google.oauth2 import service_account
from jinja2 import Environment

from utils.pipeline_logging import setup_logger

LOGGER = logging.getLogger(__name__)

jinja_env = Environment(
    # help protect against cross-site scripting vulnerabilities
    autoescape=True,
    # block tags on their own lines
    # will not cause extra white space
    trim_blocks=True,
    lstrip_blocks=True,
    # syntax highlighting should be better
    # with these comment delimiters
    comment_start_string='--',
    comment_end_string=' --')

SANDBOX_QUERY = jinja_env.from_string("""
CREATE OR REPLACE TABLE `{{sandbox_dataset}}.dc1135_observation` AS SELECT
  *
FROM
  `{{dataset}}.observation`
WHERE
  observation_id IN
(
  SELECT
    observation_id
  FROM (
    SELECT
      *,
      DENSE_RANK() OVER(PARTITION BY person_id,
       observation_source_concept_id,
        observation_source_value,
         survey_version_concept_id
           ORDER BY is_pmi_skip ASC, max_observation_datetime DESC, questionnaire_response_id DESC) AS rank_order
    FROM (
      SELECT
        observation_id,
        person_id,
        observation_source_concept_id,
        observation_source_value,
        ob.questionnaire_response_id,
      IF
        (value_source_value = 'PMI_Skip',
          1,
          0) AS is_pmi_skip,
        MAX(observation_datetime)
         OVER(PARTITION BY person_id,
          observation_source_concept_id,
           observation_source_value,
             ob.questionnaire_response_id,
              survey_version_concept_id) AS max_observation_datetime,
        survey_version_concept_id,
        value_source_value
      FROM
        `{{dataset}}.observation` AS ob
      INNER JOIN
        `{{dataset}}.observation_ext` AS cs
      USING
        (observation_id)
      WHERE
        survey_version_concept_id IS NOT Null ) o ) o
  WHERE
    o.rank_order != 1)""")

DELETE_QUERY = jinja_env.from_string("""DELETE
FROM
  `{{dataset}}.observation`
WHERE
  observation_id IN(
  SELECT
    observation_id
  FROM {{sandbox_dataset}}.dc1135_observation)
    """)


def query_runner(client: bigquery.Client, dataset: bigquery.DatasetReference,
                 sandbox_dataset: bigquery.DatasetReference,
                 query) -> bigquery.QueryJob:
    """
    Deletes Duplicate COPE responses

    :param client: Active bigquery client object
    :param dataset: the dataset to delete duplicate responses from from
    :param sandbox_dataset: Dataset to store sandbox tables in
    :param query: delete/sandbox query to run

    :return: Query job associated with removing all the records
    :raises RuntimeError if CDM tables associated with a site are not found in the dataset
    """
    LOGGER.debug(f'Delete duplicate cope responses={dataset.dataset_id}')

    errors = []
    print("query,rows_updated")
    q = query.render(dataset=dataset.dataset_id,
                     sandbox_dataset=sandbox_dataset.dataset_id)

    job = client.query(q)
    response = job.result()

    if job.exception():
        errors.append(job.exception())
        print(f"FAILURE: {job.exception()}\nProblem executing query:\n{query}")
        raise RuntimeError(f"Unable to run query->\n {query}")
    else:
        print(f"\"{query}\",{job.num_dml_affected_rows}")


def main(credentials_file, project_id, dataset_id, target_principal):
    """
    Copy tables with impersonated credentials.

    :param credentials_file:  path to your service account key file
    :param project_id:  Identifies the project name.
    :param dataset_id:  dataset to delete duplicate responses from
    :param target_principal: email address of account to impersonate.
    """
    target_scopes = [
        'https://www.googleapis.com/auth/devstorage.read_only',
        'https://www.googleapis.com/auth/bigquery',
    ]
    source_credentials = (service_account.Credentials.from_service_account_file(
        credentials_file, scopes=target_scopes))
    target_credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=target_principal,
        # TODO:  Remove this comment before adding to repo
        # target_principal='aou-res-curation-test@appspot.gserviceaccount.com',
        target_scopes=target_scopes,
        lifetime=3600)
    client = bigquery.Client(project=project_id, credentials=target_credentials)
    dataset = bigquery.DatasetReference(project_id, dataset_id)
    sandbox_dataset = bigquery.DatasetReference(project_id,
                                                dataset_id + '_sandbox')
    # sandbox duplicate cope responses
    query_runner(client, dataset, sandbox_dataset, SANDBOX_QUERY)

    # Delete duplicate cope responses
    query_runner(client, dataset, sandbox_dataset, DELETE_QUERY)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('-c',
                        '--credentials',
                        required=True,
                        help='Path to GCP credentials file')
    parser.add_argument(
        '-i',
        '--impersonate',
        required=True,
        dest='target_principal',
        help=
        'Service account email address to impersonate when running this script')
    parser.add_argument('-p',
                        '--project_id',
                        required=True,
                        help='Identifies the project')
    parser.add_argument(
        '-d',
        '--dataset_id',
        required=True,
        help='Identifies the dataset to remove duplicate COPE responses for')
    ARGS = parser.parse_args()

    setup_logger(['delete_duplicate_cope_responses.log'])
    main(ARGS.credentials, ARGS.project_id, ARGS.dataset_id,
         ARGS.target_principal)
