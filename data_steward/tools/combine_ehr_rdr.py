"""
Combine data sets `ehr` and `rdr` to form another data set `combined`

 * Load `person_id` of those who have consented to share EHR data in `combined.ehr_consent`

 * Copy all `rdr.person` records to `combined.person`

 * Load `combined.visit_mapping(dst_visit_occurrence_id, src_dataset, src_visit_occurrence_id)`
   with UNION ALL of all `rdr.visit_occurrence_id`s and `ehr.visit_occurrence_id`s that link to `combined.ehr_consent`

 * Create tables `combined.{visit_occurrence, condition_occurrence, procedure_occurrence}` etc. from UNION ALL of
   `ehr` and `rdr` records that link to `combined.person`. Use `combined.<hpo>_visit_mapping.dest_visit_occurrence_id`
   for records that have a (valid) `visit_occurrence_id`.

 * Load `combined.<hpo>_observation` with records derived from values in `ehr.<hpo>_person`

## Notes
Currently the following environment variables must be set:
 * BIGQUERY_DATASET_ID: BQ dataset where combined result is stored (e.g. test_join_ehr_rdr)
 * APPLICATION_ID: GCP project ID (e.g. all-of-us-ehr-dev)
 * GOOGLE_APPLICATION_CREDENTIALS: location of service account key json file (e.g. /path/to/all-of-us-ehr-dev-abc123.json)

TODO
 * Communicate to data steward EHR records not matched with RDR
"""
import json
import os
import logging

import bq_utils
from resources import fields_path

SOURCE_VALUE_EHR_CONSENT = 'EHRConsentPII_ConsentPermission'
CONCEPT_ID_CONSENT_PERMISSION_YES = 1586100  # ConsentPermission_Yes
EHR_CONSENT_TABLE_ID = '_ehr_consent'
VISIT_OCCURRENCE = 'visit_occurrence'
VISIT_OCCURRENCE_ID = 'visit_occurrence_id'
TABLE_NAMES = ['person', VISIT_OCCURRENCE, 'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
               'device_exposure', 'measurement', 'observation', 'death']
DOMAIN_TABLES = [VISIT_OCCURRENCE, 'condition_occurrence', 'procedure_occurrence', 'drug_exposure',
                 'device_exposure', 'measurement', 'observation']


def query(q, dst_table_id, write_disposition='WRITE_TRUNCATE'):
    """
    Run query and block until job is done
    :param q: SQL statement
    :param dst_table_id: if set, output is saved in a table with the specified id
    :param write_disposition: WRITE_TRUNCATE (default), WRITE_APPEND or WRITE_EMPTY
    """
    dst_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
    query_job_result = bq_utils.query(q, destination_table_id=dst_table_id, write_disposition=write_disposition,
                                      destination_dataset_id=dst_dataset_id)
    query_job_id = query_job_result['jobReference']['jobId']
    incomplete_jobs = bq_utils.wait_on_jobs([query_job_id])
    if len(incomplete_jobs) > 0:
        raise bq_utils.BigQueryJobWaitError(incomplete_jobs)


def ehr_consent_query():
    """
    Returns query used to get only those participants who have consented to share EHR data

    :return:
    """
    return '''
    WITH ordered_response AS
     (SELECT
        person_id, 
        value_source_concept_id,
        observation_datetime,
        ROW_NUMBER() OVER(PARTITION BY person_id ORDER BY observation_datetime DESC, value_source_concept_id ASC) AS rn
      FROM {dataset_id}.observation
      WHERE observation_source_value = '{source_value_ehr_consent}')
    
     SELECT person_id 
     FROM ordered_response
     WHERE rn = 1 
       AND value_source_concept_id = {concept_id_consent_permission_yes}
    '''.format(dataset_id=bq_utils.get_rdr_dataset_id(),
               source_value_ehr_consent=SOURCE_VALUE_EHR_CONSENT,
               concept_id_consent_permission_yes=CONCEPT_ID_CONSENT_PERMISSION_YES)


def ehr_consent():
    """
    Create and load ehr consent table in combined dataset

    :return:
    """
    q = ehr_consent_query()
    logging.debug('Query for {ehr_consent_table_id} is {q}'.format(ehr_consent_table_id=EHR_CONSENT_TABLE_ID, q=q))
    query(q, EHR_CONSENT_TABLE_ID)


def copy_rdr_person():
    """
    Copy person table from the RDR dataset to the combined dataset

    Note: Overwrites if a person table already exists
    """
    q = '''SELECT * FROM {rdr_dataset_id}.person rp'''.format(rdr_dataset_id=bq_utils.get_rdr_dataset_id())
    logging.debug('Query for person is `{q}`'.format(q=q))
    query(q, 'person')


def mapping_query(domain_table):
    """
    Returns query used to get mapping of all records from RDR combined with EHR records of consented participants

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    return '''
    WITH all_records AS
    (
        SELECT
          '{rdr_dataset_id}'  AS src_dataset_id, 
          {domain_table}_id AS src_{domain_table}_id
        FROM {rdr_dataset_id}.{domain_table}

        UNION ALL

        SELECT
          '{ehr_dataset_id}'  AS src_dataset_id, 
          {domain_table}_id AS src_{domain_table}_id
        FROM {ehr_dataset_id}.{domain_table} t
        WHERE EXISTS
           (SELECT 1 FROM {ehr_rdr_dataset_id}.{ehr_consent_table_id} c 
            WHERE t.person_id = c.person_id)
    )
    SELECT 
      ROW_NUMBER() OVER (ORDER BY src_dataset_id, src_{domain_table}_id) AS {domain_table}_id,
      src_dataset_id,
      src_{domain_table}_id
    FROM all_records
    '''.format(rdr_dataset_id=bq_utils.get_rdr_dataset_id(),
               ehr_dataset_id=bq_utils.get_dataset_id(),
               ehr_rdr_dataset_id=bq_utils.get_ehr_rdr_dataset_id(),
               domain_table=domain_table,
               ehr_consent_table_id=EHR_CONSENT_TABLE_ID)


def mapping_table_for(domain_table):
    """
    Get name of mapping table generated for a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    return '_mapping_' + domain_table


def mapping(domain_table):
    """
    Create and load a mapping of all records from RDR combined with EHR records of consented participants

    :param domain_table:
    :return:
    """
    q = mapping_query(domain_table)
    mapping_table = mapping_table_for(domain_table)
    logging.debug('Query for {mapping_table} is {q}'.format(mapping_table=mapping_table, q=q))
    query(q, mapping_table)


def load_query(domain_table):
    """
    Returns query used to load a domain table

    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    :return:
    """
    rdr_dataset_id = bq_utils.get_rdr_dataset_id()
    ehr_dataset_id = bq_utils.get_dataset_id()
    ehr_rdr_dataset_id = bq_utils.get_ehr_rdr_dataset_id()
    mapping_table = mapping_table_for(domain_table)
    json_path = os.path.join(fields_path, domain_table + '.json')
    is_visit_occurrence = domain_table == VISIT_OCCURRENCE
    id_col = '{domain_table}_id'.format(domain_table=domain_table)

    # Generate expressions for select
    with open(json_path, 'r') as fp:
        fields = json.load(fp)
        col_exprs = []
        for field in fields:
            field_name = field['name']
            field_type = field['type']
            if field_name == id_col:
                # Use mapping for unique ID column
                col_expr = 'm.%(field_name)s ' % locals()
            elif field_name == VISIT_OCCURRENCE_ID:
                # Replace with mapped visit_occurrence_id
                # Note: This is only reached when domain_table != visit_occurrence
                col_expr = 'mv.' + VISIT_OCCURRENCE_ID
            else:
                col_expr = field_name
            col_exprs.append(col_expr)
    cols = ',\n  '.join(col_exprs)

    visit_join_expr = ''
    if not is_visit_occurrence:
        # Include a join to mapping for visit_occurrence
        # Note: Using left join in order to keep records that aren't mapped to visits
        mv = mapping_table_for(VISIT_OCCURRENCE)
        visit_join_expr = '''
        LEFT JOIN {ehr_rdr_dataset_id}.{mapping_visit_occurrence} mv 
          ON t.visit_occurrence_id = mv.src_visit_occurrence_id
         AND m.src_dataset_id = mv.src_dataset_id'''.format(ehr_rdr_dataset_id=ehr_rdr_dataset_id,
                                                            mapping_visit_occurrence=mv)

    return '''
    SELECT {cols} 
    FROM {rdr_dataset_id}.{domain_table} t 
      JOIN {ehr_rdr_dataset_id}.{mapping_table} m
        ON t.{domain_table}_id = m.src_{domain_table}_id {visit_join_expr}
    WHERE m.src_dataset_id = '{rdr_dataset_id}'
    
    UNION ALL
    
    SELECT {cols} 
    FROM {ehr_dataset_id}.{domain_table} t 
      JOIN {ehr_rdr_dataset_id}.{mapping_table} m
        ON t.{domain_table}_id = m.src_{domain_table}_id {visit_join_expr}
    WHERE m.src_dataset_id = '{ehr_dataset_id}'
    '''.format(cols=cols,
               domain_table=domain_table,
               rdr_dataset_id=rdr_dataset_id,
               ehr_dataset_id=ehr_dataset_id,
               mapping_table=mapping_table,
               visit_join_expr=visit_join_expr,
               ehr_rdr_dataset_id=ehr_rdr_dataset_id)


def load(domain_table):
    """
    Load a domain table
    :param domain_table: one of the domain tables (e.g. 'visit_occurrence', 'condition_occurrence')
    """
    q = load_query(domain_table)
    logging.debug('Query for {domain_table} is {q}'.format(domain_table=domain_table, q=q))
    query(q, domain_table)


def main():
    logging.info('EHR + RDR combine started')
    logging.info('Loading {ehr_consent_table_id}...'.format(ehr_consent_table_id=EHR_CONSENT_TABLE_ID))
    ehr_consent()
    copy_rdr_person()
    for domain_table in DOMAIN_TABLES:
        logging.info('Mapping {domain_table}...'.format(domain_table=domain_table))
        mapping(domain_table)
    for domain_table in DOMAIN_TABLES:
        logging.info('Loading {domain_table}...'.format(domain_table=domain_table))
        load(domain_table)
    logging.info('EHR + RDR combine completed')


if __name__ == '__main__':
    main()
