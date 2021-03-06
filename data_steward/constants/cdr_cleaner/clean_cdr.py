from enum import Enum, unique

EHR = 'ehr'
UNIONED = 'unioned'
RDR = 'rdr'
COMBINED = 'combined'
DEID = 'deid'
DATASET_CHOICES = [EHR, UNIONED, RDR, COMBINED, DEID]

PERSON_TABLE_NAME = 'person'

# Query dictionary keys
QUERY = 'query'
LEGACY_SQL = 'use_legacy_sql'
DESTINATION_TABLE = 'destination_table_id'
RETRY_COUNT = 'retry_count'
DISPOSITION = 'write_disposition'
DESTINATION_DATASET = 'destination_dataset_id'
BATCH = 'batch'
PROCEDURE_OCCURRENCE = 'procedure_occurrence'
QUALIFIER_SOURCE_VALUE = 'qualifier_source_value'


@unique
class DataStage(Enum):
    UNSPECIFIED = 'unspecified'
    EHR = 'ehr'
    RDR = 'rdr'
    UNIONED = 'unioned'
    COMBINED = 'combined'
    DEID = 'deid'

    def __str__(self):
        return self.value
