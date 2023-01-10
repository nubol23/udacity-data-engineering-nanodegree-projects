from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from operator import itemgetter


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 conn_id,
                 checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.checks = checks

    def execute(self, context):
        hook = PostgresHook(self.conn_id)
        
        for check in self.checks:
            table, test_sql, expected, operator, msg = itemgetter(
                "table", 
                "test_sql", 
                "expected", 
                "operator", 
                "message"
            )(check)
            records = hook.get_records(test_sql)

            try:
                num_records = records[0][0]
                if not operator(num_records, expected):
                    raise ValueError(f"Quality check failed, {msg}")
            except IndexError:
                raise ValueError(f"Quality check failed, {table} returns no results")
            
            self.log.info(f"{table}'s data quality check passed with {num_records} records")
