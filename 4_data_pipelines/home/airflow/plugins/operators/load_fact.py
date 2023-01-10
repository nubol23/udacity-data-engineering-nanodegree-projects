from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Connected to redshift")
        
        self.log.info("Load data from S3 to redshift")
        hook.run("INSERT INTO {} {}".format(self.table, self.query))
