from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 table="",
                 query="",
                 mode="delete",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.table = table
        self.query = query
        self.mode = mode

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.log.info("Connected to redshift")
        
        if self.mode == "delete":
            self.log.info("Delete data on redshift")
            hook.run("TRUNCATE {}".format(self.table))
            
        self.log.info("Load data from S3 to redshift")
        hook.run("INSERT INTO {} {}".format(self.table, self.query))
