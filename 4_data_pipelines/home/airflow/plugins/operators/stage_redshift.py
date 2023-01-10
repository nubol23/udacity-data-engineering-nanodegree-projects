from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 file_format="json",
                 json_path="auto",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_format = file_format        
        self.json_path = json_path
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        db_hook = PostgresHook(postgres_conn_id=self.conn_id)

        self.log.info("Remove existing data from table")
        db_hook.run("DELETE FROM {}".format(self.table))

        self.log.info("Load data from S3 to redshift")
        if self.file_format == "json":
            file_processing = "JSON '{}'".format(self.json_path)
        elif self.file_format == "csv":
            file_processing = (
                "IGNOREHEADER '{}' DELIMITER '{}'"
                .format(self.ignore_header, self.delimiter)
            )
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            {}
        """.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            file_processing
        )
        db_hook.run(formatted_sql)
