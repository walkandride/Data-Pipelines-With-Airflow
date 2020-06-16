from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    Copies data from S3 to Redshift staging table.

    :param string  redshift_conn_id: reference to a specific redshift database
    :param string  table_name: redshift staging table to load
    :param string  s3_bucket: S3 bucket location
    :param string  s3_path: S3 file path
    :param string  aws_credentials: AWS user credentials
    :param string  region: S3 bucket location

    """

    ui_color = '#358140'

    @apply_defaults
    def __init__(self
                , redshift_conn_id
                , table_name
                , s3_bucket
                , s3_path
                , aws_key
                , aws_secret 
                , region
                , *args
                , **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table_name = table_name
        self.s3_bucket = s3_bucket
        self.s3_path = s3_path
        self.aws_key = aws_key
        self.aws_secret = aws_secret
        self.region = region

    def execute(self, context):
        self.log.info('StageToRedshiftOperator begin execute')

        # connect to Redshift
        self.hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#        conn = self.hook.get_conn()
#        cursor = conn.cursor()
        self.log.info("Connected with " + self.redshift_conn_id)
 
        # build copy statement template
        COPY_STMT = """
            COPY {} 
                FROM '{}' 
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                REGION '{}'
                JSON 'auto'
                TIMEFORMAT as 'epochmillisecs'
                TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        """

        # build copy statement based upon parameters
        sql_stmt = COPY_STMT.format(
            self.table_name
            , 's3://' + self.s3_bucket + '/' + self.s3_path  # 's3://udacity-dend/log_data'
            , self.aws_key 
            , self.aws_secret
            , self.region 
        )
        self.log.info('copy sql: ' + sql_stmt)

#        cursor.execute(sql_statement)
#        cursor.close()
#        conn.commit()
        self.log.info("StageToRedshiftOperator copy complete - " + self.table_name)
