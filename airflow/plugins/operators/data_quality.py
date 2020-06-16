from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Copies data from S3 to Redshift staging tables.

    :param string  redshift_conn_id: reference to a specific redshift database
    :param list  checks: data quality SQL stmts

    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self
                , redshift_conn_id
                , checks
                , *args
                , **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.checks = checks

    def execute(self, context):
        self.log.info('DataQualityOperator begin execute')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
#        conn = self.hook.get_conn()
#        cursor = conn.cursor()
        self.log.info(f"Connected with {self.redshift_conn_id}")

        failed_tests = []
        for check in self.checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            if not sql :
                self.log.info(f"...[{exp_result}] {sql}")
 
#            result = redshift_hook.get_records(sql)[0]
 
#            if exp_result != result[0]:
#                failed_tests.append(sql)
 
        for check in self.checks:
            sql1 = check.get('dual_sql1')
            sql2 = check.get('dual_sql2')
            descr = check.get('descr')
            if not sql1:
                self.log.info(f"...[{descr}]\n  {sql1}\n  {sql2}")

#            result1 = cursor.get_records(sql1)[0]
#            result2 = cursor.get_records(sql2)[0]
 
#            if result1[0] != result2[0]:
#                failed_tests.append(f"Mismatch: {descr}\n  {sql1}\n  {sql2}")
 
        if len(failed_tests) > 0:
            self.log.info('Tests failed')
            self.log.info(failed_tests)
            raise ValueError('Data quality check failed')

        self.log.info("DataQualityOperator complete")
