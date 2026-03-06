from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """
    Run data quality checks on Redshift tables.
    
    Parameters
    ----------
    redshift_conn_id : str
        Airflow connection ID for Redshift.
    test_cases : list[dict]
        List of dicts with keys:
            - 'check_sql': SQL query to execute.
            - 'expected_result': Expected value for comparison.
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 test_cases=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.test_cases = test_cases or []

    def execute(self, context):
        self.log.info('Starting data quality checks...')

        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failing_tests = []

        for test in self.test_cases:
            sql = test.get('check_sql')
            expected_result = test.get('expected_result')

            self.log.info(f'Running test: {sql}')
            records = redshift_hook.get_records(sql)

            if len(records) < 1 or len(records[0]) < 1:
                error_count += 1
                failing_tests.append(sql)
                self.log.error(f'Data quality check failed: {sql} returned no results')
                continue

            result = records[0][0]
            if result != expected_result:
                error_count += 1
                failing_tests.append(sql)
                self.log.error(f'Data quality check failed: {sql} — expected {expected_result}, got {result}')

        if error_count > 0:
            self.log.error(f'Data quality checks failed for {error_count} tests: {failing_tests}')
            raise ValueError('Data quality checks failed!')
        else:
            self.log.info('All data quality checks passed successfully!')
