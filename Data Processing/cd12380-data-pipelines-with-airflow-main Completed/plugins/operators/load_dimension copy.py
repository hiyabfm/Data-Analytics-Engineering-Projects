from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Loads data into dimension tables in Redshift.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql_query='',
                 truncate_before_load=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        self.log.info(f'Loading data into dimension table {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_load:
            self.log.info(f'Truncating dimension table {self.table} before loading')
            redshift.run(f'TRUNCATE TABLE {self.table}')

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query};
        """
        self.log.info(f'Executing INSERT for dimension table {self.table}')
        redshift.run(insert_sql)
        self.log.info(f'Successfully loaded data into dimension table {self.table}')
