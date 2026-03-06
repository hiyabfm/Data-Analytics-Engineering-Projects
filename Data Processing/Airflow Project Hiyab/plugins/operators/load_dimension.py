from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """
    Loads data into dimension tables in Redshift.
    Supports two modes:
      - 'append': insert new data without deleting existing records.
      - 'delete-load': clear the table before inserting new data.
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql_query='',
                 mode='append',  # new flexible mode
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.mode = mode.lower()  # ensure lowercase for consistency

    def execute(self, context):
        self.log.info(f'Loading data into dimension table {self.table} in {self.mode.upper()} mode')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Clear data if mode is delete-load
        if self.mode == 'delete-load':
            self.log.info(f'Deleting existing data from {self.table} before load')
            redshift.run(f'DELETE FROM {self.table}')
        elif self.mode == 'truncate':
            self.log.info(f'Truncating {self.table} before load')
            redshift.run(f'TRUNCATE TABLE {self.table}')
        else:
            self.log.info(f'Appending new data to {self.table} (no deletion performed)')

        # Insert new data
        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query};
        """
        self.log.info(f'Executing INSERT statement for table {self.table}')
        redshift.run(insert_sql)
        self.log.info(f'Successfully loaded data into dimension table {self.table}')
