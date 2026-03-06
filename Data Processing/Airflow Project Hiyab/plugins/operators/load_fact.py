from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    Loads data into a fact table in Redshift from staging tables.
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 sql_query='',
                 append_mode=False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_query = sql_query
        self.append_mode = append_mode

    def execute(self, context):
        self.log.info(f'Loading data into fact table {self.table}')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_mode:
            self.log.info(f'Clearing data from fact table {self.table}')
            redshift.run(f'DELETE FROM {self.table}')

        insert_sql = f"""
            INSERT INTO {self.table}
            {self.sql_query};
        """
        self.log.info(f'Executing INSERT for fact table {self.table}')
        redshift.run(insert_sql)
        self.log.info(f'Successfully loaded data into fact table {self.table}')
