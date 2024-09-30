import requests
import json
import mysql.connector
from sshtunnel import SSHTunnelForwarder
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.value_provider import RuntimeValueProvider

class MyPipelineOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        # Add all necessary options for the pipeline
        parser.add_value_provider_argument('--ssh_host', type=str, help='SSH host')
        parser.add_value_provider_argument('--ssh_user', type=str, help='SSH user')
        parser.add_value_provider_argument('--ssh_key', type=str, help='Path to SSH private key')
        parser.add_value_provider_argument('--mysql_db_host', type=str, help='MySQL database host')
        parser.add_value_provider_argument('--mysql_db_user', type=str, help='MySQL database user')
        parser.add_value_provider_argument('--mysql_db_password', type=str, help='MySQL database password')
        parser.add_value_provider_argument('--mysql_db_name', type=str, help='MySQL database name')
        parser.add_value_provider_argument('--api_url', type=str, help='GraphQL API URL')
        parser.add_value_provider_argument('--api_token', type=str, help='API Authorization token')
        parser.add_value_provider_argument('--query_limit', type=int, default=1000, help='Query limit')
        parser.add_value_provider_argument('--query_offset', type=int, default=0, help='Query offset')

# Establish SSH Tunnel
def open_ssh_tunnel(ssh_host, ssh_user, ssh_key, db_host, db_port):
    tunnel = SSHTunnelForwarder(
        (ssh_host, 22),
        ssh_username=ssh_user,
        ssh_pkey=ssh_key,
        remote_bind_address=(db_host, db_port)
    )
    tunnel.start()
    return tunnel

# Insert data into MySQL
def insert_data_to_mysql(nim, nama_program_studi, cursor):
    insert_query = """
    INSERT IGNORE INTO students (nim, nama_program_studi)
    VALUES (%s, %s)
    """
    cursor.execute(insert_query, (nim, nama_program_studi))

class FetchGraphQLData(beam.DoFn):
    def __init__(self, api_url, api_token, query_limit):
        self.api_url = api_url
        self.api_token = api_token
        self.query_limit = query_limit

    def process(self, element):
        offset = element['offset']
        headers = {
            'content-type': 'application/json',
            'Authorization': self.api_token.get()
        }
        query = """
        query ExampleQuery($where: [_Where], $limit: Int, $offset: Int) {
          data(where: $where, limit: $limit, offset: $offset) {
            nim
            nama_program_studi
          }
        }
        """
        variables = {
            "where": [{"conditions": "Aktif", "field": "nama_status_mahasiswa"}],
            "limit": self.query_limit.get(),
            "offset": offset
        }

        while True:
            payload = {"query": query, "variables": variables}
            response = requests.post(self.api_url.get(), headers=headers, json=payload)

            if response.status_code != 200:
                print(f"Request failed with status code {response.status_code}")
                break

            data = response.json()
            result_data = data.get('data', {}).get('data', [])

            if not result_data:
                break

            for item in result_data:
                yield {'nim': item['nim'], 'nama_program_studi': item['nama_program_studi']}

            variables['offset'] += self.query_limit.get()

class InsertToMySQL(beam.DoFn):
    def __init__(self, ssh_host, ssh_user, ssh_key, db_host, db_user, db_password, db_name):
        super().__init__()
        self.ssh_host = ssh_host
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key
        self.db_host = db_host
        self.db_user = db_user
        self.db_password = db_password
        self.db_name = db_name
        self.tunnel = None
        self.conn = None
        self.cursor = None

    def start_bundle(self):
        # Open SSH tunnel and MySQL connection at the start of each bundle
        self.tunnel = open_ssh_tunnel(
            self.ssh_host.get(),
            self.ssh_user.get(),
            self.ssh_key.get(),
            self.db_host.get(),
            3306
        )
        self.conn = mysql.connector.connect(
            host='127.0.0.1',
            port=self.tunnel.local_bind_port,
            user=self.db_user.get(),
            password=self.db_password.get(),
            database=self.db_name.get()
        )
        self.cursor = self.conn.cursor()

    def process(self, element):
        # Insert data into MySQL
        nim = element['nim']
        nama_program_studi = element['nama_program_studi']
        insert_data_to_mysql(nim, nama_program_studi, self.cursor)
        self.conn.commit()

    def finish_bundle(self):
        # Close MySQL and SSH connection
        self.cursor.close()
        self.conn.close()
        self.tunnel.stop()

def run():
    # Get pipeline options
    pipeline_options = MyPipelineOptions()

    # Initialize the pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        # Create a PCollection for the initial offset
        initial_offset = p | 'Create initial offset' >> beam.Create([{'offset': pipeline_options.query_offset}])

        # Fetch data from the GraphQL API
        data = (initial_offset 
                | 'Fetch data from GraphQL' >> beam.ParDo(
                    FetchGraphQLData(
                        pipeline_options.api_url,
                        pipeline_options.api_token,
                        pipeline_options.query_limit
                    )
                ))

        # Insert fetched data into MySQL
        data | 'Insert into MySQL' >> beam.ParDo(
            InsertToMySQL(
                pipeline_options.ssh_host,
                pipeline_options.ssh_user,
                pipeline_options.ssh_key,
                pipeline_options.mysql_db_host,
                pipeline_options.mysql_db_user,
                pipeline_options.mysql_db_password,
                pipeline_options.mysql_db_name
            )
        )

if __name__ == '__main__':
    run()
