from json import loads
import boto3
import psycopg
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, TokenAwarePolicy
from cassandra.policies import RoundRobinPolicy

def PostgreSQL_DB(secret_postgres):
    """
    Cria e retorna uma conexão com o banco de dados PostgreSQL utilizando credenciais armazenadas no AWS Secrets Manager.

    Parâmetros:
        secret_postgres (str): Nome ou ARN do segredo no Secrets Manager contendo as credenciais do banco.

    Retorna:
        psycopg.Connection: Objeto de conexão com o PostgreSQL.
    """
    client = boto3.client("secretsmanager")
    response = client.get_secret_value(
        SecretId=secret_postgres)
    secret_string = loads(response["SecretString"])
    db_user = secret_string["username"]
    db_pass = secret_string["password"]
    db_host = secret_string["host"]
    db_name = secret_string["dbname"]
    db_port = secret_string["port"]
    
    try:
        # Estabelece a conexão com o banco PostgreSQL usando as credenciais obtidas
        postgres_conn = psycopg.connect(host=db_host,
                                   dbname=db_name,
                                   user=db_user,
                                   password=db_pass,
                                   port=db_port)

        return postgres_conn

    except Exception as e:
        raise Exception('Erro ao abrir a conexão:' + str(e))

def Scylla_DB(secret_scylla):
    """
    Cria e retorna uma conexão com o cluster ScyllaDB utilizando credenciais armazenadas no AWS Secrets Manager.

    Parâmetros:
        secret_scylla (str): Nome ou ARN do segredo no Secrets Manager contendo as credenciais do ScyllaDB.

    Retorna:
        cassandra.cluster.Session: Objeto de sessão conectado ao cluster ScyllaDB.
    """
    db_port = 9042

    try:
        # Busca as credenciais do ScyllaDB no Secrets Manager
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(
            SecretId=secret_scylla
        )

        secret_string = loads(response["SecretString"])
        db_user = secret_string.get("username")
        db_pass = secret_string.get("password")
        db_host = secret_string.get("host").split(",")
        
    except Exception as e:
        raise Exception('Erro na obtenção da secret do scylla: ' + str(e))

    try:
        # Estabelece a conexão com o cluster ScyllaDB usando as credenciais obtidas
        auth_provider = PlainTextAuthProvider(username=db_user, password=db_pass)
        cluster = Cluster(
            contact_points=db_host,
            port=db_port,
            auth_provider=auth_provider,
            load_balancing_policy=TokenAwarePolicy(RoundRobinPolicy()),
            protocol_version=4,
        )

        return cluster.connect()

    except Exception as e:
        raise Exception('Erro ao abrir a conexão com o scylla: ' + str(e))
