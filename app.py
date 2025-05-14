import json
import base64
from datetime import date, datetime
from os import getenv
from kafka import KafkaProducer
from src.data.data_collector import get_business_relation
from src.process_json.process_message import create_message
from src.connection.database import PostgreSQL_DB, Scylla_DB
from src.utils.logger import Logger
import atexit

# Inicialização do logger customizado para rastreabilidade
logger = Logger("app.py")
from src.utils.log_manager import setup_global_logger, set_log_context

# Configuração global do logger para todo o projeto
setup_global_logger()

# Carregamento de variáveis de ambiente para parametrização do pipeline
env = getenv("LAMBDA_ENV")
lambda_name = getenv("LAMBDA_NAME")
error_message = {"lambda": lambda_name}
topic_send = "topic-success-process"
topic_send_fail = 'topic-failure-process'

# Inicialização do produtor Kafka com servidores definidos por variável de ambiente
msk_servers = getenv("MSK_SERVERS").split(",")
producer = KafkaProducer(bootstrap_servers=msk_servers)

# Conexões globais e reutilizáveis com Postgres e ScyllaDB, evitando overhead de reconexão a cada evento
secret_postgres = f"aws-account-henrique/{env}/postgres/database/postgres/username/postgres_user"
postgres_connection = PostgreSQL_DB(secret_postgres)

secret_scylla = f"aws-account-henrique/{env}/scylla/database/keyspace/username/keyspace_user"
scylla_connection = Scylla_DB(secret_scylla)

def handler(event, _):
    """
    Função principal do pipeline: processa eventos recebidos do Kafka,
    enriquece com dados do Postgres, envia para outro tópico Kafka e registra histórico no ScyllaDB.
    """
    set_log_context(event_source="MSK")
    now = datetime.now()
    date_time = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    # Validação da estrutura do evento recebido
    records = event.get("records")
    if records is None:
        logger.error("[ERROR] 'records' is None. Event data might be malformed or missing.")
        return

    partitions = records.keys()

    list_records = []
    # Decodificação dos dados recebidos em base64
    for partition in partitions:
        for record in records[partition]:
            try:
                decoded_value = base64.b64decode(record.get("value"))
                list_records.append(decoded_value)
            except TypeError as e:
                logger.error(f"[ERROR] Failed to decode base64 value: {e}. Record: {record}")
                continue  # Pula registros malformados

    # Processamento individual de cada mensagem decodificada
    for received_message in list_records:
        try:
            received_message = json.loads(received_message)
            logger.info(f"receivedmessage: {received_message['order_id']}")

            # Enriquecimento do evento com dados de negócio vindos do Postgres
            business_relation = get_business_relation(received_message, postgres_connection)

            if len(business_relation) == 0:
                logger.warning(f"ConsultantCode Inexistente: {received_message['consultant_code']}.")
                continue  # Pula eventos sem relação de negócio

            # Criação da mensagem enriquecida e envio para o tópico Kafka de sucesso
            send_json = create_message(received_message, business_relation)
            send_message = str.encode(send_json)
            producer.send(topic_send, send_message).get(10)

            # Registro do histórico do processamento no ScyllaDB
            engine_id = 1
            history_date = date.today()
            hour_range = now.hour
            uuid = json.loads(send_json)["uuid"]
            last_update = date_time
            payload = send_json

            # Atenção: para produção, prefira queries parametrizadas para evitar SQL Injection
            values = f"{engine_id},'{history_date}',{hour_range},'{uuid}','{last_update}','{payload}'"
            insert = f"INSERT INTO keyspace.message_history (engine_id, history_date, hour_range, uuid, last_update, payload) VALUES({values});"
            scylla_connection.execute(insert)

        except Exception as e:
            # Em caso de erro, envia o evento para o tópico de falha e registra o erro
            error_message["eventProcessorError"] = {"message": str(e)}
            error_message["payloadEvent"] = str(received_message)
            producer.send(topic_send_fail, str.encode(str(json.dumps(error_message)))).get(10)
            logger.error(f"Failed: {e}")

def close_connections():
    """
    Função registrada para ser executada ao final do processo Python,
    garantindo o fechamento das conexões com os bancos de dados.
    """
    try:
        postgres_connection.close()
        logger.info("Conexão com Postgres fechada.")
    except Exception as e:
        logger.error(f"Erro ao fechar conexão Postgres: {e}")
    try:
        scylla_connection.close()
        logger.info("Conexão com ScyllaDB fechada.")
    except Exception as e:
        logger.error(f"Erro ao fechar conexão ScyllaDB: {e}")

# Garante que as conexões serão fechadas ao encerrar o processo (não necessário em ambientes serverless)
atexit.register(close_connections)