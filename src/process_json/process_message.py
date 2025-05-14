import json

def create_message(received_message, business_relation):
    """
    Monta e retorna uma mensagem enriquecida no formato JSON, 
    combinando dados do evento original (received_message) com informações de negócio (business_relation).

    Parâmetros:
        received_message (dict): Evento original recebido do Kafka.
        business_relation (list): Lista de tuplas com dados de negócio vindos do Postgres.

    Retorna:
        str: Mensagem enriquecida serializada em JSON.
    """
    file_send = {}
    # Dados originais do evento
    file_send["uuid"] = received_message["uuid"]
    file_send["country"] = received_message["country"]
    file_send["business_model"] = received_message["business_model"]
    file_send["company"] = received_message["company"]
    file_send["order_id"] = received_message["order_id"]
    file_send["order_calculation_date"] = received_message["order_calculation_date"]
    file_send["order_date"] = received_message["order_date"]
    file_send["order_date_release"] = received_message["order_date_release"]
    file_send["order_itens"] = received_message["order_itens"]
    file_send["person_status"] = received_message["person_status"]
    file_send["channel_id"] = received_message["channel_id"]
    file_send["order_value"] = received_message["order_value"]
    file_send["order_status"] = received_message["order_status"]
    file_send["products"] = received_message["products"]

    # Dados enriquecidos vindos do Postgres
    file_send["consultant_code"] = business_relation[0][0]
    file_send["structure_level"] = business_relation[0][1]
    file_send["structure_code"] = business_relation[0][2]
    file_send["business_status_code"] = business_relation[0][3]
    file_send["business_status_cycle"] = business_relation[0][4]
    file_send["person_id"] = str(business_relation[0][5]) if business_relation[0][5] else None

    return json.dumps(file_send)