def get_business_relation(received_message, session):
    """
    Consulta informações de negócio (business relation) no banco Postgres
    com base nos dados do evento recebido.

    Parâmetros:
        received_message (dict): Evento original recebido do Kafka.
        session: Conexão ou sessão ativa com o banco Postgres.

    Retorna:
        list: Lista de tuplas com os dados de negócio encontrados.
    """
    con = session
    cur = con.cursor()

    data_message = received_message["consultant_code"]
    # Validação: se o código do consultor for uma lista, lança exceção (caso de código inexistente)
    if isinstance(data_message, list): 
        raise Exception(
            f"ConsultantCode Inexistente: {received_message['consultant_code']}.")

    country = received_message["country"]

    # Monta a query SQL para buscar a relação de negócio mais recente do consultor    
    query = f"""
        SELECT  person_code,
                structure_level,
                structure_code,
                business_status_code,
                cycle, 
                person_uid as person_id
        FROM (SELECT DISTINCT   bp.person_code,
                                br.structure_level,
                                br.structure_code, 
                                br.country, 
                                br.company, 
                                br.created_at, 
                                br.business_status_code, 
                                br."cycle",
                                bp.person_uid,
                                rank() over (partition by bp.person_code order by br.created_at desc) rank 
                FROM postgres.business_relation br, postgres.person bp
                WHERE bp.person_uid = br.person_uid
                    AND br.structure_code is not null
                    AND br.country = {country}
                    AND br.company = 1
                    AND br.business_model = 1
                    AND br.function = 1
                    AND br.role = 1
                    AND bp.person_code in ({data_message})
                    ) actual_structure
                WHERE rank = 1 
                AND business_status_code in (3,2)
    """

    cur.execute(query)

    result = cur.fetchall()

    return result
