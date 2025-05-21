# AWS Lambda kafka-lambda-enrichment

## Visão Geral

Este projeto implementa um pipeline de dados em Python para processamento de eventos em tempo real, integrando AWS Lambda, Apache Kafka, PostgreSQL e ScyllaDB. 

O objetivo é consumir eventos de um tópico Kafka, enriquecer os dados com informações de negócio provenientes do PostgreSQL, persistir o histórico no ScyllaDB e encaminhar os eventos processados para novos tópicos Kafka, garantindo rastreabilidade e resiliência.

## Arquitetura

- **Kafka**: Utilizado como sistema de mensageria para ingestão e distribuição dos eventos.
- **PostgreSQL**: Fonte de dados para enriquecimento dos eventos, consultando informações de negócio.
- **ScyllaDB**: Banco NoSQL utilizado para armazenar o histórico dos eventos processados.
- **Python**: Linguagem principal do pipeline, responsável por orquestrar o fluxo, processar e enriquecer os dados.

## Fluxo do Pipeline

1. Consome eventos do Kafka.
2. Decodifica e valida os dados recebidos.
3. Enriquecimento dos eventos via consulta ao PostgreSQL.
4. Envia eventos processados para um novo tópico Kafka.
5. Persiste o histórico do processamento no ScyllaDB.
6. Em caso de erro, envia o evento para um tópico de falha e registra logs detalhados.

## Boas Práticas Adotadas

- **Organização modular**: Separação clara entre camadas de aplicação, domínio, integrações e utilitários.
- **Reuso de conexões**: Conexões com bancos e Kafka são criadas uma única vez e reutilizadas durante toda a execução, evitando overhead.
- **Fechamento seguro de conexões**: Uso do módulo `atexit` para garantir o fechamento das conexões ao encerrar o processo.
- **Tratamento de exceções**: Erros são tratados individualmente por mensagem, garantindo resiliência e rastreabilidade.
- **Validação de dados**: Estrutura dos eventos e campos obrigatórios são validados antes do processamento.
- **Logs estruturados**: Implementação de logs customizados para facilitar o monitoramento e troubleshooting.
- **Documentação e comentários**: Código amplamente comentado para facilitar manutenção e entendimento.
- **Atenção à segurança**: Recomenda-se o uso de queries parametrizadas para evitar SQL Injection.

## Como Executar

1. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```
2. Configure as variáveis de ambiente necessárias (Kafka, PostgreSQL, ScyllaDB, etc).
3. Execute o pipeline:
   ```bash
   python app.py
   ```

## Estrutura de Pastas

```
src/
├── application/      # Orquestração do pipeline
├── data/             # Integração e coleta de dados externos
├── json/             # Transformação e montagem de mensagens
├── connection/       # Conexão com bancos de dados
├── utils/            # Utilitários e logs
```

## Observações

- Este projeto foi desenvolvido para um caso de uso real, mas está estruturado para fácil adaptação a outros cenários.
- Para ambientes serverless (ex: AWS Lambda), o fechamento manual das conexões pode ser omitido.
- Recomenda-se revisar e adaptar as queries SQL para uso de parâmetros, aumentando a segurança.

---

**Dúvidas ou sugestões? Fique à vontade para abrir uma issue ou contribuir!**
