# Etapa 3 — Python Producer + Kafka

Status: especificação aprovada pelo usuário em 2026-10-08; plano e implementação pendentes. Etapas 1 e 2 concluídas; Git limpo na inspeção inicial.

## Objetivo e escopo

Coletar os eventos normalizados da Etapa 2 e publicá-los corretamente em weather.raw. Adicionar o container producer, mantendo Kafka/Flink/PostgreSQL separados. Não implementar job Flink, Risk Engine, persistência de negócio ou alert-consumer nesta etapa.

Implementadores continuam GPT-6 Luna Max. Para reduzir coordenação, uma entrega coesa de implementação será revisada e validada pelo principal e por um revisor independente; testes existentes permanecem.

## Contrato e topics

Reutilizar OpenMeteoClient, LOCATIONS e WeatherEvent sem mudar o schema da Etapa 2. Valor Kafka: JSON UTF-8 compacto, allow_nan=False, com limite de 16 KiB. Chave: location_id em UTF-8. event_timestamp e ingestion_timestamp preservados; metadado CreateTime da mensagem usa ingestion_timestamp em milissegundos.

Identificação lógica para logs: location_id + event_timestamp UTC. A idempotência do cliente Kafka reduz duplicações de retries na mesma sessão; não garante deduplicação de negócio entre polls ou reinícios. Isso será tratado na Etapa 5.

Topics mínimos: weather.raw e weather.alerts, ambos com três partições e replicação 1, conforme documentação existente. weather.alerts fica vazio nesta etapa. Criar explicitamente no startup com AdminClient, sem serviço/container auxiliar e com timeout total de 15 segundos. Se já existem, conferir partições/replicação; falhar claramente em incompatibilidade, sem recriar, excluir ou alterar partições automaticamente.

Retenção permanece a política atual do broker: 24 horas, 1 GiB por partição e segmentos de 256 MiB. Criação automática continua desabilitada.

## Producer e execução

Usar o cliente confluent-kafka, versão fixada no plano e verificada em Python 3.11. É uma biblioteca cliente, sem adicionar plataforma Confluent, Schema Registry ou formatos novos.

CLI: python -m climate_logistics.producer, com modo contínuo e --once para executar um ciclo verificável. Cada ciclo consulta sequencialmente RJ01/SP01/CT01 usando a sessão HTTP existente, enfileira eventos válidos, atende callbacks e confirma entregas com flush de até 12 segundos. Falha HTTP de uma localidade não impede a coleta das demais; não publicar dados fabricados.

Intervalo padrão entre ciclos: 300 segundos. Configuração validada via ambiente: KAFKA_BOOTSTRAP_SERVERS=kafka:9092 e WEATHER_POLL_INTERVAL_SECONDS=300. Sem senha PostgreSQL no ambiente do Producer e sem dump de configuração/variáveis nos logs.

Configuração Kafka inicial: enable.idempotence=true, acks=all, compression.type=gzip, linger.ms=20, batch.size=32768 e delivery.timeout.ms=10000. Começar com uma instância; nenhuma otimização adicional sem medida.

Não considerar produce() como confirmação de entrega. on_delivery deve registrar sucesso/falha e ser atendido por poll()/flush(). Queue cheia: atender callbacks e repetir enqueue com prazo total de 5 segundos; não acumular estado indefinidamente nem criar loop infinito. Falha definitiva de Kafka ou mensagens pendentes após flush resulta em erro explícito e saída não zero. Evitar reenvio manual cego após timeout ambíguo.

--once sai com zero apenas se todas as localidades tiverem evento válido e confirmação de entrega; um ciclo parcial tem saída não zero e logs claros. Modo contínuo pode continuar após falha isolada HTTP no próximo ciclo normal, sem retries adicionais aos já limitados no cliente. Falha Kafka encerra o processo.

SIGTERM/SIGINT solicita parada: interromper a espera/ciclos novos, fechar sessão HTTP e fazer flush final com timeout de 12 segundos. Documentar que requisição HTTP em andamento respeita os timeouts existentes, sem garantia de deadline total absoluto.

## Docker e logging

Imagem derivada Python 3.11 slim com tag explícita; instalar apenas dependências runtime, copiar src e executar como usuário não-root. Producer depende do Kafka saudável, sem publicar porta, sem .env copiado para imagem e sem acesso a secrets PostgreSQL. Reinícios por falha limitados a três; stop_grace_period de 75 segundos para requisição em andamento e flush limitado.

Logging JSON com timestamp UTC, component, operation, status e, quando necessário, location_id/event_id/error. Registrar resumo por ciclo e falhas; sucesso de enqueue não deve aparecer como entregue. Sem credenciais, corpo HTTP inteiro ou eventos completos repetidamente.

## Arquivos previstos

- src/climate_logistics/producer.py: CLI/ciclo/configuração e encerramento.
- src/climate_logistics/kafka_publisher.py: serialização, topics e confirmação de entrega.
- docker/producer/Dockerfile, compose.yaml e .env.example.
- requirements.txt: adicionar somente o cliente Kafka fixado.
- tests/unit/test_producer.py e test_kafka_publisher.py.
- tests/integration/test_producer_kafka.py: validação real opt-in.
- README.md e docs/progresso.md: operação, evidências e limitações.

Sem alterar modelos/client HTTP aprovados salvo bug concreto necessário.

## Testes e aceite

TDD e suíte offline: chave/JSON/timestamps, config inválida, callbacks de sucesso/erro, fila cheia, timeout/pending no flush, topics já existentes/incompatíveis, falha isolada de coleta e parada. Reusar fixture existente; não repetir casos de validação da Etapa 2.

Integração Kafka real opt-in, dentro da rede Docker: publicar fixture determinística e consumir registro para verificar chave, valor, timestamps e entrega. Usar os topics previstos, sem criar topics extras ou remover dados existentes. Isolar o teste com identificador único no evento/tempo do fixture e consumidor temporário com offsets controlados; não resetar grupos operacionais.

Smoke ponta a ponta com API real: executar Producer --once e comprovar os eventos desse ciclo em weather.raw por consumo de validação. Criar topics idempotentemente, verificar partições, buildar imagem e subir producer no Compose. Se a fonte falhar, preservar evidências e reportar pendência; fixture sozinha não comprova coleta real.

Ruff, suíte inteira e Docker build devem passar. Aprovar somente após dados chegarem corretamente ao Kafka, erros de entrega serem visíveis, nenhum secret versionado e revisão sem achados impeditivos. Registrar ETAPA 3 CONCLUÍDA; a Etapa 4 fica fora desta especificação.

## Fonte primária consultada

https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html — Producer, AdminClient, callbacks, poll e flush.