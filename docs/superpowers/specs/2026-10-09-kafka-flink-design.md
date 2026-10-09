# Etapa 4 — Kafka → Flink

Status: especificação aprovada pelo usuário em 2026-10-09; plano aprovado; Etapa 4 implementada/verificada. Evidências em docs/progresso.md. Etapas 1–3 concluídas; repositório limpo na inspeção inicial. Containers estavam parados, sem alteração de dados nesta preparação.

## Objetivo e escopo

Executar o primeiro job PyFlink DataStream no cluster existente, consumindo weather.raw, interpretando o schema versão 1 e emitindo evidência diagnóstica de eventos processados. Implementador GPT-6 Luna Max, uma entrega coesa, revisão independente e testes pelo principal.

Preservar Producer, Kafka e PostgreSQL. Deduplicação/qualidade adicional entram na Etapa 5; Event Time, watermarks, estado e janelas na Etapa 6; Risk Engine e sinks de negócio nas Etapas 7–8. Não instalar JDBC ou criar topics novos nesta etapa.

## Runtime e compatibilidade

Manter Flink/PyFlink 1.20.3, Java 17 e Python 3.11. A documentação oficial aceita Python 3.11; o código existente usa datetime.UTC dessa versão.

Criar uma imagem derivada de flink:1.20.3-scala_2.12-java17 para JobManager e TaskManager, com o mesmo Python 3.11.14, apache-flink==1.20.3 e conector Kafka. Preferência: CPython compilado de fonte oficial verificada em build multi-stage, sem PPA e sem copiar um Python de outra distribuição com ABI incompatível. Build deve evitar PGO/LTO desnecessários, separar ferramentas de compilação do runtime e usar checksums fixados no plano.

Conector proposto: flink-sql-connector-kafka:3.3.0-1.20, JAR com dependências Kafka necessário ao KafkaSource, instalado no classpath uma única vez. Confirmar artefato/checksum e execução antes do aceite; não confiar apenas na existência do arquivo.

Dependências Flink em requirements-flink.txt, separadas de Requests/confluent-kafka do Producer e das ferramentas de desenvolvimento. Copiar o pacote climate_logistics para ambos os nós e configurar Python/PYTHONPATH/executáveis do cliente e worker. Processos continuam não-root quando suportado pelo entrypoint oficial.

Preservar slots/paralelismo 1 e os healthchecks. Orçamento inicial atual: JobManager 768 MiB/limite1 GiB; TaskManager 1280 MiB/limite1536 MiB. Medir o consumo com Python worker e só ajustar se necessário para execução real, documentando causa e medida.

## Fluxo e contrato

KafkaSource → SimpleStringSchema UTF-8 → parser puro → Row tipada → resumo diagnóstico → PrintSink.

Source: bootstrap kafka:9092, topic weather.raw, group.id=weather-flink-ingestion, offsets iniciais earliest, auto-commit desabilitado. Releitura entre submissões sem checkpoint é uma limitação explícita; nenhuma promessa de recuperação/exactly-once nesta etapa.

Usar WatermarkStrategy.no_watermarks() somente como requisito da API; não atribuir Event Time ou implementar política temporal agora.

Parser reusa WeatherEvent para campos/tipos/intervalo/version/source e converte timestamps ISO aware para UTC. Limite de mensagem16 KiB, JSON inválido/complexo e schema incompatível geram erro controlado, sem registrar payload completo. Erros conhecidos de parsing são contados/logados e descartados pelo wrapper para não derrubar o primeiro job. Não adicionar novas regras de negócio ou estado de deduplicação.

Row preserva todos os campos do schema da Etapa 2: coordenadas/medições como DOUBLE, weather_code/schema_version/measurement_interval_seconds como INT, identidade/source e timestamps UTC ISO como STRING. Definir ordem e nomes explícitos no plano; não usar inferência de tipos implícita.

A saída diagnóstica é JSON compacto com component=flink, operation=parse, status=success, location_id, event_timestamp, ingestion_timestamp e schema_version. É prova técnica temporária, sem PostgreSQL/Kafka Sink de negócio e sem despejar todos os eventos crus.

## Submissão e operação

Manter cluster de sessão. Após docker compose up -d, submeter explicitamente pelo CLI no JobManager e obter job ID. README terá comandos de submit/list/cancel; cancelar somente o job identificado, nunca jobs de terceiros automaticamente. Inspecionar jobs existentes antes de submeter e evitar múltiplas instâncias do job operacional.

Nome operacional: weather-kafka-ingestion; versão de teste usa nome exclusivo. A submissão manual é suficiente para a Etapa 4; automatizar o bootstrap final somente quando necessário ao fechamento do MVP.

Job operacional unbounded. Modo bounded reservado à integração: consumir um intervalo finito já publicado, terminar e liberar o slot. Cancelar/parar qualquer job operacional conhecido somente para permitir teste no único slot, restaurando-o ao final. Não apagar topics, registros, volumes ou offsets de outros grupos.

## Arquivos e delegação

- src/climate_logistics/weather_parser.py: parser puro e contrato de saída.
- src/climate_logistics/flink_job.py: KafkaSource, Row tipada, diagnóstico e execução.
- docker/flink/Dockerfile, requisitos Flink e checksums: runtime reproduzível.
- compose.yaml: mesma imagem customizada em JM/TM; configuração pública, sem secrets PostgreSQL.
- tests/unit/test_weather_parser.py e tests/integration/test_kafka_flink.py.
- README.md e docs/progresso.md: operação, evidências e limitações.

Uma tarefa para Luna Max; principal faz verificação de imagens/CLI/REST/logs, e um revisor independente verifica a mudança. Evitar alterações dos módulos anteriores salvo bug necessário.

## Testes e aceite

Testes offline do parser: fixture válida, todos os campos/tipos/timestamps, erro de JSON/schema/version, mensagem excessiva e ausência de exposição do corpo. Não duplicar toda a matriz de validação dos modelos existentes. Imports dos testes puros não exigem JVM/PyFlink instalado.

Integração opt-in em Flink real: publicar fixture identificável no Kafka existente, submeter job bounded com nome único, comprovar FINISHED via REST e encontrar resumo da fixture após parsing no TaskManager. Testar também um registro inválido seguido de válido, sem crash. Métrica de source sozinha não comprova que o Python parser executou.

Operação: job unbounded em RUNNING, consumir novo evento válido e demonstrar saída; confirmar source/operadores/slot via REST. Toda espera de validação tem deadline; falha no build, download, submissão ou Python worker deve ser reportada sem aceite falso.

Ruff, suíte existente e novos unitários, build da imagem e integração real aprovados. Verificar que Producer continua funcional e o job operacional fica ativo ao final. Registrar ETAPA 4 CONCLUÍDA somente então; Etapa 5 não iniciada.

## Fontes primárias

- https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/python/installation/
- https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/
- https://central.sonatype.com/artifact/org.apache.flink/flink-sql-connector-kafka/3.3.0-1.20
