# Etapa 8 — preparação e decisões

Data local:2026-10-11. Base:a26109e, checkout dedicado foundation. Usuário solicitou iniciar a etapa8 e reafirmou que o principal orquestra/revisa e agentes GPT-6 Luna Max implementam. Estado atual: tarefas 1 e 2 implementadas e aprovadas; tarefa 3 em execução. Os registros abaixo preservam o histórico de preparação.

## Exploração concluída

- Checkout limpo no início; etapa7 aceita. Escopo original confirmado: três tabelas de negócio, Flink→PostgreSQL e Flink→weather.alerts; consumer pertence à etapa9.
- Bootstrap existente cria usuário de aplicação não-super e schema climate, com CONNECT/USAGE; tabelas de negócio ainda não são definidas nos scripts do repositório. Volume existente exige migração explícita.
- Imagem Flink atual tem Kafka connector, sem JDBC/driverPostgreSQL. Compose não injeta credenciais do banco no Flink. Mudanças propostas serão específicas da integração nativa e usarão somente usuário de aplicação.
- Principal explorou código/grafo, contratos17/21, UIDs e catálogo3localidades. Agente read-only stage8_sink_design, GPT-6 Luna Max, analisou alternativas e riscos; sem implementação, alterações, testes, Docker ou commits pelo agente.
- Documentação oficial Flink1.20/JDBC/Kafka e PostgreSQL17 verificada. Proposta JDBCUPSERT/Kafkaat-least-once não declara atomicidade entre destinos. CTE comRETURNING liga métrica/alerta dentro de uma SQL, sem corrida deFK.

## Continuidade operacional

Serviços reiniciados antes desta preparação: Docker saudáveis, REST semjobs. Checkpoints40/41 da operação7 preservados. Principal retomou somente o grafo7, sem sourcealterado, usando `/opt/flink/checkpoints/3caa432342067923b7edae698657e8d6/chk-41/_metadata`.

Novo JobID cf62f651459bb421c75fd589955091ae RUNNING; REST confirmou restored41, completed44 e failed0. Nenhum volume, checkpoint, topic, tabela ou dado removido. Esse é o job7, não uma implementação antecipada da etapa8.

## Desenho para aprovação

Spec proposta: docs/superpowers/specs/2026-10-11-business-sinks-design.md. Define objetivo, três alternativas, política proposta HIGH/CRITICAL, identidade porlocal/janela/versão, contratoalerta22, schema3tabelas, privilégios, batching, tratamento de desconhecidos, limites de recuperação e critérios de QA.

Self-review do principal: contratos/campos coerentes; separação entre atomicidade PostgreSQL e Kafka; nenhum consumer/serviço adicional; QA anterior sem writes porpadrão; tabelas não guardam raw; escopo e decisões de produto explicitamente propostos. Os parâmetros exatos de conectores/driver e tarefas serão definidos no plano escrito após aprovação da spec, antes de instalação ou código de produto.

Próxima ação requerida: usuário revisar/aprovar a spec. Depois, principal escreve plano com interfaces/tarefas e execução por agentes LunaMax; aprovação do plano precede implementação conforme workflow arquitetural. Nenhum agente implementador foi despachado para código daetapa8.

## Spec aprovada e plano escrito

Usuário aprovou a spec em2026-10-11. Principal usou writing-plans e escreveu docs/superpowers/plans/2026-10-11-business-sinks.md. Três gates: contrato/migração PostgreSQL; sinks nativos/config/wiring; integração/replay/migração/faultQA. Implementação permanece nãoiniciada, método preservado agentesLunaMax.

Inspeção read-only da API real confirmou JdbcSink.sink com reflexão createRowJdbcStatementBuilder e builder JDBC aninhado; serializersKafka padrão recebem registro inteiro, helper de coluna nativo somente segundo campo. Plano inclui pequeno serializerJavaRow2 exclusivamente para keylocation/valueJSON22, semtransportepróprio. JDKsomente builder, runtimeinalterado. JDBC3.3.0-1.20/pgJDBC42.7.14 fixados, hashesverificados na tarefa de build. Credenciais nuncaimpressas; nenhumproduto/dependência instalado.

Self-review: oito critérios da spec têm owners/QA; cincoReviewFocus comtestes definidos; RowJDBC2parâmetros/RowKafka2campos/schemamétrica19colunasdados/alertpayload22 coerentes; divergência mesmaidentidade recusa transacionalmente paraevitaralertstale. Captura do grafo7 eQAslotúnico precedem deploy8. Faults usamconfig somente dejobQA próprio, sempararserviçoscompartilhados. Semtarefas paralelas cominterfacesdependentes; semconsumer9.

Próximaação: revisar/aprovar plano; não pedir escolha de modelo/método novamente, já definidos pelo usuário.

Timeoutdevalidação JDBC explicitado5s via builderJava público, alémconnect/socket10s e retries3; evita default60s por tentativa no orçamentoCP120s. NativeRowSerializer inspecionado, SDKwrappedcustomschema aceita instânciaJava; nenhuma instalação/função deproduto executada. Plano self-review concluído, semgaps/TODO.

## Execução autorizada

Usuário aprovou o plano em2026-10-11. Task1 delegada aoagente stage8_records_impl GPT-6 Luna Max, TDDantesprodução. Ledgerdeexecução específico:stage-8-sdd-progress.md. Legacy7QAe0fc5a701089bc01b09f8de036b5f6b5/CP2 capturado antesnovo grafo; backupimagem/metadata/sourcehashes preservados. Op7temporária f50cfe22e4aaf194d1eea1cb55c0d84b retomada. Baseline482/SDK40/Ruffverificados pelo principal; Task2/3 não iniciadas.


## Estado atual — tarefas 1 e 2 aprovadas

Task1 commit a88fb65: contrato puro, SQL idempotente e schema PostgreSQL; testes reais de migração/replay/roles/cold-init aprovados. Task2 commit982f485: sinks JDBC/Kafka nativos, configuração e wiring; SDK83, suíte geral595/Ruff/build aprovados, nós saudáveis. Revisão independente de cada tarefa sem P1/P2 pendentes. Task3 delegada a stage8_delivery_impl GPT-6 Luna Max: provas de entrega real, replay, migração7→8 e falhas. Principal coordena o slot único e QA. Operação7 preservada em CP164; operação8 será restaurada no aceite final. Etapa8 ainda não concluída; etapa9 não iniciada.
