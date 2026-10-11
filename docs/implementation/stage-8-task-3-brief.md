# Etapa 8 — Business Sinks Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persistir avaliações de janela e publicar alertas HIGH/CRITICAL, comprovando replay idempotente e recuperação dos sinks reais.

**Architecture:** Contrato puro compartilhado, uma SQL parametrizada com CTE/RETURNING para métricas e alertas no PostgreSQL, e KafkaSink nativo separado. Preservar o grafo stateful e acrescentar os destinos após o risco. Dois pequenos helpers Java adaptam Row(key,payload) para ProducerRecord e Row(payload,idnullable) para o statement JDBC público, sem implementar transporte próprio.

**Tech Stack:** Python3.11.14/PyFlink1.20.3, Java17, Kafka3.9.1, PostgreSQL17.11, JDBC connector3.3.0-1.20, pgJDBC42.7.14, Pytest8.4.2/Ruff0.14.0 e Compose existentes.

**Spec:** docs/superpowers/specs/2026-10-11-business-sinks-design.md — aprovada pelo usuário em2026-10-11.

## Global Constraints

- Principal orquestra, revisa e valida; implementadores GPT-6 Luna com esforço Max. Três tarefas sequenciais, cada uma com gate de revisão; não paralelizar tarefas dependentes.
- Mesmo checkout dedicado foundation conforme processo aprovado, commits locais sem push. Reavaliar isolamento com using-git-worktrees no preflight, sem criar infraestrutura concorrente.
- Persistir todas as classes; somente HIGH/CRITICAL (score>=50) geram alerta. Nenhum cooldown, consumer, serviço ou topic novo; etapa9 não iniciada.
- Avaliação21 preservada; alerta22 troca operation para weather_alert e acrescenta alert_id. Schema1/componentflink/rulesmvp-1. Identidade(loc,start_ms,end_ms,rules_version), hash SHA256 do JSON-list compacto UTF-8, ensure_ascii=False, sem sort de valores da lista.
- PostgreSQL UPSERT at-least-once, métrica/alerta da avaliação atômicos numa SQL; Kafka AT_LEAST_ONCE, keylocation_id. Não prometer exatamente uma mensagem nem atomicidade entre destinos.
- Preservar Row15, parser/qualidade, UIDs antigos, dedupTTL86400/cache0, janela30/5, watermark120s/idleness60s/lateness0, checkpoint60s/timeout120s/minpause10s/concorrência1/retenção2/restart3×10s e recursos atuais.
- Aplicação sem admin/DDL/DELETE; dimensão somente SELECT, métricas/alertas SELECT/INSERT/UPDATE. Secrets fora de URLs/CLI/logs/artefatos; senha em campo repr=False. JM/TM UID9999; somente credencial de aplicação no Flink.
- Principal controla Docker e fault tests. Nunca esperar silenciosamente por aprovação em subagente: enviar o comando/necessidade ao principal. Não apagar volumes/topics/checkpoints ou dados alheios.

## Review Focus

1. Replay com identidade igual: nenhuma linha adicional, nenhum alert_id diferente; avaliação divergente para a mesma identidade não deve deixar alerta stale ou ser ocultada.
2. Migração reaplicada sobre schema incompleto/incompatível: falhar e rollback, preservando dados e grants mínimos; não considerar IF NOT EXISTS uma validação de schema.
3. Native Kafka serializer: key precisa ser location_id e value JSON22, não Row serializada, JSON duplicado em ambos ou null key; comprovar no runtime Java real.
4. QA antiga/IDs desconhecidos/secrets: não escrever negócio em QA sem opt-in; desconhecidos continuam no diagnóstico; falha não pode imprimir senha/config sensível.
5. Falha após escrita externa mas antes de checkpoint: não confirmar entrega falsa; restauração do checkpoint anterior converge com UPSERT, mantendo dedup/offsets/acumuladores e slot coordenado.

## Task 3 — Provas reais de entrega e recuperação

**Files:** criar tests/integration/test_flink_business.py; usar helpers existentes em test_flink_temporal/test_kafka_flink/test_flink_risk e SQL/contrato Task1; criar docs/implementation/stage-8-implementation-report.md; README documenta execução e limites. Principal mantém controladores/manifestos/provas temporárias em .venv, depois versiona stage-8-acceptance.md e status.

**Interfaces QA:** holders guardam JobID antes de RESTwait; helpers de submissão/cancelamento aguardam terminal e só atuam em jobs próprios. Consulta psql JSON por chaves exatas, sem SELECTraw/secrets; observer Kafka one-off usa offsets capturados e prazo, filter por identidade exata. Não adicionar dependênciaPythonDB ao produto.

- [ ] Escrever testes/fixtures antes de executar o novo fluxo: Producer pausado em try/finally, QAunboundedlatest/audit/business/checkpoint15s. Quatro perfis em três localidades reais: LOWRJ/MEDIUMSP/HIGHCT no mesmo instanteT histórico exclusivo; cópia extremaLOW mesma identidade antes do avanço deve serduplicate; CRITICALRJ emT+60min e sentinelCTLOW emT+100min fecham as24 janelas dos perfis sem fechar as janelas do sentinel. Ingestion original coerente, semoverride de relógio/validação. Oracle independente dos módulos de risco/hash sob teste.
- [ ] Comparar todas as24 avaliações persistidas por identidade/colunas completas; somente12 alertasHIGH/CRITICAL nas tabelas/topic, JSON22 e keycorretos. LOW/MEDIUM semalertas. Poll sob deadline paraJDBCflush2s/SDKbundle eKafkaACK; não usar sleeps como prova. Conferir desconhecido nos diagnósticos/contador, sem linha/alerta.
- [ ] Replay nativo de checkpoint próprioC após buffersflush, com mesmos grupo/UID e offsets: id/payload persistem, linhasnãoaumentam. Se mensagemKafka repetir, id/payload/key iguais. Persistir prova RESTlatest.restored e novoCOMPLETED, metadata emambosnodes.
- [ ] Migrar MANIFESTOLEGADO7→8, novoJobID/mesmonomegrupo/metadata7, não allowNonRestoredState. Reenviar revisões dos dois eventos capturados: duplicateaudit; sentinelavança watermark e avaliações abertas alimentam negócios. Comparar métricas/risco/SQL/topic; confirmar estado/offsets/restored7 e novoCP8COMPLETED.
- [ ] Fault tests reais separados: QA com checkpoint60s, disparar manualmente checkpointC bom com mesmosUIDs; publicar inputposteriorC, fechar suas janelas, observar escrita externa e cancelar o job bom antes de snapshot seguinte, conservandoC. QAentrypoint usa build_pipeline e dataclasses.replace somente nas opções tipadas do próprio job (JDBCpublicurl postgres:1 ou writerKafka kafka:1, source continuaKafka9092), restaurandoC. Aplicarvalidadoresdeidentidade antesdesubmissão; sem novasflags/opções inseguras deprodução. Comprovar falha/restart/exceção do destino e nenhumcheckpointfalsamenteCOMPLETED após entrega pendente. Deadline240s porcaso; cancelar somentejobfault próprio/aguardarterminal, restaurar configuração boa peloC e exigirconvergência dos destinos/UPSERT sobre a escrita externa anterior. Erros nãoexpõem credenciais. Não parar infra compartilhada para induzir falha.
- [ ] Principal coordena regressõesKafka3,temporais3,risk2 comslotúnico e QAsemwrites; caso legado skipped por cancelamento ainda emprogresso recebe execuçãoisolada. Logs duráveis e contagens de PASS, sem contabilizar skips como sucesso.
- [ ] Focused/suítes gerais/SDK/Ruff aprovados, revisão independente dos testes/oracles/cleanup, correções porLunaMax; principal confirma runtimes/build/resultados antes de commit local `test: verify business sinks and checkpoint recovery`.


## Estado confirmado e rulings do principal

Tasks 1 e 2 concluídas e revisadas. Task1 commit a88fb65. Task2: 595 unit/general passed, 26 skips; SDK real83passed; Ruff/build PASS. Imagem atual ambosnós sha256:29ac52a48276e05d227e1f20b98f480f07e5653df6567b34b8dfa1277ca9e635. Nenhum job Flink ativo, Producer ativo, Kafka/Postgres saudáveis. Banco final com 3locations e nenhum fato business antes dos testes. Task1 migração e segurança já validadas, não refazer.

Só implementar Task3; NÃO executar Docker/rede/integrações reais nem commits. Root controla runtimes e credenciais. Faça primeiro fixtures/testes e avise RED-ready; não espere aprovação do usuário. Permissão para editar tests/integration/test_flink_business.py, helpers QA estritamente necessários, testes unit correspondentes, README e docs/implementation/stage-8-implementation-report.md. Nenhum produto deve ser alterado sem Root adjudicar um defeito concreto. Relato deve distinguir testes não executados dos aprovados.

Ruling: estender _submit_unbounded_qa_job existente com business_sinks:bool=False e python_entrypoint:str|None=None é permitido para testes QA, defaults preservam legado; entrypoint alternativo limitado a caminho /tmp/weather-flink-qa-* validado, nome/grupo QA sempre verificados, flag de negócio exigeaudit. Reusar holder-before-REST; não duplicar helper inteiro. Novo cleanup business cancela apenas próprio job e aguarda estado terminal antes de próximo caso. Regressões legadas não alterar assertions.

Root Python controlador .venv/flink-control/Scripts/python.exe; fixtures podem executar via host Python3.12, produto/SDK exatos Python3.11.14/PyFlink1.20.3. Rede REST127.0.0.1:8081. Containers rede climate-logistics_default. Um slot, jamais jobsconcorrentes. Testes novos opt-in por FLINK_BUSINESS_TEST=1 (documentar), não contar skips como PASS. Faulttests opt-in adicional se necessário, Root roda ambos. Case timeout240s conforme plano; observações bounded polling, sem sleepfixo como prova.

Legacy REAL7 manifesto .venv/stage8-legacy-checkpoint.json: origin_stage7, has_business_sinksFalse, JobID e0fc5a701089bc01b09f8de036b5f6b5, CP2 /opt/flink/checkpoints/e0fc5a701089bc01b09f8de036b5f6b5/chk-2/_metadata, nome/grupo/perfis no manifesto. Não sobrescrever. Histórico13:10Z 2020-01-01, RJHIGH vento40/SPLOWvento5, ingestion original igualevent; demaisvalores no manifesto. Ler só esse manifesto para teste migração real. NovoJobID mesmoQAname/group usandoimagem8, no allowNonRestoredState. RestoredCP7 deve constar REST +novoCOMPLETED8 legível ambosnós. Eventos2020 com ingestion2020 continuamválidos; não inventaroverride de relógio. Preencher métricas17/risk21/SQL19/alert22 por oracle independente.

Backup OP7 separado .venv/stage8-operational-before-build.json CP164 /opt/flink/checkpoints/f50cfe22e4aaf194d1eea1cb55c0d84b/chk-164/_metadata, reservado para aceitefinalRoot. NÃO consumir/modificar isso nos testesTask3. Imagem7 tag climate-logistics-flink:stage7-legacy preservada.

Task2 API ver código/reporte aprovado; BusinessSinkSettings frozen porttipado1..65535, SQLúnica2params, JavaBinderRow(payload21,idnullable); native JDBCbatch100/2000/3 e validation5/connect10/socket10; Java nativeKafkaBuilder ATLEASTONCE/writerdelivery30s/maxblock10s. business_sinks opcionalbuild_pipeline; CLI --qa-business-sinks requerqa-audit+nome/grupo exclusivos antescredentials. Fault entrypoint próprio deve reutilizar build_pipeline/validação/config e dataclasses.replace apenasport1 ou writerkafka:1, sourceKafka9092. Não criar flagsoperacionais/alterarCompose/pararinfra. Errosnão devem exporconfigsecreta.

Kafkaobserver one-off usa Producerimage/confluent_kafka existente, capturaoffsets de weather.alerts3partitions antesinput, consume boundedfilter identidadeexata. Semnovo serviço/dependency no produto. PG consultas app via dockercomposeexecPostgres bash envinterno/psql SQLstdin semimprimircredenciais; JSON por identidades completas. Cleanup administrativo apenasidentidadesfixtureown, FKalertsprimeiro; nunca wildcard deletes/volumes/topic cleanup. Producer pause in try/finally sempre restaura. CLI jobholder antesREST, cleanupatéterminalmesmosefalha.

Autoria prod anteriormente somenteagentesLunaMax, Root orquestra/revisa/QA. Não despachar outrosimplementadores ou revisores porconta própria. Enviar dúvidas técnicas concretas aoRoot, decisões rotineiras fazer e registrar, nenhuma pergunta ao usuário. Semrerunbaseline inútil/relerrelatórios completos. Objetivo: testes reais com oracle independente e artefatos de prova, seguidos revisãoRoot. Env .env nunca imprimir; secrets não entraramURLs/CLI/report.
