# Progresso — Climate Logistics Streaming Platform

Plano: `docs/superpowers/plans/2026-10-07-fundacao.md`

## Estado

- Etapa 1: concluída; implementação, revisão e critérios de infraestrutura aprovados em 2026-10-07.
- Etapa 2: concluída; critérios e revisão aprovados em 2026-10-07.
- Etapa 3: concluída; revisão e critérios aprovados em 2026-10-08.
- Etapa 4: concluída; critérios/revisão aprovados em 2026-10-09.
- Etapas 5–10: pendentes.
- Implementadores: GPT-6 Luna, raciocínio Max.
- Git inicializado na branch `foundation`.
- Docker Desktop confirmado: engine Linux/amd64 acessível com permissão fora do sandbox.

## Revisão prévia do plano

| Tarefas | Contrato revisado | Resultado |
| --- | --- | --- |
| 1 e 2 | Nomes de serviços, variáveis e schema consumidos pelo README | Compatíveis; tarefa 2 após estabilização da infraestrutura |
| 1 | Arquivos infra, secrets obrigatórios, healthchecks e checks reais | Coerentes |
| 2 | Estrutura sem lógica futura, Ruff e ausência honesta de casos Pytest | Coerentes |
| Validação final | Prontidão, login restrito, persistência e reinício | Aceite condicionado à execução real |

## Decisões de execução

- Ruling: usar o diretório dedicado inicial e branch foundation — plano aprovado prevê Git novo sem checkout redundante — custo se inadequado: isolamento adicional posterior.
- Ruling: armazenar briefs e relatórios em docs/implementation no Windows — manter trilha legível sem depender de scripts Bash auxiliares — custo: arquivos adicionais de documentação.

## Evidências

- `docker version`: exit 0; engine Linux/amd64.
- `git init -b foundation`: exit 0.
- Primeira inicialização: Kafka/PostgreSQL/JobManager saudáveis; TaskManager falhou por orçamento de memória insuficiente.
- Correção pelo agente infra: TaskManager processo 1280 MiB, container 1536 MiB. Revisor independente confirmou P1 resolvido.
- `docker compose up -d --wait --wait-timeout 180`: exit 0, quatro serviços saudáveis.
- Kafka `kafka-broker-api-versions.sh --bootstrap-server kafka:9092`: exit 0, broker id 1 respondeu.
- Flink REST `/taskmanagers`: exatamente um TaskManager e um slot livre, antes e depois do reinício.
- Script `verify-postgres.sh`: login TCP autenticado, privilégios administrativos ausentes, USAGE em climate e CREATE negado em climate/public.
- Script `verify-persistence.sh before/after`: marcador persistiu após reinício; tabela/schema técnicos removidos após teste.
- `docker compose restart` seguido de `up -d --wait`: exit 0, recuperação dos quatro serviços.
- Bootstrap com senha contendo aspas simples/duplas, espaços, ponto e vírgula, barra e dólar: login/privilégios passaram em projeto isolado `climate-foundation-verification`.
- PostgreSQL de validação parado; volume isolado preservado. Nenhum volume foi removido.
- `docker compose --env-file .env.example config --quiet`: exit 1 por senhas vazias; `docker compose config --quiet` com `.env` local: exit 0.
- `.env` confirmado ignorado pelo Git; scripts shell com finais LF.
- Ruff 0.14.0 em Python 3.11.14: `All checks passed!`.
- Pytest 8.4.2: 0 casos coletados, exit 5 confirmado; ausência de suíte nesta etapa explicitamente documentada, não reportada como testes aprovados.
- Consumo observado sem jobs: Kafka ~324 MiB, JobManager ~203 MiB, TaskManager ~310 MiB, PostgreSQL ~23 MiB. São medições de fundação ociosa, sem inferência sobre throughput.
- Processos JVM do JobManager/TaskManager rodam como usuário `flink`; Kafka como `appuser`.
- Kafka `meta.properties` mantém cluster ID configurado após reinício.
- Revisão final encontrou P2 no comando psql do README: socket local não comprova senha. Implementador corrigiu para `-h 127.0.0.1` e explicitou autenticação TCP; checks reais já usavam TCP.
- Senha incorreta em psql/TCP: rejeitada por PostgreSQL, conforme esperado.
- P3 documental: retenção Kafka incluída no README (24 horas, 1 GiB por partição, segmentos 256 MiB, limpeza 5 minutos; sem alegar teto rígido de disco).
- Revisão final independente: P2/P3 resolvidos, nenhum achado remanescente.
- `.gitattributes` preserva LF em scripts shell nos checkouts Windows; linhas vazias extras corrigidas após `git diff --cached --check`.
- Verificação dos valores locais de credenciais nos arquivos preparados para versionamento: zero ocorrências; `.env` fora do índice.

## Tarefas

- Tarefa 1: implementada, revisada e verificada; P1 de memória corrigido.
- Tarefa 2: concluída; estrutura, exclusões, dependências fixadas e README revisados.
- Validação runtime: concluída; revisão final sem bloqueadores funcionais.

## Aceite da Etapa 1

Os quatro serviços permanecem saudáveis. O comando `docker compose up -d` subiu a infraestrutura; `--wait` comprovou prontidão com prazo limitado. Os testes operacionais descritos acima passaram; Pytest permanece sem casos e não é usado como evidência funcional desta etapa. Nenhum job, integração Open-Meteo, topic ou tabela de negócio foi antecipado.

Etapa 2 concluída. Ledger: docs/implementation/stage-2-progress.md.

## Etapa 2 — Open-Meteo concluída

Implementadores: GPT-6 Luna Max, tarefas sequenciais; revisão independente por contrato e final.

Implementado:
- Location/WeatherEvent imutáveis, localidades RJ01/SP01/CT01, validação direta e normalização pura.
- Schema versão 1 com source=open-meteo e measurement_interval_seconds, unidades fixas e timestamps UTC/Z.
- Cliente HTTP síncrono Requests 2.34.2: timeout de conexão/leitura de 3/10 s, até 3 tentativas, backoff de 0,5/1 s e Retry-After até 5 s.
- TLS verificado, redirects recusados, sessão própria sem configuração .netrc, respostas sempre fechadas e teto de 128 KiB descomprimidos.
- Testes unitários, integração HTTP real no loopback e smoke público opt-in; README atualizado.

Evidências verificadas pelo principal em Python 3.11.14:
- Primeira tarefa:245 testes passaram e Ruff aprovado após correção UTC.
- Suíte final offline:291 passed, 1 skipped, exit 0, em 5,84 s.
- Ruff final:All checks passed, exit 0.
- Smoke OPEN_METEO_LIVE=1: 1 passed, exit 0, em 0,77 s; evento RJ01 obtido da API e serializado conforme contrato.
- Revisão final: ambos P2 resolvidos, sem achados abertos.
- git diff --check sem erros antes do registro final.

Problemas e correções:
- Artefato de newline em teste: literal removido, todos os cenários preservados.
- Datetime aware cuja conversão UTC sai do domínio: construção rejeita WeatherValidationError; regressões red/green.
- JSON com inteiro com 5000 dígitos/aninhamento extremo:ValueError/RecursionError convertidos em OpenMeteoError, sem retry e com resposta fechada.
- README sem caminho de importação src: PYTHONPATH documentado em PowerShell/Linux.
- Importação fora de ordem apontada por Ruff: corrigida.

Limitações:
- Dados da fonte são modelados, não observações de estação; precipitação corresponde ao intervalo retornado.
- Timeouts de conexão/leitura não são deadline absoluto; Retry-After em formato de data usa backoff local.
- Python 3.11 é o ambiente de referência dos testes. Revisão de código não substitui a execução nessa versão.
- Nenhum polling, Producer, topic Kafka, job Flink, sink ou serviço novo foi antecipado.

Etapa 3 em execução. Ledger: docs/implementation/stage-3-progress.md.

## Etapa 3 — Python Producer + Kafka concluída

Implementação por GPT-6 Luna Max; revisão independente e validação pelo principal em 2026-10-08.

Implementado: publisher com confirmação de delivery, startup idempotente dos topics, CLI contínua/--once, logs JSON, container Producer não-root e configuração pública separada dos secrets PostgreSQL.

Evidências do principal:
- Python 3.11.14: 329 passed, 4 skipped em 6,02 s; Ruff aprovado.
- Kafka real: fixture, falha do SDK contra porta fechada e API→CLI→Kafka com consumo verificado: 3 passed em 13,30 s.
- weather.raw e weather.alerts: três partições, replicação 1, líder/ISR ativos em todas.
- Build final aprovado. Container coleta, aceita e confirma três eventos por ciclo.
- UID/GID 10001; nenhuma variável POSTGRES_/APP_DB_ no Producer; sem porta publicada.
- Parada SIGTERM: exit 0 em cerca de 1,06 s. Após reinício, topics existentes reconhecidos e novo ciclo de três entregas confirmado.
- weather.alerts permanece vazio: offsets 0 nas três partições.
- Memória observada do Producer: cerca de 25,5 MiB no limite de 256 MiB; medição de um ciclo pequeno, não benchmark de throughput.
- Revisão scoped: cinco achados resolvidos, sem pendências.

Problemas corrigidos: validação de metadata dos topics existentes; logs INFO da CLI; comando Docker --once; retry depois do deadline; contagem de ACKs em lote parcialmente falho. Observer de testes passou a atualizar metadata/repetir somente erros transitórios sob deadline de 15 s após erro NOT_LEADER_FOR_PARTITION no primeiro startup.

Limites: idempotência Kafka dentro da sessão não elimina duplicações de negócio entre polls/reinícios; deduplicação na Etapa 5. Dados meteorológicos modelados e intervalo da precipitação preservados. Sem HA/TLS/SASL/ACL no broker local.

Producer contínuo deixado em execução. Nenhum volume, topic ou registro foi removido. Próxima etapa: Etapa 4 — Kafka → Flink, ainda não iniciada.

## Etapa 4 — Kafka → Flink concluída

Implementação GPT-6 Luna Max; revisão independente e validação pelo principal em 2026-10-09.

Implementado:
- Runtime comum JM/TM: Flink/PyFlink 1.20.3, Java17, CPython3.11.14 compilado de fonte verificada e conector Kafka3.3.0-1.20 com SHA256 fixado.
- KafkaSource weather.raw, earliest, auto-commit false, paralelismo1/no-watermarks; parser puro e Row tipada com15 campos.
- Diagnóstico JSON/PrintSink e métricas parsed_records/invalid_records, sem payload completo em erros.
- Job bounded isolado de QA e unbounded operacional; CLI/list/cancel documentados.

Evidências do principal:
- Build aprovado, SSL/ctypes/sqlite/bz2/lzma e PyFlink importam nos dois nós; versões3.11.14/1.20.3 confirmadas.
- Suíte leve CPython3.11.14:346 passed,6 skipped em6,30s; Ruff aprovado.
- Regressão get_metrics_group no runtime PyFlink:1 passed em0,37s. Aviso de cache Pytest ocorreu somente por bind de testes readonly no container temporário.
- Integração Kafka→Flink bounded:1 passed em18,06s, FINISHED viaREST, diagnóstico JSON completo da fixture e log controlado de invalid_records.
- Job operacional4de3a42bec3d9d68a659f5cce2064247 (weather-kafka-ingestion):RUNNING. Source/parser/diagnóstico/Sink RUNNING com paralelismo1.
- Evento novo OP-1d7e85dee11d44be:1 ACK Kafka e PrintSink correspondente com timestamps11:20:23.201871Z/schema1.
- Um TaskManager/um slot ocupado. JM/TM userflink, sem variáveis PostgreSQL.
- Memória observada durante processamento:TM571MiB/limite1536MiB, JM286MiB/limite1GiB; sem necessidade de aumentar recursos.
- Producer segue funcionando:novo ciclo confirmou collected3/accepted3/delivered3.
- Review final aprovada, sem P1/P2 abertos; git diff --check aprovado.

Problemas/correções:
- Submissão python -m poderia criar execução local: substituída por flink run -d -py com JobID nativo e comprovação REST.
- Primeiro teste real falhou ao abrir métricas: get_metric_group não existe no runtime Python. Corrigido para get_metrics_group, com regressão RED/GREEN e novo teste real.
- Strings JSON com surrogate escapado: rejeitadas antes da serialização Row.
- Prova/cleanup do teste fortalecidos:diagnóstico completo, ID preservado, cancelamento apenas do job de teste sob prazo limitado.

Limites preservados:sem checkpoint/retomada/exactly-once ou política temporal/deduplicação/sinks de negócio. Submissão do job ainda manual; restart completo do cluster exige reenvio. Erros conhecidos de parsing são descartados/logados, regras adicionais ficam na Etapa5.

Job operacional e Producer deixados ativos. Nenhum volume/topic/registro/grupo alheio foi removido. Próxima etapa:Etapa5 — Data Quality, ainda não iniciada.
