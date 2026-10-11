# Relatório — Etapa 8, Tarefa 3

**Status:** os quatro cenários reais da Tarefa 3 passaram sob o gate do Root: delivery/replay, migração do checkpoint Stage 7 e faults JDBC/Kafka com recuperação. Os resultados abaixo identificam as tentativas intermediárias que falharam e as execuções finais aprovadas.

## Escopo desta tarefa

`tests/integration/test_flink_business.py` cobre a entrega Kafka→Flink→PostgreSQL/Kafka, o checkpoint QA próprio e a restauração do checkpoint Stage 7 preservado. `tests/integration/test_flink_business_faults.py` cobre as falhas reais JDBC/Kafka, a ausência de checkpoint posterior enquanto há entrega pendente e a convergência após restaurar o checkpoint com configuração válida. Os helpers de submissão e cleanup existentes foram estendidos em `tests/integration/test_flink_temporal.py`; a identidade do job é validada antes de Docker nas submissões com sinks ou entrypoint alternativo, e a limpeza espera o JobID próprio atingir estado terminal.

Os eventos dos cenários novos usam quatro perfis em RJ01, SP01 e CT01. O instante T vem do sufixo exclusivo da fixture, cai em um slot de cinco minutos entre 2020 e 2024 e fica afastado do evento de 2020-01-01 do checkpoint Stage 7. A ingestão original é igual ao timestamp do evento; só a cópia LOW duplicada usa ingestão um segundo depois. O cenário normal espera 24 avaliações alvo, 12 alertas HIGH/CRITICAL, nenhuma linha para o sentinela ainda aberto e seis rejeições de negócio para a localidade desconhecida, que continua nos diagnósticos.

As consultas SQL usam as identidades exatas da fixture e leem objetos JSON completos: as 19 colunas de cada avaliação e as oito colunas de alerta, incluindo o payload de 22 campos. O observer Kafka captura offsets antes da entrada e consome até offsets finais capturados; a seleção usa `(location_id, window_start_ms, window_end_ms, risk_rules_version)`, sem filtrar pelo hash sob teste. A validação compara cada key e payload Kafka completos e também detecta alertas LOW/MEDIUM, desconhecidos ou do sentinela. O replay consulta os commits do grupo QA pelo broker e os compara com os end offsets do tópico após o checkpoint, além de reenviar as seis identidades para comprovar as auditorias `duplicate`.

O teste de migração lê somente `.venv/stage8-legacy-checkpoint.json`; não consome o backup operacional da Etapa 7. Ele reapresenta revisões dos dois eventos já contidos no estado restaurado, verifica auditorias `duplicate`, envia o sentinela e compara métricas17, avaliações21, linhas PostgreSQL19 e alertas22 com oracles independentes. Como o checkpoint pode conter um backlog maior que a fixture, o teste tira snapshots das tabelas de fatos antes/depois pela role de aplicação; a limpeza administrativa remove somente as identidades novas atribuíveis ao job e verifica que os rows anteriores permaneceram iguais. A prova fica em `.venv/stage8-legacy-business-diff.json`, sem credenciais.

Os testes de falha produzem um entrypoint temporário sob `/tmp/weather-flink-qa-*`, validam o nome/grupo QA antes de carregar configuração de aplicação e usam `dataclasses.replace` somente em `port=1` ou `kafka_bootstrap_servers="kafka:1"`. A fixture de fault abre seis janelas LOW de RJ01 em T−60 antes de C; após C, um replay do seed é descartado como `duplicate`, e os eventos HIGH em T e LOW em T+100 fecham 12 métricas e seis alertas na mesma key Kafka. O bootstrap da fonte permanece `kafka:9092`.

Os cenários de fixture exigem que as identidades próprias estejam ausentes antes de publicar. O `finally` espera o job próprio chegar a um estado terminal; o cleanup remove somente chaves de fixture verificadas ou as adições isoladas pelo snapshot Stage 7, apagando alertas antes de métricas. O Producer é pausado e retomado em fixture `try/finally`. Nenhum teste para serviços, apaga volumes, topics ou checkpoints compartilhados.

## RED/GREEN local

- O Root registrou RED no gate de job/grupo QA: `3 failed, 9 deselected` em `.venv/stage8-task3-helper-red.log`. Depois do gate restrito a business QA/entrypoints, o Root confirmou `12 passed` em `tests/unit/test_flink_qa_helpers.py`.
- O Root registrou RED do primeiro oracle porque T era recente (2026), em `.venv/stage8-task3-fixtures-red.log`. Após o mapeamento histórico determinístico e a remoção de DELETE pré-teste, a primeira rodada de helpers passou com **13 testes**.
- O Root registrou RED do observer por ausência do predicado de identidade, em `.venv/stage8-task3-observer-red.log`. O predicado agora seleciona mensagens pela identidade lógica mesmo quando `alert_id` está incorreto.
- O Root confirmou RED de equivalência numérica (`20` do JSONB contra `20.0` do oracle) em `.venv/stage8-task3-json-comparison-red.log`; o comparador semântico mantém todas as colunas e rejeita booleanos nos campos numéricos.
- O Root confirmou RED do observer de commits por ausência do helper em `.venv/stage8-task3-offsetprobe-red.log` e RED da guarda legacy em `.venv/stage8-task3-legacy-delta-red.log`. Ambos foram implementados e entram na suíte unitária abaixo.
- O Root confirmou RED da guarda real de senhas com DUMMYPASS e texto sem newline em `.venv/stage8-password-guard-probe.py`; o loop Bash agora processa também a última linha sem newline. A guarda só informa `clean`/`exposed`, sem retornar o valor.
- O Root confirmou RED do contador vazio em `.venv/stage8-task3-zero-counter-red.log`: com 12 métricas, seis alertas e nenhuma avaliação desconhecida, o helper consultava uma métrica Flink não exportada. Agora o contador é observado somente para fixtures com desconhecidos; o teste principal continua cobrindo a contagem seis.
- Para evitar falso PASS por OOM ou indisponibilidade da fonte, o novo guard de causa exige endpoint, sink e erro de conexão do destino. A unitária teve RED (**1 failed**, helper ausente) e GREEN com sinais JDBC/Kafka corretos e amostras negativas de Metaspace/KafkaSource.
- O comparador de JSON usado pela oracle e pela preservação do snapshot legacy foi centralizado em `business_qa_support.py`, preservando o tratamento numérico e booleano estrito já exercitado.
- Testes unitários adicionais tiveram RED para deadline compartilhado, diagnóstico sensível sem echo, checkpoint manual correlacionado e deadline expirado; os quatro passaram após os ajustes.
- Suíte local helpers/oracles: `.venv/flink-control/Scripts/python.exe -m pytest tests/unit/test_flink_business_fixtures.py tests/unit/test_flink_qa_helpers.py -q --tb=short` → **26 passed**.
- Coleta offline dos módulos de integração e unidades: `.venv/flink-control/Scripts/python.exe -m pytest --collect-only tests/integration/test_flink_business.py tests/integration/test_flink_business_faults.py tests/unit/test_flink_business_fixtures.py -q` coletou **17 testes**.
- Ruff escopado nos módulos de negócio, faults, helpers e regressões QA → **All checks passed**; `git diff --check` não encontrou whitespace inválido.

Esses resultados cobrem helpers e oracles locais. O implementador não executou integrações reais; os quatro cenários runtime acima foram executados pelo Root.

## QA real executado pelo Root

`test_real_flink_business_delivery_and_checkpoint_replay_are_idempotent` passou em **103,15 s**. O Root confirmou 24 métricas completas, 12 alertas com os 22 campos e key Kafka corretos, seis rejeições do ID desconhecido e nenhum fato para o sentinela ainda aberto. O JobID inicial `e264f4ee2bfc3710a809481f76aa93f0` completou C4; a restauração usou o JobID novo `d72d4fc49db69d426e47343fc1555aa0`, reportou `restored=4` e completou o checkpoint 6. O reenvio das seis identidades gerou seis auditorias `duplicate`; as 24 métricas e 12 alertas persistiram sem alteração, e os commits do grupo QA corresponderam aos end offsets Kafka depois do checkpoint seguinte. Ambos os jobs QA chegaram a estado terminal; o banco voltou a zero fatos e o Producer foi retomado. Evidências duráveis: `.venv/stage8-business-delivery-third.log` e `.venv/stage8-business-delivery-third-proof.json`.

Duas tentativas anteriores encontraram problemas de comparação: primeiro, 20/20.0 no JSONB e oracle; depois, os gauges `currentOffset=-1` recém-restaurados. O comparador agora usa igualdade semântica JSON com tipos booleanos estritos. A prova de offsets usa `Consumer.committed()` no grupo Kafka QA e o replay explícito, sem tratar `-1` como posição consumida.

O cenário real `test_real_flink_business_restores_stage7_checkpoint_and_writes_open_windows` passou em **59,84 s**. O JobID QA `dc4bf5f757dda7c863c45662bad4981e` reportou o checkpoint legado C2 como restaurado e completou o checkpoint novo C5. O Root confirmou as 12 métricas e seis alertas alvo, auditou as revisões duplicadas e comparou snapshots before/after; o backlog restaurado acrescentou 56 métricas e seis alertas no delta total. O cleanup removeu somente essas chaves novas e comprovou o baseline final de zero fatos, preservando cada row preexistente. A prova está em `.venv/stage8-business-legacy-seven-proof.json` e `.venv/stage8-legacy-business-diff.json`.

As regressões das Etapas 3, 5, 6 e 7 executadas pelo Root passaram. Três tentativas JDBC anteriores foram encerradas sem aprovar o cenário completo: a primeira esperou um contador vazio não exportado; a segunda confirmou o writer `postgres:1` recusado, mas o matcher QA exigia um rótulo ausente no trace; a terceira selecionou o checkpoint global C2 em vez do checkpoint manual solicitado. As respectivas causas foram corrigidas no helper QA: contador observado somente para fixtures com desconhecidos, matcher vinculado ao writer/endpoint/causa reais, e checkpoint manual correlacionado pelo `request-id` até seu checkpoint concluído. O prazo monotônico de 240 segundos agora cobre falha e recuperação; o observador reparte um deadline pelas etapas e checa o orçamento antes de aprovar.

O fault JDBC passou em **131,84 s**. O job válido `cde3951b4fe42e702fb75c9ea4759a3d` concluiu C2; o job defeituoso `b93d797fd8d163915a6820e797590da6` restaurou C2, falhou em `postgres:1` e não criou checkpoint posterior; a recuperação `2897787f5459dcece5926a947bf71a5e` restaurou C2 e completou C3. A recuperação convergiu para 12 métricas e seis alertas, com ao menos duas mensagens equivalentes por identidade. Evidências: `.venv/stage8-business-fault-jdbc-fourth.log`, `.venv/stage8-business-fault-jdbc-fourth-proof.json` e `.venv/stage8-business-fault-jdbc-fourth-tm-restart.log`.

O fault Kafka passou em **188,00 s**. O job válido `714db9839d988d73bc148e4935acafbc` concluiu C2; o job defeituoso `824bef83507e8cd2b92e9d79b2fa750c` restaurou C2, falhou no writer `weather.alerts` apontado a `kafka:1` e não criou checkpoint posterior; a recuperação `7f3def5761359eacb17a95d25ecaa508` restaurou C2 e completou C3. A recuperação também convergiu para 12 métricas/seis alertas e ao menos duas mensagens equivalentes por identidade. Evidências: `.venv/stage8-business-fault-kafka.log`, `.venv/stage8-business-fault-kafka-proof.json` e `.venv/stage8-business-fault-kafka-tm-restart.log`. Nos dois faults, o bootstrap da fonte permaneceu em `kafka:9092`; o Producer foi retomado e somente jobs/fixtures QA próprios foram limpos.

## Comandos de QA real

PowerShell, primeiro entrega/replay e migração Stage 7:

```powershell
$env:FLINK_BUSINESS_TEST = "1"
.\.venv\flink-control\Scripts\python.exe -m pytest tests/integration/test_flink_business.py -q --tb=short
Remove-Item Env:FLINK_BUSINESS_TEST
```

PowerShell, casos separados de falha JDBC e Kafka:

```powershell
$env:FLINK_BUSINESS_TEST = "1"
$env:FLINK_BUSINESS_FAULT_TEST = "1"
.\.venv\flink-control\Scripts\python.exe -m pytest tests/integration/test_flink_business_faults.py -q --tb=short
Remove-Item Env:FLINK_BUSINESS_FAULT_TEST
Remove-Item Env:FLINK_BUSINESS_TEST
```

O runtime precisa ter Kafka, PostgreSQL, JobManager, TaskManager e Producer saudáveis, com o único slot livre antes de cada caso. A fixture pausa o Producer apenas enquanto publica dados controlados; o cleanup aguarda terminal do JobID próprio antes de liberar o slot. As regressões das Etapas 3, 5, 6 e 7 passaram no gate serial do Root.

## Evidências anteriores da Etapa 8

A Task 1 está registrada em `a88fb65`. A Task 2 foi consolidada em `982f485`; o reviewer independente aprovou o parecer da tarefa. Antes da Task 3, o Root reportou **595 aprovados e 26 ignorados** na suíte geral e **83 aprovados** no SDK Flink/PyFlink 1.20.3. Esses números pertencem à validação anterior e não incluem os novos testes de integração desta tarefa.
