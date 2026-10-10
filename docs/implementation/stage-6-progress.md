# SDD ledger — plan: docs/superpowers/plans/2026-10-10-temporal-streaming.md

Spec/plano aprovados pelo usuário. Base baa6474, branch foundation; alterações documentais da etapa6 presentes antes de iniciar código.

Ruling: usar checkout dedicado existente foundation conforme plano aprovado — preserva infraestrutura local e preferência das etapas anteriores — custo se inadequado: migrar alterações para checkout isolado posteriormente.
Ruling: brief/ledger versionados em docs/implementation como nas etapas anteriores, sem workspace temporário Bash — trilha durável em Windows — custo: documentação adicional no repositório.
Ruling: pausar Producer somente durante QA temporal histórico e restaurar em finally; bounded usa anchorUTC atual — evita avanço do watermark por input concorrente/backlog — custo: pausa reversível da coleta durante QA.

| Tarefas | Contrato / preflight |
| --- | --- |
| 1 e 2 | Ambas usam flink_temporal/flink_job e testes: execução sequencial e UIDs estáveis; checkpoint liga após janela revisada |
| 1 | Acumulador11 escalares, output definido, seis janelas1800000/300000; tardio parcial e máximo datetime cobertos |
| 2 | Storage/config e restore explícito; prova offsetB depois checkpointC e dedupA, sem exactly-once PrintSink |

Baseline Python3.11.14: 361 passed,15 skipped; Ruff aprovado. Serviços inicialmente parados, restauração em andamento, volumes preservados.
Task 1: em execução — agente GPT-6 Luna Max.
Task 2: pendente; não despachar antes de revisão/QA tarefa1.

Preflight runtime: cinco serviços restaurados saudáveis; REST sem jobs. Não existe operacional a cancelar neste início. Aggregate/ProcessWindowFunction/OutputTag conferidos. DiscardingSink não exportado no pacote Python; SinkFunction Java org.apache.flink.streaming.api.functions.sink.DiscardingSink confirmado. RestartStrategies aceita int(ms) ou timedelta, não pyflink Time; plano corrigido para10000ms, mesma política3/10s. Configuration storage filesystem e MAX_RETAINED_CHECKPOINTS2 efetivos confirmados. Commit docs kickoffb6fe7ec; usar esta base nos diffs de execução.

Task1: RED puro noPython3.11 collectionModuleNotFoundError; GREEN8passed0,21s+Rufffocused. SDKREDModuleNotFoundError, GREEN14passed e depois15passed10,72s (factorywatermarkreal+200ms), avisos10préexistentes de deps.
Ruling: unit SDK renomeado para test_flink_temporal_sdk.py — basename igual ao teste integration impedia coleta pytestprepend — custo: atualizar referências de arquivo, sem alteração global de pytest.
Ruling: publisher QA temporal usa one-off compose run --rm --no-deps -T producer com comandoPython sobrescrito — helperlegacyexec não funciona com Producer parado — custo: startup curto de container por lote; nenhum novo serviço permanente.

Task1 QA real rodada1: bounded+unbounded2failed75,37s, jobs4c01a88a1233ccf60a3f9e7d4381d0b6/f95e44344e25a915a4f414d3c288579dFAILED. REST root exception prova TypeError int+Time em WindowOperator.cleanup_time; allowed_lateness deveintms0, nãoTime. Snapshot SDK/grafo não executava cleanup_time; regressãoSDK de contrato e repetição live solicitadas. Producer restaurado no finally. Revisão encontrouP2 cleanupstartup dohelperunbounded; holderexterno/fallback pornomeexclusive solicitado mesmawave. Task1 não concluída, Task2 pendente.

Task1 round1GREEN:371passed18skips/Ruff;SDK16passed11,74s/buildhealthy. Live rodadaround1:2reaisFAILED+2mocksPASS119,69s, Producerrestaurado. Boundedjob e8d33281f2afb116075abecff9d5583dFINISHEDmasfronteiraCerrada; unboundedc9da4a4146c58cb57735ede80914dd0aCANCELED,wmwallclockKafka. ProvaSDK strategy._timestamp_assignerNone: with_idleness cria novoWrapper e perdeassignerPython configuradoantes. Round2:timestamp_assigner porúltimo, regressãoSDKretainassigner+UTCms; consolidarcleanupmocks unitcomCLI-before-ID/namefallback, semalteraroracle. Task2aindapendente.

Task1 round2GREEN: SDK17passed13,25s; leve371passed18skips/Ruff, buildhealthy. Re-review factory/P2 endereçada. Live round3: boundedPASS, unboundedFAIL118,44s após watermark correto+65s sem saída; contador late_records ainda não registrado antes primeira entrada atrasada. Ajuste somente teste: removerleituras0prematuras, aguardarregistrométrica30s eexigirREAL1apósD; preservar saída exata deC e ausência de reabertura. Re-reviewtestaprovada. Novaexecunboundedúnica emandamento; Producerrestauradosempre nofinally. Task2pendente.

Task 1: complete — produção/SDK/light/Ruff/revisão aprovados. Bounded passou na rodada3; unbounded1passed125,00s na reexecução única round3 após ajustar startup de métrica. Producerrestaurado; nenhumjoboperacional a restaurar antes deTarefa2, pois início estava semjobs. Commit local a seguir. Task2 liberada após esse gate, não pedir nova aprovação.
