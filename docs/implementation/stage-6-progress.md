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

Task1 commit5e81753. Index/check/secretscan aprovados, .env fora do índice. Task2: em execução com agentefreshGPT-6 LunaMax; briefstage-6-task-2-brief.md. Serviços5restaurados, sem job operacional submetido atéfinaletapa6.

Task2 SDKRED: configure_checkpointingausente0,47s. Primeiraimplementação:6failed25passed6,42s devido bindingExternalizedCheckpointRetentionPython procurarclasseJavaerrada. Configopçãopública execution.checkpointing.externalized-checkpoint-retention confirmouenumJava/cp RETAIN_ON_CANCELLATION, sempatchlibs. SDKGREEN31passed13,19s(16avisos), mesmaspolíticasdo spec. Cli60overrideexigeQA, guardregexajustado. Infra/volume/buildeQArecuperação emandamento; Task2notcomplete.

Task2 recovery rodada1:1failed2deselected56,48s. Checkpoints dojob46edf6dd2ae11cacfcabb63fd06ca080C2/D3COMPLETED, metadataC2 ausenteapóscancel; directoryonlychk3. RuntimeJobManagersemretençãoexplicit; chavecanônicaJava execution.checkpointing.num-retained default1. Ruling: aplicarretenção2 noConfigdojob eFLINK_PROPERTIESdosdoisnós — retenção efetiva do coordenador é configuração de cluster e valor2 jáaprovadono spec — custo: dois snapshots porjobativo, previsto, semaumentarpolicy/paralelismo. Patch/SDKtypedteste/re-review/rebuild/recuperaçãoagendados. Nenhumcheckpoint/dado apagado; Producerrestauradofinally.

Task2 fixretençãoGREEN: SDK31passed13,84s; clusterREST execution.checkpointing.num-retained=2 confirmado. Recoveryfinal:1passed2deselected77,26s, orig/restoredjobsbc6d948e3b5710b1f96c61c37faffd45 e804b3ee142e643f363bff67d47763741, ambosCANCELEDpróprioscleanup. CP C anterior aD preservado/restaurado, BpósC relido, duplicataA descartada, agregadoA+B eCPnovoCOMPLETED. Producerrestauradofinally. Re-reviewretençãoaproved. Task2completeemvalidaçãotécnica; commit eQAbranchfinala seguir.

Task 2: complete — retençãoefetiva2/JM+TMConfig, volume9999, SDK31/light371/Ruff/revisão+recover77,26s aprovados. OrigCP3maisrecente; restoreCP2 confirmadoREST/newCP5. Commitlocalatualizado e verificaçãowholebranch/operacional a seguir; etapa6aindanãoencerrada.

Regressões finais legadas Kafka/Flink:3passed86,18s comcheckpoints habilitados. Verificação temporalwhole3(em vez de limitarQAoTask1semCP) a seguir; sourceprod estável. Task2indexcheck detectoublankEOFbrief e foi corrigido semalterarlógica.

## Retomada após continue do usuário

Código/commits preservados (5e81753/2e30975), checkoutlimpo. Sessão doúltimoQAtemporalglobal27836 indisponível apósinterrupção/quota, portREST8081semconexão. NãoinferirPASSsemoutput; serviçosserãorestaurados eQAglobalreexecutadocomlogdurável. Ruling: revisorfinalGPT-6.1SolHigh emvezAstra — tentativaAstrafalhoulimiteuso — custo: revisãoemmodelodisponível, semalterarmodelodosimplementadoresLunaMax. Nenhumtestecódigo/revisãocompletadaredespachada; somentegatefinalpendente. Etapa7nãoiniciada.

QA temporalfinal apósretomada:2passed1skipped162,52s(log.venv/stage6-final-temporal.log). Skipnãoaceito comoPASS; prováveltransiçãoCancelled asyncjobpróprio/slot, motivo nãoimpresso por -q anterior. Recuperaçãoseráexecutadaisoladamente comslotlivre e-rs/logdurável, semmudarprodução. Revisorfinalfallback6.1Sol aprovouwholebranch b6..2e30975, semP1P2. Integração/operacionalrestantesroot.

QAfinalretomada:temporalbounded/unbounded2passed162,52s; recuperaçãoisolada1passed2deselected73,01s(logdurável.venv/stage6-final-recovery.log), semskipcounted. Todos6cenáriosreais possuemPASS:3legados86,18s +2temporais +1recuperação. RevisorfinalfallbackwholebranchaprovadosemP1P2. JobQAcleanupIDs4655395ac4dd2a7f28abbd859e3051e4/5ab7216be1ede558d71546fa85113e85 eorig/restoredcfa934431b03582d4b2078efa214a3f8/4e363cec305f74c52123fbc0265d8cd5 terminalCANCELED/FINISHED. Producerrestaurado. Operacionalsubmissãoemexecução; etapa6nãoanunciaratéCP/métricas/resource+docscommit.

## Fechamento da Etapa 6

Finalfresh:371passed19skips6,93s/Ruff; SDK31passed16,80s. SeiscenáriosreaisPASS em execuções coordenadas; recovery73,01s finalisolada, nenhumskiptratadocomoaceite. Operacional762137e915474bd6827555e68fc8e4c6 RUNNING, 174métricasCT/RJ/SP na prova,4CPsCOMPLETED/0failed, CP4estado232628bytes/14ms. TM781,6MiB/JM476,3MiB, backpressure0 nos3vertices, UID9999, semPGvars. Producer recuperoudeOpenMeteoErrorRJ e último ciclo3/3 failures0. Ruling: manterbranchfoundationlocalsem push/merge conformeplanoaprovado — integraçãoremota nãoautorizada — custo: integrarposteriormente quando solicitado. Todosgates atendidos; documentodeaceite/reviewfinal/progress atualizados, indexcheck/scan/commitfinal aseguir. Workspace scratchQA e volumespreservados, nãodeletarworkspacealheio. Etapa7nãoiniciada.
