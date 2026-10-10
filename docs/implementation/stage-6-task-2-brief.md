# Stage 6 Task 2 brief

Spec/plano aprovados. Tarefa1 concluída e commit5e81753: janelas corretas/counter/SDK17passed, leve371passed18skipsRuff, boundedPASS e unboundedPASS125s. User exigeGPT-6 Luna Max implementadores. Trabalhar no checkout dedicado foundation. Principal coordena commits/build/serviços/QA, não criaragentes/não.env/nãoalteraroutrosdocs.

Runtimebase: CPython3.11.14/PyFlink1.20.3; grupo/job QA weather-flink-qa-<uuid>; publicador fixturesoneoffcompose run comProducerparado; helperunbounded salvaJobID noholder antespoll ecleanupnomexclusivo. Preservar .allowed_lateness(0) int e estratégia .with_idleness(...).with_timestamp_assigner(...) last, para não regredir testesreais. Não reiniciar/rebuild/submeterjobs sozinho. Docker exigeescalada root, então enviar comandosRED/GREEN pronto; nãoesperar aprovação silenciosa. SDKtestsbind PYTHONPATH=/tmp/qa:/workspace/src evitaimagemstale paraunit. Rootruntime imagecopysrc precisa rebuild parareal.

APIsjáconfirmadas root: RestartStrategies.fixed_delay_restart(3,10000) intmsou timedelta (nãoTime), execution.checkpointing.storage=filesystem/dir=filepathviaConfiguration env.configure, CheckpointingOptions.MAX_RETAINED_CHECKPOINTS2 mediante state.checkpoints.num-retained. Verifique demaischeckpointAPI viaSDKtests. Doc set_checkpoint_storage deprecated, prefConfiguration. Nenhum grupo/tópico/volume apagado. Report docs/implementation/stage-6-task-2-report.md; informar preocupações cedo.

## Tarefa 2 — Storage, checkpoints e recuperação

**Files:** modificar compose.yaml, docker/flink/Dockerfile, flink_job.py; acrescentar configure_checkpointing em flink_temporal.py; ampliar tests/unit/test_flink_temporal_sdk.py e tests/integration/test_flink_temporal.py; atualizar README.

**Interfaces:**
- configure_checkpointing(env: StreamExecutionEnvironment, *, interval_seconds: int = 60) -> None.
- JobSettings acrescenta checkpoint_interval_seconds: int = 60. CLI --qa-checkpoint-interval-seconds (5..60). Valores bool/não inteiros recusados; custom só com qa_audit=True e nome/grupo exclusivo na validação execute existente.
- Não adicionar argumento Python para restore: usar CLI Flink run -s <metadata> -py ... com os mesmos args/UIDs; não usar allowNonRestoredState.

- [ ] Escrever SDK RED para valores efetivos de checkpoints/restart/storage/retention e QA identity. Operacional60000ms, timeout120000, pausa10000, concurrency1, EXACTLY_ONCE, tolerable_failures0, RETAIN_ON_CANCELLATION; rejeitar custom em operacional/mismatch/valores fora5..60.
- [ ] Implementar configurações usando Configuration+env.configure para execution.checkpointing.storage=filesystem, execution.checkpointing.dir=file:///opt/flink/checkpoints, state.checkpoints.num-retained=2; API checkpoint para interval/mode/timeout/pause/concurrency/failures/external retention. RestartStrategies.fixed_delay_restart(3,10000). Preservar state.cache-size0 e defaults do bundle operacional.
- [ ] Adicionar diretório /opt/flink/checkpoints na imagem com owner flink e volume flink-checkpoints em ambos os nós, sem serviço novo. Executar compose config --quiet sem imprimir configuração expandida e rebuild; confirmar writer/readers uid9999 conseguem criar/ler pequeno marcador no novo volume. Remover apenas esse marcador técnico próprio após teste, nunca diretório/volume inteiro.
- [ ] Executar SDK no runtime e suíte anterior para detectar opções ignoradas/signaturas erradas. Confirmar metadata/checkpoint storage efetivo, não só objeto mock.
- [ ] Escrever recuperação RED: QA/latest, checkpoint_interval10s e dedup TTL operacional86400; Producer pausado. Publicar A de2020 numa janela aberta, esperar aceiteaudit e checkpoint COMPLETED disparado depois do aceite; guardar external_path/_metadata, id, timestamps e offsets do source. Verificar arquivo do path capturado existe nos dois nós.
- [ ] Após capturar checkpointC, publicar B novo na mesma janela e confirmar ACK. Cancelar somente job próprio antes deC ser removido pela retenção2 (deadline menor que20s); restaurar explicitamente C com mesmo grafo/grupo e novo JobID, mesmo quando há checkpoint mais recente. Verificar path após cancel; se orçamento falhar, teste falha/reinicia com evidência, não escolhe um checkpoint que já contémB.
- [ ] Depois do restore, publicar cópiaA revisada e sentinel avançando Event Time40min emoutra localidade. Exigir duplicate audit deA, resultado da janela com A+B/count2, extremos/média esperados; isso prova tanto acumulador/marcador restaurado quanto leitura deB publicado depois deC, apesar de initializer QA/latest. Nenhum resultadocount1/3 para essa janela.
- [ ] Confirmar REST restored-checkpoint no novo job referenciaC e pelo menos um checkpoint novo COMPLETED. Separar logs por horário/JobID/localidade; repetição de PrintSink não será alegada exatamente uma vez. Capturar metadata e contagem de arquivos sem logs de secrets.
- [ ] Garantir cleanup finally cancela apenas ids QA capturados, preserva checkpoints para inspeção. Pausar/reiniciar TaskManager só se necessário para prova adicional, não simular falha do cluster inteiro para um teste que só precisa restore explícito.
- [ ] README documenta restore CLI semallowNonRestoredState, retenção2 não limita diretórios dejobs antigos, limpeza manual autorizada, restart limitado, ausência de HA/autossubmissão e PrintSink não transacional. Registrar tarefa2/QA; revisão antes decommit.
