# Etapa 6 — Tarefa 2: checkpoints e recuperação

Status: implementação e verificação final concluídas.

## Implementado

- `configure_checkpointing` aplica checkpointing filesystem em `/opt/flink/checkpoints`, intervalo operacional de 60 s, modo `EXACTLY_ONCE`, timeout de 120 s, pausa mínima de 10 s, concorrência 1, tolerância zero a falhas e retenção externalizada no cancelamento. A estratégia de restart é fixed delay, com três tentativas e 10 s entre elas.
- O volume `flink-checkpoints` é compartilhado pelo JobManager e TaskManager; a imagem cria o caminho com proprietário `flink` e mantém `USER flink`.
- `JobSettings` e `--qa-checkpoint-interval-seconds` aceitam 5–60 s. Um intervalo customizado exige auditoria QA e identidade exclusiva nome/grupo validada antes da submissão. Mesmo a flag explícita com valor 60 exige `--qa-audit` e namespace QA.
- Os SDK tests cobrem valores efetivos do objeto Flink e da configuração Java, preservação de `python.state.cache-size=0`, defaults e override QA, restart e validação da identidade.
- A integração de recovery captura checkpoint C após o aceite de A, publica B depois de C, confirma um checkpoint D mais novo, cancela somente o JobID de teste e restaura `_metadata` de C. A prova exige leitura do metadata nos dois nós, restauração de C pela REST, replay de B, duplicata de A descartada pelo estado restaurado, agregado A+B e checkpoint concluído após a restauração. O checkpoint D é disparado pela REST após o ACK de B para respeitar a janela de retenção do teste.
- O README descreve retenção, limpeza manual por JobID, limite de restart, restauração explícita, ausência de HA/autossubmissão e a semântica limitada do PrintSink.

## Validação

- RED SDK observado pelo principal antes da implementação: a coleta falhou especificamente porque `configure_checkpointing` ainda não existia; as importações dos tipos do SDK de checkpoint/restart passaram.
- RED de recovery observado no runtime: após completar C e D e cancelar o job, C deixou de existir; a REST mostrava três checkpoints concluídos, mas o volume só continha `chk-3`. A causa confirmada pelo principal foi a chave legada `state.checkpoints.num-retained`, ignorada pelo coordenador Flink 1.20, e a ausência da opção canônica `execution.checkpointing.num-retained` na configuração dos serviços.
- Corrigido para aplicar `execution.checkpointing.num-retained=2` no JobManager, TaskManager e configuração da aplicação. O SDK test consulta `CheckpointingOptions.MAX_RETAINED_CHECKPOINTS` tipada, e a integração valida o valor efetivo em `/jobmanager/config` antes do recovery.
- GREEN SDK: 31 testes passaram, com 16 warnings. `compose config --quiet`, build/startup saudável e leitura/escrita por UID 9999 nos dois nós passaram; o marcador técnico próprio foi removido.
- GREEN live recovery: 1 teste passou e 2 cenários foram deselecionados. A REST confirmou checkpoint C id 2 preservado em `file:/opt/flink/checkpoints/<job-id>/chk-2`, apesar de checkpoint D mais recente; o job restaurado reportou C como restored checkpoint e concluiu checkpoint 5. A cópia A foi deduplicada, B foi consumido do offset posterior a C e o acumulador restaurado produziu A+B/count 2. Ambos os jobs QA foram cancelados no cleanup; o Producer foi restaurado.
- Suíte completa: 371 passaram e 19 foram pulados; Ruff passou. A prova cobre estado/offsets do Flink local. Não declara exactly-once ponta a ponta nem considera logs do PrintSink evidência transacional.
