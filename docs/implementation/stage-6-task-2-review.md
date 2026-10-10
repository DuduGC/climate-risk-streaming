# Etapa 6 — Revisão independente da Tarefa 2

Data: 2026-10-10. Base informada: `5e81753`. Escopo: pacote `.venv/stage6-task2-review.txt`, arquivos correspondentes no checkout, spec aprovada e Tarefa 2/constraints do plano temporal. Revisão estática; nenhum Docker, teste, alteração de código, ambiente, commit ou subagente foi executado pelo revisor.

## Resultado

A revisão estática inicial não identificou P1/P2. A integração live posterior do principal demonstrou o P1 abaixo e invalidou a aprovação inicial de retenção. A rereview focada da correção foi concluída: o P1 está corrigido no código/configuração, sujeito à confirmação da nova prova live de recovery pelo principal. Nenhum novo achado acionável no patch de retenção. Esta avaliação não encerra a etapa nem substitui essa prova.

## P1 — Retenção configurada no job não preserva dois checkpoints no coordenador

Localização inicial: `src/climate_logistics/flink_temporal.py:116` e `compose.yaml`, blocos `FLINK_PROPERTIES` do JobManager/TaskManager. A aplicação define `state.checkpoints.num-retained=2` no ambiente do job, mas a configuração do coordenador standalone não recebe a política. A assertion SDK inicial lê a opção definida no objeto da aplicação, sem provar a retenção do coordenador que gerencia checkpoints concluídos.

Evidência live informada pelo principal: recovery falhou em 56,48 s; no job `46edf6dd2ae11cacfcabb63fd06ca080`, REST registrou três checkpoints concluídos (IDs 1, 2 e 3), mas o diretório continha apenas `chk-3`. O metadata de C (`chk-2`) já havia sido removido ao completar D (`chk-3`). O endpoint `/jobmanager/config` não mostrava retenção configurada. Consulta Java do principal identificou `CheckpointingOptions.MAX_RETAINED_CHECKPOINTS.key()` como `execution.checkpointing.num-retained`, default 1.

Impacto: o fluxo de restore explícito de C após checkpoint mais novo falha porque C deixa de existir, contrariando a política aprovada de retenção de dois checkpoints. Correção solicitada ao implementador pelo principal: usar a chave canônica na aplicação e em `FLINK_PROPERTIES` de ambos os nós; assertion via opção Java tipada. Manter retenção 2 e o teste causal existente. Não aumentar retenção, copiar metadata ou alterar a escolha do snapshot para contornar a falha.

## Rereview focada do P1

Status: corrigido estaticamente; validação live final pendente do principal.

- `src/climate_logistics/flink_temporal.py:116`: a aplicação agora define `execution.checkpointing.num-retained=2`.
- `compose.yaml:58` e `compose.yaml:92`: os blocos `FLINK_PROPERTIES` do JobManager e TaskManager recebem a mesma chave canônica com valor 2. Isso inclui a configuração do coordenador que estava ausente no runtime anterior.
- `tests/unit/test_flink_temporal_sdk.py:255–257`: a assertion usa `CheckpointingOptions.MAX_RETAINED_CHECKPOINTS` e exige 2, eliminando a inferência por leitura textual da chave anterior.
- `tests/integration/test_flink_temporal.py:520–533`: guard consulta `/jobmanager/config` e exige o valor efetivo `execution.checkpointing.num-retained=2`; uma chave ausente ou valor diferente falha. O guard executa antes da submissão do job de recovery, após as verificações de coordenação.
- O patch mantém retenção 2, checkpoint C selecionado antes da publicação de B, prova de D mais novo e restauração explícita de C. Não acrescenta cópia de arquivos nem altera o teste causal para ocultar a falha original.

O revisor examinou somente a correção de retenção e seu guard/assertion, sem reauditar Task 1 ou partes não alteradas e sem executar runtime. Build/SDK da correção estavam em execução pelo principal no momento desta rereview.

## Evidências revisadas

- `src/climate_logistics/flink_temporal.py:98–125`: filesystem em `file:///opt/flink/checkpoints`, retenção externalizada no cancelamento, intervalo operacional 60 s, `EXACTLY_ONCE` de estado/offsets, timeout 120 s, pausa 10 s, concorrência 1, tolerância 0 e restart fixed delay com três tentativas/10 s. A suíte SDK consulta a opção Java tipada da retenção externalizada e exige `RETAIN_ON_CANCELLATION`. A retenção de dois checkpoints concluídos, inicialmente inferida da configuração da aplicação, foi refutada pela prova live acima.
- `compose.yaml` e `docker/flink/Dockerfile`: um named volume no mesmo caminho nos dois nós; diretório criado com proprietário `flink`; `USER flink` preservado. Não há novo serviço, socket Docker ou ampliação de permissões do host.
- `src/climate_logistics/flink_job.py:247–311` e `351–408`: `python.state.cache-size=0`, paralelismo, schema/Row e UIDs existentes preservados. Intervalo customizado exige QA; a flag CLI explícita, inclusive com valor 60, exige auditoria e namespace nome/grupo correspondente antes da execução. Não há override por ambiente.
- `tests/integration/test_flink_temporal.py:1044–1197`: recusa job ativo ou Producer concorrente; ACK e audit de A precedem um checkpoint COMPLETED disparado depois do aceite. B só é publicado após C; avanço de offset e um checkpoint D com trigger posterior ao aceite de B são exigidos. A restauração recebe `_metadata` de C, sem `allowNonRestoredState`, exige novo JobID e confirmação REST do ID de C; o replay de B é verificado após restore. O arquivo de C precisa ser legível nos dois nós antes e depois do cancelamento, com orçamento explícito de retenção.
- `tests/integration/test_flink_temporal.py:1199–1273`: cópia revisada de A deve ser duplicata; sentinel avança watermark real; oracle completo exige estado de janela A+B, incluindo contagem, média e extremos. Um checkpoint novo COMPLETED é exigido no job restaurado. A prova combina estado do source, marcador de dedup e acumulador, em vez de inferir recuperação somente de logs de submissão.
- `tests/integration/test_flink_temporal.py:300–412` e `1274–1296`: captura de JobID antes da espera por RUNNING e antes de erros de CLI; restore exclui IDs anteriores na busca fallback; cleanup considera somente IDs capturados ou encontrados pelo nome QA exclusivo e estado ativo. Não remove checkpoints, diretórios ou volumes.
- README: restauração explícita, limite de restart, retenção de jobs cancelados, limpeza manual por JobID, ausência de HA/autossubmissão e possibilidade de repetição do PrintSink estão descritas; não há alegação de exactly-once ponta a ponta.

## Evidências de execução atribuídas ao principal

O principal informou SDK inicial: 31 passed em 13,19 s, 16 warnings; suíte leve: 371 passed/19 skips em 6,73 s e Ruff aprovado; Compose config quiet, build e serviços saudáveis; volume com UID 9999 e escrita/leitura pelos dois nós aprovados. A primeira integração real de recovery falhou em 56,48 s pela retenção descrita no P1. O principal informou que restaurou o Producer após a falha. A correção e sua rereview estática foram concluídas; a nova integração real de recuperação permanece necessária para o aceite. O revisor não repetiu essas execuções. Os skips não são tratados como prova de integração.

Task 1 não alterada e demais etapas não foram reauditadas.
