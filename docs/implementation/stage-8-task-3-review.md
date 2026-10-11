# Revisão independente — Etapa 8, Task 3

Data: 2026-10-11. Base: `982f485`. Snapshot revisado: `.venv/stage8-task3-review.diff`, SHA256 `5C1801779D038BF1CAC535DFA8DB6C71ECD4C275299E5E9510C3C3A743BA8F9C`.

**Veredito do snapshot: correções localizadas necessárias nos gates QA.** Dois achados P2 abaixo. Nenhum defeito novo de produto confirmado; nenhuma expansão funcional, dependência ou refactor amplo é necessária.

**Atualização após fix1 final: os dois P2 e o correlator de checkpoint estão aprovados na revisão estática, sem condição nullable pendente.** O aceite runtime dos faults continua pendente. Os achados abaixo preservam o diagnóstico do snapshot original.

## Achados

### [P2] O gate de texto sensível pode imprimir a própria credencial ao falhar

Arquivo/linhas do snapshot: `tests/integration/test_flink_business.py:1171–1174`; chamada em `tests/integration/test_flink_business_faults.py:496–497`.

`_assert_no_sensitive_failure_text` executa `assert marker not in lowered` no módulo de teste, sujeito ao assertion rewriting padrão do pytest. Se uma exceção/log curto contiver, por exemplo, `password=<valor>`, o pytest pode renderizar o operando `lowered` na explicação da asserção, expondo o valor justamente ao rejeitar a saída sensível. Esse helper é chamado antes de `_assert_no_runtime_passwords_in_text`, cuja exceção tem mensagem genérica e não inclui o valor. A unitária existente testa outro helper de valores, não essa sequência real. O caminho viola o requisito de não externalizar credenciais no relato de falha.

Correção mínima: usar uma condição e `raise AssertionError` com mensagem genérica, sem interpolar o texto; executar a guarda de valores reais antes de qualquer asserção que possa representar o conteúdo. Não é necessário modificar o produto.

### [P2] O prazo de 240 segundos não limita a recuperação nem o caso completo

Arquivo/linhas do snapshot: `tests/integration/test_flink_business_faults.py:467–487`, `537–556`.

`fault_deadline` nasce depois do baseline e limita somente a espera até o job fault ficar `FAILED`. Depois disso, a recuperação concede novos prazos independentes: restore de 60 segundos, auditorias, outputs de 180 segundos, observer e checkpoint. Assim, uma falha qualificada em aproximadamente 200 segundos seguida de recuperação de 60 segundos pode terminar com PASS, embora fault/recuperação ultrapassem 240 segundos. Os helpers ainda reiniciam prazos para leituras separadas. O brief exige deadline de 240 segundos por caso e o README declara falha mais recuperação dentro desse limite.

Correção mínima: compartilhar o prazo acordado entre as fases e passar o orçamento restante aos waits/submissões/observadores que possam bloquear. Ruling do principal após este achado: os 240 segundos começam antes da submissão ruim e abrangem fault mais recuperação; preparação boa e cleanup ficam fora desse orçamento, com essa delimitação explícita na documentação. Reservar cleanup obrigatório para alcançar estado terminal mesmo quando o prazo de aceitação se esgotar. Não introduzir nova arquitetura de QA.

## Verificações estáticas e alcance

- Diff lido integralmente em segmentos, com SHA conferido antes e depois. Brief e relatório lidos; contexto dos helpers legados consultado somente para os contratos usados.
- Oracles de métricas/risco usam valores e janelas esperados definidos em testes; hash calculado independentemente com JSON-list compacto UTF-8. Conferência completa das 19 colunas SQL, oito colunas de alerta e payload de 22 campos, incluindo tipos booleanos estritos.
- Observer seleciona identidades lógicas de todas as fixtures, incluindo classes inelegíveis/desconhecido/sentinela, sem depender do `alert_id` produzido. O range usa offsets inicial/final e valida key/payload/hash; replay aceita somente repetições equivalentes.
- Replay compara commits do grupo QA via broker com end offsets e reapresenta seis eventos para provar dedup. Migração usa somente o manifesto legado autorizado, sem `allowNonRestoredState`, e confirma restore mais checkpoint novo.
- Submissão conserva defaults legados, valida nome/grupo/entrypoint antes de Docker, guarda JobID antes do polling REST e cleanup verifica nome e aguarda terminal do job próprio. Fixture pausa/retoma Producer em `try/finally`.
- PostgreSQL usa role de aplicação para leitura, identidades exatas para fixture e ausência prévia; cleanup apaga alertas antes de métricas. No legado, snapshot isola adições, cleanup exclui somente essas identidades e verifica preservação integral das linhas anteriores sob o preflight de slot único e Producer pausado.
- Faults preservam source `kafka:9092`, usam somente opções tipadas do sink em entrypoint QA e verificam restore C, restart, causa, ausência de checkpoint posterior e convergência. O checkpoint C antecede a entrada posterior; existe guarda de que ele ainda é o último completo antes do cancelamento.

O matcher JDBC que exigia o label `weather-postgres` ausente no trace real já foi identificado/adjudicado pelo principal e tem correção escopada em andamento. Não é achado novo desta revisão e deve ser reavaliado no diff de correção, preservando rejeição de OOM e atribuição ao writer nativo/endpoint correto.

Revisão escopada adicional: `.venv/stage8-task3-guard-fix.diff`, SHA256 `A826173FCD8E0FBBDCEB096D605B56F1875BB135F2EB70B978BECCD110886FEF`, lido integralmente e aprovado estaticamente. O matcher JDBC passou a exigir `unable to open JDBC writer`, `postgres:1` e sinal de SQLException/conexão. Retirar a palavra isolada `metaspace` evita confundir menção informativa com falha; a rejeição de `OutOfMemoryError`, heap e impossibilidade de criar thread permanece. Esse diff não corrige os dois P2 acima; sua correção e respectivas evidências ainda serão revisadas separadamente.

## Evidência de runtime recebida do principal

Esta revisão não executou testes, Docker ou rede. O principal confirmou delivery/replay real PASS em 103,15 s: 24 métricas, 12 alertas PostgreSQL/Kafka completos, key/hash corretos, seis rejeições desconhecidas e seis duplicatas; C4 restaurado e novo C6 completo. Confirmou legado real PASS em 59,84 s, C2→C5, 12 métricas/seis alertas alvo, snapshots e limpeza das 56 métricas/seis alertas adicionados com baseline preservada. Confirmou suíte Python 3.11 com 615 passed/30 skipped, Ruff PASS e SDK anterior 83 PASS para o mesmo produto Task 2.

Fault JDBC teve execução real com conexão a `postgres:1` recusada, três restarts, restore C1 e nenhum checkpoint novo, mas o teste não passou devido ao matcher conhecido. A primeira tentativa parou antes da injeção e não prova fault. Kafka ainda pendente no despacho desta revisão. Esses casos não são declarados PASS, e o aceite runtime final permanece com o principal após correções e provas dos dois faults.

## Re-review escopada — fix1

Artifact: `.venv/stage8-task3-fix1.diff`, SHA256 `769D6038598F2222FF1CA50597D8A5B2839A71B336DF18B58FA4BB3666AEF41A`, conferido e lido integralmente. Nenhum rerun de testes, Docker ou rede; o snapshot original não foi revisitado.

- P2 secrets resolvido: `_assert_no_sensitive_failure_text` usa `if` seguido de exceção genérica; a guarda dos valores reais roda antes das asserções sobre logs/exceptions. O conteúdo sensível não entra na mensagem de erro desse caminho.
- P2 deadline resolvido para o gate de aceitação: o mesmo deadline começa antes da submissão ruim e atravessa fault, auditorias, restauração, entregas, observador Kafka e checkpoint de recuperação. Os waits recebem orçamento restante e um check final rejeita PASS fora de 240 segundos. Preparação boa e cleanup ficam excluídos conforme ruling. Algumas operações mantêm timeouts próprios, mas seu tempo é descontado das chamadas seguintes e o check final impede aprovação tardia; cleanup continua obrigatório fora do orçamento.
- Checkpoint manual: a POST fornece `request-id`; o polling consulta `/checkpoints/<request-id>`, obtém `operation.checkpointId` e exige `details.status == "COMPLETED"` com ID correspondente. Isso substitui a busca pelo último checkpoint global e trata o gatilho real da corrida JDBC3. Falhas dessa operação levantam mensagem genérica.
- Condição conhecida em fix1: o payload REST em andamento pode ter `operation: null`. O complemento final abaixo resolve essa condição; não permanece pendente.

O principal informou **26 unit passed em 0,11 s** e Ruff PASS para fix1. JDBC3 terminou em **52,03 s antes da injeção**, pela corrida de checkpoint corrigida aqui; não comprova fault/recovery. Próximo gate: complemento nullable e provas JDBC/Kafka, sem alteração de produto ou novos cenários.

## Complemento final de fix1

Artifact: `.venv/stage8-task3-fix1-final.diff`, SHA256 `724886D31117F3EC715FC951BBD851E8C14B898C6FBBDC782C720084F7804D4D`, conferido. Comparação somente com fix1 anterior: além dos hashes internos dos arquivos, as únicas diferenças são `operation_details = operation.get("operation") or {}` e a unitária de polling em andamento com `"operation": None`. O helper segue aguardando a operação exata sem chamar `.get` sobre `None`; complemento aprovado estaticamente.

Principal confirmou novamente **26 unit passed em 0,11 s**. Sem rerun local, Docker ou rede. Nenhuma condição estática ou achado P1/P2 desta revisão permanece aberto; JDBC4 está em execução pelo principal e o aceite real JDBC/Kafka continua pendente. Produto Task 2 inalterado.
