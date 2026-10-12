# Aceite da etapa 9 — consumer de alertas

Data: 2026-10-12. Escopo MVP aprovado: Python consumer de weather.alerts, logs JSON e healthcheck. Implementação por agentes GPT-6 Luna Max; principal responsável por orquestração, revisão e execução de QA. Etapas 1–9 concluídas; etapa 10 pendente.

## Implementado

Validação estrita do contrato de 22 campos, catálogo/key/hash e risco HIGH/CRITICAL, usando regras existentes. Consumer sequencial com auto-commit e auto-offset-store desativados; commit síncrono do offset seguinte somente após escrita/flush. Grupo novo confirma baseline latest antes de receber; grupos existentes retomam posições confirmadas. Cache FIFO de 10.000 IDs e fingerprints normalizados suprime duplicatas dentro do processo. Inválidos geram razão segura e são confirmados sem payload bruto.

Logs JSON de recebimento, duplicata, inválido, início, atribuição, erro e encerramento. Healthcheck por arquivo atômico exige PID ativo, assignment e heartbeat/metadata recentes, inclusive sem alertas novos. Compose reutiliza a imagem Python existente, UID 10001, 128 MiB e 0,25 CPU. Não há novo banco, tópico, dependência ou integração externa.

## Evidências verificadas pelo principal

- Suíte geral no Python 3.11.14: 692 passed, 31 skipped em 7,99 s; Ruff src/tests aprovado. Cenários externos continuam opt-in.
- Correção final de lifecycle: 165 testes afetados aprovados em 1,10 s no Python 3.11.14; Ruff aprovado. Demais caminhos não alterados após a suíte geral.
- Build inicial e final aprovados; runtime Python 3.11.14/confluent-kafka 2.12.2/UID 10001.
- Uma integração real: 1 passed em 32,57 s. Validou alerta HIGH, equivalente duplicado, inválido sem raw, commits, saúde ociosa e retomada após recriar o consumer com o mesmo grupo. Fixtures identificadas por ID e ACK topic/partition/offset. Containers exclusivos removidos; grupos QA preservados. A correção posterior alterou somente logs lifecycle e teve testes afetados + execução nativa final.
- Consumer operacional healthy, com logs nativos consumer_started e consumer_assigned. Cinco serviços existentes conservaram os mesmos IDs de container. Mesmo job Flink 0b39a2acc47743560987333bea204347 RUNNING, checkpoint 224 e zero falhas.
- Revisões independentes Task1 e Task2 aprovadas; P2 lifecycle fechado na rechecagem. Revisão final ampla registrada separadamente.

Provas temporárias locais ignoradas: .venv/stage9-python311.log, stage9-lifecycle-python311.log, stage9-integration.log, stage9-build-final.log e stage9-operational-proof.json. Relatórios versionados em docs/implementation/stage-9-*.

## Problemas e correções

JSON profundamente aninhado provocava RecursionError no parser novo; convertido em erro de contrato, sem alterar parser antigo. Revisão do cliente encontrou nome incorreto do flag auto-commit e subscribe com string: corrigidos para API nativa e cobertos por testes. A integração foi renomeada para evitar colisão de import do pytest. Review detectou ausência dos logs de ciclo de vida previstos e a correção mínima foi implementada e rechecada.

Antes da implementação, o JobManager havia reiniciado e não mantinha job ativo; Root retomou o job pelo checkpoint 183 existente. Isso foi continuidade operacional, sem mudança no código Flink e sem reiniciar serviços para QA etapa 9.

## Limites e decisões

Logs são at-least-once e cache é somente em memória: restart, evicção e falha entre stdout e commit podem repetir registros. Nenhuma idempotência durável ou entrega transacional de stdout é prometida. Latest no primeiro uso ignora backlog deliberadamente; offsets existentes são preservados. QA é controlada e não constitui alerta meteorológico real.

Checkout foundation dedicado reutilizado; sem merge/push, remoto GitHub não configurado. .env e .venv permanecem ignorados. Etapa 10 será finalização: testes finais, integração, Ruff/Pytest, CI e documentação do MVP.
