# Etapa 10 — MVP Finalization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development para executar as tarefas com agentes GPT-6 Luna Max. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Finalizar o MVP existente com CI reproduzível e documentação técnica coerente, preservando o pipeline operacional.

**Architecture:** Um workflow verifica qualidade, imagem Python/integração Kafka e imagem Flink/SDK. A documentação descreve o sistema implementado; nenhum novo caminho de produto. Principal controla QA Docker, revisão e commits locais.

**Tech Stack:** Python 3.11.14, Kafka 3.9.1, Flink/PyFlink 1.20.3, PostgreSQL 17.11, Pytest 8.4.2, Ruff 0.14.0, Docker Compose e GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-10-12-mvp-finalization-design.md`, aprovada em 2026-10-12.

## Global Constraints

- Implementadores GPT-6 Luna Max; principal orquestra/revisa/QA. Checkout foundation dedicado reutilizado, sem remoto novo, push, publicação ou deploy.
- Sem novas funcionalidades, serviços, Dockerfiles, dependências de produto ou mudanças nos contratos. Correções só de defeitos concretos encontrados.
- Actions fixadas em commits completos conferidos em fontes oficiais durante implementação. Permissão `contents: read`; sem acesso a secrets reais ou `pull_request_target`.
- Reutilizar duas imagens existentes; Python é compartilhada por Producer/consumer. Não parar/recriar os seis containers operacionais nem cancelar seu job Flink.
- QA efêmera usa projeto Compose exclusivo; `.venv` ignorada armazena provas locais. Não imprimir `.env` ou configuração Compose expandida.
- Evidências anteriores são reutilizadas quando código não muda. Não repetir recuperação histórica/faults, criar benchmark ou auditoria extensa.

## Review Focus

1. Broker novo não contém weather.alerts e auto-create está desativado: preparar explicitamente o tópico antes do teste, sem iniciar Producer/API.
2. SDK ausente faz Pytest ignorar módulos: verificar import/versão PyFlink na imagem e exigir que os módulos SDK executem, sem aceitar skips como êxito.
3. Falha/interrupção de integração deixa containers QA: cleanup `always()` limitado ao projeto CI próprio; comandos locais não atingem o projeto operacional.
4. PR não confiável/configuração Compose: token somente leitura, actions fixadas, ambiente fictício e nenhuma senha real/impressão de config.
5. Infra healthy sem job ativo: documentação diferencia prontidão dos serviços de execução do job e fornece retomada pelo checkpoint existente.

## Task 1 — CI, builds e integração reproduzível

**Files:** criar `.github/workflows/ci.yml`; report em `docs/implementation/stage-10-task-1-report.md`. Consumir testes/Dockerfiles existentes sem alterá-los, salvo defeito comprovado e reportado ao principal.

**Interfaces:** workflow `CI`, eventos push/pull_request/workflow_dispatch; jobs `quality`, `python-kafka` e `flink-sdk`, independentes. Produz verificações sem publicar imagens. Task 2 documenta esses nomes e comandos.

- [ ] Conferir fontes oficiais das actions checkout/setup-python e fixar SHAs completos; registrar versões e origem no report. Runner `ubuntu-24.04`, timeouts 10/15/45 minutos para os três jobs, respectivamente.
- [ ] Job quality: checkout, setup Python 3.11.14, instalar requirements.txt + requirements-dev.txt, executar `python -m pytest -q` e `python -m ruff check src tests`. Não ativar flags de API/broker/checkpoints. Não acrescentar testes que só espelhem YAML.
- [ ] Job python-kafka: preparar Python 3.11.14/dependências e projeto Compose exclusivo derivado de run ID e attempt. Fornecer POSTGRES_DB/USER/PASSWORD e APP_DB_USER/PASSWORD fictícios apenas para interpolação; nenhum serviço PostgreSQL/Flink/Producer é iniciado.
- [ ] Construir `docker compose build alert-consumer`; iniciar somente `docker compose up -d --wait --wait-timeout 180 kafka`. Criar weather.alerts com CLI Kafka, três partições/replicação 1/if-not-exists. Executar `ALERT_CONSUMER_INTEGRATION=1 python -m pytest tests/integration/test_alert_consumer_integration.py -q`; usar o teste existente sem duplicar cenário.
- [ ] Verificar versão Python/cliente e UID 10001 na imagem Python com container temporário. Cleanup em etapa `if: always()` usando o mesmo projeto exclusivo, sem remover imagens ou recursos de outros projetos. Projeto local de QA também exclusivo; principal controla qualquer remoção de volumes próprios.
- [ ] Job flink-sdk: `docker build -f docker/flink/Dockerfile -t climate-logistics-flink:local .`; container temporário com bind readonly do checkout, import explícito PyFlink/versão 1.20.3 e Python 3.11.14. Instalar somente dependências dev de QA no container, sem alterar Dockerfile; executar os quatro módulos test_flink_job.py, test_flink_temporal_sdk.py, test_flink_risk_sdk.py e test_flink_business_sdk.py. Sem iniciar cluster/job ou acessar banco/broker.
- [ ] Congelar fonte e enviar report ao principal. Principal valida workflow por inspeção, executa os comandos equivalentes: suíte geral/Ruff, builds, SDK e uma integração em broker novo exclusivo. Resultado deve ser exit 0 e SDK efetivamente executado; comparar pipeline operacional antes/depois. Revisão independente spec+qualidade antes do commit local `ci: verify MVP quality builds and alert integration`.


## Instruções do implementador

Base 65ea7fc; plano/spec aprovados pelo usuário. Implementar somente Task1, workflow e próprio report. Sem commits, Docker, instalações ou alterações de produto/testes pelo agente; principal executa comandos externos. Pode consultar fontes oficiais na web para SHAs das actions. Host .venv/flink-control/Scripts/python.exe é Python3.12.14/pytest; Root confirma3.11 exato. Ruff global disponível. Sem novos frameworks/testes que espelhem YAML; validar por inspeção e comandos reais. Envie fonte congelada assim que pronta; report curto pode ser concluído em paralelo. Não despachar outros agentes. Toda QA deve respeitar projeto Compose exclusivo e valores fictícios, não usar credenciais locais.
