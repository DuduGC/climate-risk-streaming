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

## Task 2 — Documentação final e operação

**Files:** modificar `README.md` e `docs/progresso.md`; criar `docs/architecture.md`; report `docs/implementation/stage-10-task-2-report.md`. Não alterar código de produto.

**Interfaces:** consumir workflow/nomes/comandos aprovados da Task 1 e as fontes em src, compose.yaml, docker/postgres/migrations/001-business-sinks.sql e aceites das etapas 1–9. Produzir documentação da versão implementada, com referências a essas fontes de verdade.

- [ ] README: atualizar resumo para MVP implementado, tabela de seis containers e limites totais de memória 4,375 GiB (além de overhead Docker), preservando recomendação de 6 GiB/4 vCPUs. Corrigir informações antigas de permissões/tabelas e etapas pendentes.
- [ ] Documentar build das duas imagens, start e verificação dos serviços. Separar prontidão de infraestrutura de `/jobs/overview` com job RUNNING. Documentar comando de submissão existente e restauração `flink run -d -s <metadata> -py /opt/climate/src/climate_logistics/flink_job.py`; caminho deve vir do checkpoint persistido disponível, sem hardcode de JobID local, allowNonRestoredState ou ressubmissão de job já ativo.
- [ ] README: explicar CI, comandos locais e opt-ins; diferença entre workflow configurado/validado localmente e execução GitHub ainda indisponível sem remoto. Não orientar rodar toda integração pesada indiscriminadamente no pipeline ativo.
- [ ] docs/architecture.md: diagrama Mermaid do fluxo; tópicos weather.raw/weather.alerts com keys e topologia; campos/unidades/schema e regras/versionamento com referências a src; tabelas PostgreSQL, identidade/hash e garantias reais. Explicar janelas 30 min/slide 5 min, watermark 120 s, idleness 60 s e TTL 24 h conforme código.
- [ ] Documentar segurança e recursos atuais, medidas já observadas sem chamá-las de benchmark, limitações e lista curta de melhorias futuras. Explicar cache consumer em memória/at-least-once e ausência de transação distribuída. Descrever dados Open-Meteo como modelo, fixtures como QA; ausência de alertas meteorológicos novos não torna consumer unhealthy.
- [ ] Atualizar estado em docs/progresso.md: etapas 1–9 concluídas; etapa 10 em verificação até aceite do principal. Conferir links locais, comandos, thresholds e números contra as fontes. Revisão independente da documentação; principal corrige somente por agente Luna Max e registra commit local `docs: document MVP architecture operation and limitations`.

## Aceite final do principal

- [ ] Revisão ampla independente: nenhum P1/P2 aberto; segurança do workflow, escopo MVP e coerência das garantias. Não executar scan extensivo ou benchmark novo.
- [ ] Confirmar os mesmos IDs dos seis containers e JobID operacional antes/depois, consumer healthy e checkpoints avançando. Não recriar serviços por efeito de build. Se o ambiente tiver mudado externamente, diagnosticar e registrar, sem atribuir ao CI sem evidência.
- [ ] Scan seguro de credenciais/index, `.env`/`.venv` ignoradas, diffcheck; documentar resultados reais e limites em docs/implementation/stage-10-acceptance.md. Atualizar docs/progresso.md para etapa 10 concluída somente após gates.
- [ ] Commits locais, checkout limpo e relatório de finalização ao usuário. Nenhuma execução no GitHub será alegada sem run real; remoto/push continuam fora do escopo autorizado.

## Self-review e método

CI/builds/SDK/integração e riscos 1–4 pertencem à Task 1; documentação/operação e risco 5 à Task 2. Os arquivos não se sobrepõem, mas Task 2 consome comandos finais da Task 1; executar sequencialmente evita documentação divergente. Reutilizar evidências caras de etapas anteriores. Não há nova interface de produto ou dependência.

Método preservado: fresh implementador GPT-6 Luna Max e revisão por tarefa, principal executa QA e commits, revisão ampla final. Briefs/ledger em docs/implementation e provas em .venv. Plano aguarda revisão do usuário antes de implementar.
