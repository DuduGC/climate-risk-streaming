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

## Task 2 — Documentação final e operação

**Files:** modificar `README.md` e `docs/progresso.md`; criar `docs/architecture.md`; report `docs/implementation/stage-10-task-2-report.md`. Não alterar código de produto.

**Interfaces:** consumir workflow/nomes/comandos aprovados da Task 1 e as fontes em src, compose.yaml, docker/postgres/migrations/001-business-sinks.sql e aceites das etapas 1–9. Produzir documentação da versão implementada, com referências a essas fontes de verdade.

- [ ] README: atualizar resumo para MVP implementado, tabela de seis containers e limites totais de memória 4,375 GiB (além de overhead Docker), preservando recomendação de 6 GiB/4 vCPUs. Corrigir informações antigas de permissões/tabelas e etapas pendentes.
- [ ] Documentar build das duas imagens, start e verificação dos serviços. Separar prontidão de infraestrutura de `/jobs/overview` com job RUNNING. Documentar comando de submissão existente e restauração `flink run -d -s <metadata> -py /opt/climate/src/climate_logistics/flink_job.py`; caminho deve vir do checkpoint persistido disponível, sem hardcode de JobID local, allowNonRestoredState ou ressubmissão de job já ativo.
- [ ] README: explicar CI, comandos locais e opt-ins; diferença entre workflow configurado/validado localmente e execução GitHub ainda indisponível sem remoto. Não orientar rodar toda integração pesada indiscriminadamente no pipeline ativo.
- [ ] docs/architecture.md: diagrama Mermaid do fluxo; tópicos weather.raw/weather.alerts com keys e topologia; campos/unidades/schema e regras/versionamento com referências a src; tabelas PostgreSQL, identidade/hash e garantias reais. Explicar janelas 30 min/slide 5 min, watermark 120 s, idleness 60 s e TTL 24 h conforme código.
- [ ] Documentar segurança e recursos atuais, medidas já observadas sem chamá-las de benchmark, limitações e lista curta de melhorias futuras. Explicar cache consumer em memória/at-least-once e ausência de transação distribuída. Descrever dados Open-Meteo como modelo, fixtures como QA; ausência de alertas meteorológicos novos não torna consumer unhealthy.
- [ ] Atualizar estado em docs/progresso.md: etapas 1–9 concluídas; etapa 10 em verificação até aceite do principal. Conferir links locais, comandos, thresholds e números contra as fontes. Revisão independente da documentação; principal corrige somente por agente Luna Max e registra commit local `docs: document MVP architecture operation and limitations`.


## Contexto e instruções

Somente README.md, docs/progresso.md, docs/architecture.md e próprio report. Sem produto/testes/CI, Docker, instalações ou commits; principal verifica e commita. Documentação em português, clara, curta e sem excesso de histórico. Pode reorganizar README para evitar instruções contraditórias, mantendo detalhes úteis e opt-ins pertinentes. Não despachar subagentes.

Task1 workflowCI: jobs quality/python-kafka/flink-sdk, actions fixadas, tags/projeto exclusivos, QA broker novo. Root geral Python3.11 695pass31skip/Ruff; buildsPython/FlinkPASS, integração nova1pass33,76s/cleanuppróprioPASS. SDK final61passedzero skips18,54s; não alegar GitHubrun. Git sem remoto.

Antes desta implementação, JobManager reiniciado20:58UTC deixou jobs vazio em standalone. Root retomouCP229 do job anterior0b39...; novoJobID60b77eb183c15da8e8e3ab5e11509bd5RUNNINGcomCP230/0falhas. Não hardcode IDs locais nos comandos de docs. Baselineproof em .venv. O problema é serviçohealthy semjob, explicitamente cobrir diagnóstico/submissão/restore com caminho disponível e sem jobduplicado.

Consumidor de alertas já passou QA Stage9, cache apenas memória, commitapóslog/flush e offsets manual. DadosQA históricos não devem pareceralertas atuais. performance: não há benchmarkthroughput, apenasobservações locais documentadas. Fontesverdade código atual/Compose/SQL/aceites. Evidências definitivas QA e aceitefinal Root. Envie frozen cedo; relatório curto.

Task1 gate fechado e commit local registrado. Task2 autorizada; preserve fonte CI e código produto. Root efetua aceite final e alteração do estado etapa10 para concluída.
