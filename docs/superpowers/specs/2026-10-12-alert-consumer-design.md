# Etapa 9 — consumer de alertas e observabilidade

**Status:** proposta para aprovação. Data: 2026-10-12. Base: `ecde2e3`, etapa 8 aceita. Principal orquestra e revisa; agentes GPT-6 Luna Max implementam. Escopo: demonstrar o recebimento confiável dos alertas já produzidos, mantendo o MVP simples.

## Abordagem escolhida

Um processo Python consome `weather.alerts`, valida o contrato, registra o resultado em JSON e confirma o offset. Reutiliza `confluent-kafka==2.12.2`, Python 3.11.14 e a imagem/Dockerfile Python existentes. O Compose acrescenta somente `alert-consumer`, executando o módulo específico, com UID 10001, limite inicial 128 MiB/0,25 CPU e dependência Kafka saudável. Nenhuma alteração do Flink, esquema PostgreSQL, topics ou requisitos Python.

Alternativas consideradas: registrar IDs processados em PostgreSQL/SQLite permitiria deduplicação durável, mas acrescentaria armazenamento e transações para um efeito que é apenas logging; usar transações Kafka não tornaria stdout transacional. Para o MVP, escolhe-se cache limitado em memória e declara-se sua garantia real.

## Contrato e processamento

- Validar JSON UTF-8 de até 16 KiB, objeto com as 22 chaves exatas, `operation=weather_alert`, schema 1 e regras `mvp-1`. Reutilizar a validação/cálculo da etapa 8, sem duplicar regras ou thresholds. Aceitar somente HIGH/CRITICAL, localidades do catálogo e `alert_id` igual ao SHA256 esperado da identidade. Key Kafka deve corresponder ao `location_id`.
- Acrescentar validação pública de alerta ao contrato existente de forma aditiva, preservando o comportamento de `parse_assessment`. Ela não faz IO.
- Grupo operacional `weather-alerts-consumer`, tópico fixo `weather.alerts`. Configuração usa bootstrap Kafka existente e grupo validado; nenhuma credencial de banco. Grupo novo começa em `latest`, evitando apresentar as fixtures antigas como alertas atuais; grupo existente retoma offsets confirmados.
- Desabilitar auto-commit e auto-offset-store. Poll sequencial; confirmar sincronamente o offset somente depois de concluir o tratamento da mensagem, verificando também erros de partição retornados pelo commit. Não confirmar mensagens ainda não tratadas.
- Recebimento válido gera log JSON com componente, operação, status, timestamp, ID, localidade, janela, versão, risco e topic/partition/offset. Escrita e flush de stdout devem propagar falhas; confirmar offset depois dessa escrita. Não prometer persistência transacional de stdout.
- Cache limitado a 10.000 IDs, armazenando fingerprint do payload normalizado. Mesmo ID/payload gera log de duplicata sem repetir o recebimento principal; mesmo ID com payload divergente gera erro de contrato. Cache entra após tratamento bem-sucedido; eliminação dos mais antigos limita memória.
- JSON/key/contrato inválidos são registrados sem imprimir conteúdo bruto e confirmados como mensagens tratadas, para não bloquear a partição. Sem dead-letter topic. Erro de commit ou falha operacional encerra com erro genérico; offsets não confirmados podem ser reentregues.

## Saúde e encerramento

Healthcheck sem HTTP server: arquivo temporário atualizado atomicamente pelo processo, com PID, estado e horário. O consumer faz poll com timeout curto e probe de metadata Kafka a cada 15 s, com timeout de 5 s. Saúde exige processo ativo, heartbeat recente, metadata válida e partições atribuídas; ausência de alertas não é falha. Probe local não cria outro consumer Kafka. Docker verifica a cada 15 s, com tolerância inicial de 30 s e heartbeat máximo de 45 s.

SIGTERM/SIGINT encerram o loop, marcam parada e fecham o cliente. Timeouts de IO e grace period de 30 s mantêm o encerramento limitado. `restart: on-failure:3` mantém tentativas limitadas, seguindo o padrão do Producer. Logs de início, atribuição, recebimento, duplicata, inválido, erro e encerramento oferecem observabilidade básica; não adicionar métricas/servidores próprios.

## Garantias e limites

Entrega e logs são at-least-once. Cache é local ao processo: reinício, rebalanceamento entre processos ou evicção podem permitir novo log para um ID antigo. Falha após stdout e antes do commit também pode repetir o log. Idempotência durável de efeitos externos fica fora da etapa 9. Nenhum Slack, Teams, e-mail, dashboard, Grafana ou integração externa.

## Validação e aceite

Testes focados de contrato22/key/hash/classes, cache/evicção/divergência, ordem log→commit, falha de escrita/commit, inválidos sem conteúdo bruto, saúde ociosa/indisponibilidade e encerramento. Uma integração Kafka real com grupo/container QA próprios verifica válido, duplicata, inválido, offsets e retomada do grupo, com deadline curto e cleanup somente do próprio consumer. Não reiniciar/parar o Flink nem repetir os cenários caros de restauração da etapa 8.

Root revisa código e executa suíte Python 3.11/Ruff, build Python e integração pertinente. Aceite: consumer operacional saudável, logs e offsets verificados; Producer/Flink/PostgreSQL preservados. Commits locais sem push. Arquivos temporários de QA permanecem em `.venv` ignorada; relatório/aceite versionados em `docs/implementation`. Etapa 10 não iniciada.

Fonte de API: [Confluent Python Consumer](https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.commit), consultada para semântica de commit síncrono e offset seguinte. A documentação corrente pode mostrar versão mais nova; a implementação permanece na dependência 2.12.2 já instalada e sua API será conferida no runtime existente, sem upgrade.
