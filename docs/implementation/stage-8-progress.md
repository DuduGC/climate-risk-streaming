# Etapa 8 — preparação e decisões

Data local:2026-10-11. Base:a26109e, checkout dedicado foundation. Usuário solicitou iniciar a etapa8 e reafirmou que o principal orquestra/revisa e agentes GPT-6 Luna Max implementam. A implementação da etapa8 ainda não começou.

## Exploração concluída

- Checkout limpo no início; etapa7 aceita. Escopo original confirmado: três tabelas de negócio, Flink→PostgreSQL e Flink→weather.alerts; consumer pertence à etapa9.
- Bootstrap existente cria usuário de aplicação não-super e schema climate, com CONNECT/USAGE; tabelas de negócio ainda não são definidas nos scripts do repositório. Volume existente exige migração explícita.
- Imagem Flink atual tem Kafka connector, sem JDBC/driverPostgreSQL. Compose não injeta credenciais do banco no Flink. Mudanças propostas serão específicas da integração nativa e usarão somente usuário de aplicação.
- Principal explorou código/grafo, contratos17/21, UIDs e catálogo3localidades. Agente read-only stage8_sink_design, GPT-6 Luna Max, analisou alternativas e riscos; sem implementação, alterações, testes, Docker ou commits pelo agente.
- Documentação oficial Flink1.20/JDBC/Kafka e PostgreSQL17 verificada. Proposta JDBCUPSERT/Kafkaat-least-once não declara atomicidade entre destinos. CTE comRETURNING liga métrica/alerta dentro de uma SQL, sem corrida deFK.

## Continuidade operacional

Serviços reiniciados antes desta preparação: Docker saudáveis, REST semjobs. Checkpoints40/41 da operação7 preservados. Principal retomou somente o grafo7, sem sourcealterado, usando `/opt/flink/checkpoints/3caa432342067923b7edae698657e8d6/chk-41/_metadata`.

Novo JobID cf62f651459bb421c75fd589955091ae RUNNING; REST confirmou restored41, completed44 e failed0. Nenhum volume, checkpoint, topic, tabela ou dado removido. Esse é o job7, não uma implementação antecipada da etapa8.

## Desenho para aprovação

Spec proposta: docs/superpowers/specs/2026-10-11-business-sinks-design.md. Define objetivo, três alternativas, política proposta HIGH/CRITICAL, identidade porlocal/janela/versão, contratoalerta22, schema3tabelas, privilégios, batching, tratamento de desconhecidos, limites de recuperação e critérios de QA.

Self-review do principal: contratos/campos coerentes; separação entre atomicidade PostgreSQL e Kafka; nenhum consumer/serviço adicional; QA anterior sem writes porpadrão; tabelas não guardam raw; escopo e decisões de produto explicitamente propostos. Os parâmetros exatos de conectores/driver e tarefas serão definidos no plano escrito após aprovação da spec, antes de instalação ou código de produto.

Próxima ação requerida: usuário revisar/aprovar a spec. Depois, principal escreve plano com interfaces/tarefas e execução por agentes LunaMax; aprovação do plano precede implementação conforme workflow arquitetural. Nenhum agente implementador foi despachado para código daetapa8.
