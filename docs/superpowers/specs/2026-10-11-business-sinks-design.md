# Etapa 8 — PostgreSQL e Kafka Alerts

Status: desenho aprovado pelo usuário em2026-10-11. Plano de implementação escrito, aguardando revisão do usuário antes de execução. Escopo original: tabelas `locations`, `weather_metrics`, `weather_alerts`; Flink → PostgreSQL e Flink → `weather.alerts`. Base: `a26109e`, branch `foundation`.

## Objetivo e responsabilidades

Transformar as avaliações de janela da etapa7 em dados consultáveis e alertas de negócio. Persistir agregados, preservar a identidade em reprocessamento e demonstrar os dois sinks reais, com falhas visíveis. O principal define a arquitetura, orquestra, revisa e valida; agentes GPT-6 Luna com esforço Max implementam. O consumer e a observabilidade da etapa9 não fazem parte desta entrega.

## Alternativas consideradas

| Opção | Vantagem | Custo/limite |
| --- | --- | --- |
| JDBC nativo com UPSERT e KafkaSink AT_LEAST_ONCE — recomendada | Integração direta ao job/checkpoints existentes, sem runtime novo; replay idempotente no banco | Kafka pode repetir um alerta após recovery; identidade estável permite deduplicação no consumer posterior |
| JDBC com UPSERT e KafkaSink EXACTLY_ONCE | Evita duplicatas visíveis para consumers read_committed em recuperação compatível | Transações, prefixos exclusivos e timeouts adicionais; visibilidade depende do checkpoint; não cria atomicidade com PostgreSQL |
| XA e coordenação transacional mais ampla | Garantias adicionais no destino JDBC | XA não está exposto pela API Python documentada; exigiria integração Java/configuração adicional e ainda resolver coordenação com Kafka |

A recomendação segue a prioridade corretude/simplicidade do MVP. Garantias serão descritas por destino, sem declarar uma transação distribuída PostgreSQL/Kafka.

## Fluxo e política de alertas

Preservar o grafo stateful, os UIDs e os três ramos diagnósticos da etapa7. Acrescentar um ramo de negócio após `weather-risk-assessment`, validando o contrato de 21 campos. Não recalcular janelas ou alterar a política `mvp-1`.

Persistir avaliações LOW, MEDIUM, HIGH e CRITICAL em `weather_metrics`. **Proposta de política de publicação:** gerar alertas somente HIGH/CRITICAL, isto é, score >=50. LOW/MEDIUM permanecem consultáveis, sem mensagem de alerta. Não criar cooldown ou supressão entre janelas: cada janela elegível é uma identidade de alerta.

Identidade de avaliação: `(location_id, window_start_ms, window_end_ms, risk_rules_version)`. Versão da regra faz parte da identidade para distinguir recalibrações futuras. Identidade do alerta: SHA256 do JSON canônico UTF-8 de uma lista com esses quatro valores, com ordem fixa e serialização compacta. Sem relógio/RNG na identidade ou no payload.

Mensagem em `weather.alerts`: mesma avaliação21, alterando apenas `operation` para `weather_alert`, mais `alert_id` (22 campos); schema_version1 e componentflink preservados. Kafka key UTF-8 = location_id. Mesmo replay produz mesma identidade e conteúdo. A versão do contrato é distinta da versão da política de risco.

## Localidades e dados persistidos

`climate.locations`: location_id como PK; city, latitude e longitude validados. Seed administrativo das localidades reais existentes RJ01/SP01/CT01, derivado do catálogo atual; não duplicar manualmente coordenadas divergentes. Usuário de aplicação lê a dimensão, sem permissão de alterar seu cadastro.

O ramo de negócio aceita somente localidades configuradas no catálogo operacional. IDs desconhecidos continuam nos diagnósticos anteriores, mas são rejeitados nesse ramo com contador e log estruturado controlado. Não inventar cidade/coordenadas ou registrar localidades QA automaticamente. A inclusão de novas localidades exige cadastro e configuração coerentes antes da operação.

`climate.weather_metrics`: PK composta pela identidade da avaliação, FK para locations; colunas tipadas para estatísticas da janela, timestamps UTC em BIGINT epoch-millis, schema_version, score, classe, versão da regra e componentes JSONB. Sem persistir eventos crus. CHECKs para fronteiras ordenadas, contagem positiva, valores finitos/intervalos válidos e score/classe/componentes coerentes.

`climate.weather_alerts`: alert_id como PK, UNIQUE da identidade da avaliação, FK para a avaliação e somente HIGH/CRITICAL. Guardar a avaliação necessária para explicar o alerta. Índices adicionais limitados a consulta de janelas/alertas recentes por localidade; não indexar todos os campos.

## PostgreSQL: escrita, migração e segurança

Um único JdbcSink nativo recebe cada avaliação validada como Row com JSON compacto. Uma SQL parametrizada usa CTE: UPSERT de weather_metrics com RETURNING, seguido de UPSERT condicional de weather_alerts a partir desse resultado. Isso evita corrida de FK entre sinks independentes e mantém métrica/alerta daquela avaliação na mesma transação SQL. Reexecução não aumenta linhas nem altera identidades. Não pressupor que todo o lote JDBC ou os dois destinos externos constituam uma única transação.

Proposta inicial de batching: até100 avaliações ou2000ms, com flush no checkpoint; até3 retries. Timeouts explícitos de conexão/leitura, dentro do orçamento de checkpoint120s. Exaustão de retries deve falhar o job e acionar o restart limitado existente; nunca descartar silenciosamente uma escrita.

Migração explícita e reaplicável, executada pela identidade administrativa existente em transação, incluindo seed e grants. Também integrar a inicialização de volumes novos. Não depender de scripts initdb para atualizar o volume já existente; não recriar nem apagar o volume. Verificar a estrutura existente e recusar incompatibilidade, em vez de esconder drift com IF NOT EXISTS.

Aplicação recebe somente SELECT na dimensão e SELECT/INSERT/UPDATE nas tabelas de negócio, sem DELETE/DDL/superusuário. Identidade administrativa não entra no Flink. Senha da aplicação via ambiente local, sem argumentos CLI/URL sensível/logs/Dockerfile/Git. JM/TM permanecem UID9999; PostgreSQL sem porta publicada. O acesso JDBC exigirá a credencial de aplicação nos nós necessários, mudança intencional em relação à etapa7.

Usar conector JDBC compatível com Flink1.20 e driver PostgreSQL com versões/checksums fixados, disponíveis no runtime comum dos dois nós. Confirmar artefatos e APIs exatos no plano; nenhuma nova biblioteca Python de acesso ao banco é necessária no job.

## Kafka, recuperação e limites

KafkaSink nativo para o topic já existente weather.alerts, AT_LEAST_ONCE, acks=all e timeouts/retries limitados. Checkpoint habilitado; falha de entrega não pode ser tratada como sucesso. Sem topic/serviço extra. Consumer Python fica na etapa9.

Banco idempotente por chave lógica e UPSERT; Kafka pode repetir a mesma identidade após recovery. Não prometer exatamente uma mensagem nem atomicidade entre PostgreSQL e Kafka. Interrupção pode produzir visibilidade temporariamente diferente entre destinos; a restauração/releitura deve convergir quando o checkpoint e os registros necessários ainda estão disponíveis.

Antes do deploy8, preservar imagem7 e checkpoint7 criado pelo grafo antigo. Restaurar o novo grafo a partir desse snapshot, sem allowNonRestoredState e sem modificar UIDs antigos. Não esperar que janelas já fechadas antes da adição dos sinks sejam publicadas retroativamente: o aceite cobre janelas abertas no checkpoint e novas janelas. Backfill histórico não faz parte desta etapa.

QA legado continua exercitando o pipeline anterior sem tocar nos destinos de negócio por padrão. QA dos novos sinks habilita explicitamente os destinos sob nome/grupo exclusivos, com Producer pausado, localidades reais configuradas e janelas sintéticas exclusivas identificáveis. Cleanup limitado às linhas próprias de QA; não remover registros/offsets/topics/volumes alheios. Esperar terminal antes de reutilizar o único slot.

## Critérios de aceite

1. Migração em volume existente e inicialização de volume novo verificadas; reaplicação preserva dados; constraints/grants/identidade de aplicação comprovados.
2. Testes puros de contrato22, identidade, elegibilidade LOW/MEDIUM/HIGH/CRITICAL, limites e entradas inválidas; testes SDK de Row, SQL parametrizada, serializer key/value, garantias e grafo/UIDs.
3. Kafka→Flink real produz avaliações das quatro classes: todas persistidas, somente HIGH/CRITICAL no banco de alertas e topic; comparar colunas/payloads completos e keys/identidades.
4. Reprocessamento/restauração não duplica linhas no PostgreSQL; duplicatas Kafka, se ocorrerem, preservam alert_id/payload. Falhas JDBC/Kafka não são escondidas; provar recuperação sob deadline.
5. Migração real de checkpoint7 para8 conserva offsets, deduplicação, acumuladores e produz novos sinks a partir de janelas abertas; checkpoint8 novo COMPLETED.
6. Suítes gerais/SDK/integrações anteriores continuam aprovadas; skips por coordenação não contam como PASS. Ruff e build verificam versões corretas.
7. Único job operacional8 RUNNING, métricas persistidas de localidades reais, Producer funcional, checkpoints60s e recursos/backpressure observados sob carga pequena; alertas testados com fixtures controladas, sem afirmar que a API real forneceu risco alto quando não forneceu.
8. Principal faz review de arquitetura/código/segurança/performance, revisões independentes quando apropriado, scan de credenciais e documentação de limites; commits locais sem push.

## Estado observado na preparação

Em2026-10-11, checkout limpo e serviços saudáveis após restart; REST inicialmente sem jobs. Checkpoints40/41 do job7 `3caa432342067923b7edae698657e8d6` preservados. Principal retomou grafo7 pelo checkpoint41, JobID `cf62f651459bb421c75fd589955091ae` RUNNING. Nenhuma tabela, dependência, código de produto ou recurso foi alterado nesta preparação.

## Fontes técnicas verificadas

- [Flink1.20 JDBC DataStream](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/jdbc/): API Python, batching/flush de checkpoint, entrega at-least-once e UPSERT; XA sem API Python documentada.
- [Flink1.20 Kafka DataStream](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/connectors/datastream/kafka/): KafkaSink, serializers, checkpoint e garantias por destino.
- [PostgreSQL17 CTEs](https://www.postgresql.org/docs/17/queries-with.html): comunicação entre operações de escrita via RETURNING.

Próximo passo após aprovação desta spec: plano escrito com tarefas implementáveis, interfaces e ordem de QA; execução por agentes LunaMax após aprovação do plano. Etapa9 não iniciada.

Detalhamento técnico no plano: serializerJava mínimo preserva key/value do KafkaSink nativo; buildJava17 restrito ao builder. UPSERTs idênticos são aceitos; alteração de valores para a mesma identidade é recusada transacionalmente para evitar alerta stale, sem adicionar protocolo de revogação ou backfill. Esses detalhes concretizam o contrato determinístico aprovado.
