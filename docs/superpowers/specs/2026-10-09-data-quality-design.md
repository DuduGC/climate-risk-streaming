# Etapa 5 — Data Quality

Status: especificação aprovada em 2026-10-09; plano/implementação pendentes. Etapas 1–4 concluídas; inspeção inicial sem alterações Git desconhecidas.

## Objetivo

Impedir que eventos inválidos ou duplicados avancem no job Flink, sem quebrar o processamento dos válidos. Reutilizar validação existente, acrescentar coerência temporal e deduplicação com estado gerenciado/TTL. Implementador GPT-6 Luna Max; uma entrega coesa com revisão independente e aceite real.

## Escopo preservado

Producer, schema versão 1, Row com 15 campos, runtime Python 3.11.14/PyFlink 1.20.3 e topic weather.raw permanecem. Sem novo topic/DLQ/serviço/dependência, sinks de negócio, Risk Engine ou janelas/watermarks.

O parser já cobre JSON, tamanho de 16 KiB, UTF-8, campos/tipos/ranges/source/version e timestamps aware. A Etapa 5 não duplicará essas regras. Erros conhecidos continuam descartados e contados; exceção de programação continua falhando visivelmente.

## Regras temporais

Validação pura com relógio UTC injetável para testes. Proposta inicial de limites técnicos inclusivos:

- event_timestamp e ingestion_timestamp devem ser posteriores ou iguais a 1970-01-01T00:00:00Z.
- event_timestamp não pode ultrapassar ingestion_timestamp em mais de cinco minutos.
- ingestion_timestamp não pode ultrapassar o relógio observado pelo operador em mais de cinco minutos.
- ingestion_timestamp menos event_timestamp não pode exceder 24 horas.

Esses limites são qualidade de entrada, não regras de risco meteorológico. Comparar atraso com o horário original de ingestão permite reprocessar Kafka histórico sem rejeitar dados somente porque o job iniciou depois. Não substituir timestamps, zerar valores ou alterar payload para fazer um evento passar.

Campos ausentes/null/bool/não finitos/unidades do wire/schema inválidos continuam usando parser/modelos existentes. Não impor allowlist das três localidades: IDs válidos adicionais/testes continuam suportados, sem cadastro novo nesta etapa.

## Deduplicação

Fluxo: KafkaSource → parsing/validação existente → coerência temporal → keyBy(location_id) → deduplicação stateful → diagnóstico atual.

MapState por localidade: chave event_timestamp normalizado UTC, valor marcador booleano. Primeiro evento válido da identidade é emitido; ocorrências seguintes são descartadas enquanto o marcador estiver vigente. ingestion_timestamp não participa da identidade. Mesmo timestamp/instante com offsets diferentes deve formar a mesma chave.

Eventos de localidades diferentes ou com event_timestamp diferente continuam independentes. Inválido não cria marcador e não impede uma versão válida posterior. Payload diferente com a mesma identidade segue política primeira ocorrência válida vence; revisões da fonte não substituem automaticamente o primeiro evento nesta etapa.

TTL padrão de 24 horas por entrada em processing time, OnCreateAndWrite/NeverReturnExpired. Duplicado apenas consulta o estado, sem renovar TTL. Usar limpeza incremental do backend heap já adotado; não varrer todo MapState a cada registro. Confirmar API real no runtime e testar expiração.

TTL limita o horizonte lógico, não estabelece um teto rígido de memória: volume depende da taxa de identidades únicas. Limpeza física é incremental/best effort e pode aguardar atividade. Estado não deve ser um dict/set Python global sem expiração.

keyBy e estado entram aqui apenas porque a deduplicação os exige. Event Time, watermarks, janelas e checkpointing operacional continuam na Etapa 6. Sem checkpoint, reiniciar o job perde os marcadores; após TTL, reocorrência pode ser aceita novamente. Não alegar exactly-once/deduplicação permanente.

## Métricas e erros

Preservar parsed_records/invalid_records e adicionar aceitos, rejeitados por temporalidade e duplicados, com nomes/semântica documentados. Um rejeitado não deve aparecer como aceito. Diagnóstico de sucesso permanece compatível, agora emitido somente após deduplicação.

Razões temporais controladas por códigos estáveis, sem mensagens contendo payload inteiro. Log de erro/sumário limitado, sem credenciais ou um INFO para cada duplicado normal do polling. Não adicionar Prometheus/Grafana.

## Arquivos e implementação

- src/climate_logistics/data_quality.py: política temporal pura e códigos de rejeição.
- src/climate_logistics/flink_job.py: integração de validação e operador keyed/MapState TTL.
- tests/unit/test_data_quality.py e testes de funções/runtime Flink necessários.
- tests/integration/test_kafka_flink.py: cenários de qualidade/dedup/TTL no cluster.
- README.md/docs/progresso.md: contrato, métricas, operação e limites.

Preservar interfaces do parser/Row e testes existentes, ajustando somente expectativas que passam a envolver deduplicação. Reutilizar imagem/cache; mudanças de recursos somente por medição/falha concreta.

## Testes e aceite

Testes puros: limites temporais e relógio aware, offsets equivalentes, replay histórico com ingestão original, razões de rejeição e ausência de mutação. Não repetir a matriz completa de ranges já coberta.

Testes de operador no runtime PyFlink: validação antes de gravar estado, identidade lógica, entradas independentes, duplicado sem renovar TTL, configuração de 24 h e API real. Uma fake de estado não comprova o mecanismo de TTL do Flink.

Integração bounded isolada: publicar inválidos e válidos, duplicata com ingestão distinta, mesmo timestamp em outra localidade e timestamp novo na mesma localidade; job FINISHED, diagnóstico completo exatamente uma vez por identidade válida e rejeições/duplicados comprovados por métricas ou evidência controlada.

Para TTL, job de teste usa duração curta explicitamente isolada (ex.: 2 segundos), aguarda expiração e republica identidade; provar aceitação novamente e impedir override acidental da política operacional de 24 h. Teste deve distinguir processing time de event time, sem introduzir watermark.

Antes de usar o único slot, inspecionar jobs e coordenar parada somente do operacional conhecido; cancelar somente jobs de teste identificados, restaurando um job operacional ao fim. Sem apagar records/topics/volumes ou resetar grupos alheios.

Ruff/suíte completa Python 3.11 e regressões SDK aprovados, build de código atualizado, integração real e operação contínua de qualidade comprovadas. Verificar Producer funcional e custo de estado sob carga pequena. Registrar ETAPA 5 CONCLUÍDA somente após critérios; Etapa 6 não iniciada.

## Abordagens consideradas

Recomendação: MapState gerenciado por keyBy de localidade, com TTL e política temporal pura. Alternativa dict Python mantém estado fora do Flink e exige limpeza manual. Alternativa último timestamp em ValueState perde duplicatas fora de ordem; não atende a identidade completa. Não adicionar Redis ou armazenamento externo.

## Fonte primária

https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/fault-tolerance/state/ — keyed state, TTL por entrada, processing time e cleanup incremental.
