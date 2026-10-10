# Etapa 5 — aceite técnico

Implementado: coerência temporal pura, identidade localidade/instante UTC, deduplicação por MapState BOOLEAN com TTL de 24 horas e métricas de decisões. A primeira ocorrência válida vence; eventos rejeitados não criam marcadores. QA exige nomes/grupos exclusivos.

## Verificação final

- Python 3.11.14: 361 passed, 15 skipped. Os skips correspondem a gates opt-in e SDK indisponível nesta imagem; não contam como integração aprovada.
- SDK PyFlink 1.20.3 na imagem final: 8 passed (10 avisos de dependências), incluindo leitura Java das opções bundle e state cache.
- Ruff: All checks passed. Build da imagem aprovado e serviços saudáveis.
- Kafka/Flink reais: 3 passed em 74,65 s, sem skips. Bounded confirma rejeição e exatamente três identidades aceitas; teste legado preservado.
- TTL real20 s, identidade histórica de 2020: accepted 2026-10-10T00:17:44.368Z, duplicate 00:17:56.444Z, accepted 00:18:09.865Z. Segunda aceitação 25,497 s após a primeira e 13,421 s após a duplicata, comprovando expiração em processing time e ausência de refresh.
- Operacional: job `27e5e15139ad82f8beb22c40025c95bf`, `weather-kafka-ingestion`, RUNNING, TTL86400. Duas publicações da identidade `QA-OP-88606be90ad4` produziram uma única saída; contador duplicate_records passou de41 para42.
- Producer: ciclo real com collected3/accepted3/delivered3/failures0.
- Memória observada sob esta carga pequena: TaskManager710,1 MiB/1536 MiB; JobManager292,1 MiB/1024 MiB. Não constitui benchmark nem teto de estado.
- UID/GID Flink9999; configuração PostgreSQL ausente nos dois containers Flink. Volumes/dados preservados.

## Problemas e correções

A opção bundle.time de QA exige milissegundos numéricos; corrigida e protegida por regressão Java. O primeiro teste TTL tinha janela estreita para overhead Docker: QA passou para20 s mantendo prova antes do deadline hipotético renovado.

O teste real revelou um marcador ainda visível após expiração. No SDK instalado, o caminho de desativação de cache por TTL escreve `_max_entries`, enquanto Beam2.48 controla a capacidade por `_max_weight`. A correção usa a opção pública `python.state.cache-size=0` em todo pipeline, sem modificar bibliotecas, e foi confirmada por regressão SDK e pelo teste real que antes falhava. A opção é documentada na [configuração oficial Flink1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/zh/docs/dev/python/python_config/). Custo possível: mais acessos ao backend; throughput não foi medido nesta etapa.

A prova operacional aguardou a disponibilização das métricas do worker; a primeira consulta antecipada não encontrou counters, sem publicar fixture nem cancelar o job. A segunda execução reutilizou somente o JobID conhecido e completou a prova.

## Decisões e limites

TTL limita a janela lógica; limpeza física incremental/best effort. Sem checkpoint, restart perde os marcadores. Não há deduplicação permanente nem garantia exactly-once. QA unbounded começa em latest para isolar seu próprio experimento; operacional e bounded preservam earliest. Sem novos tópicos, dependências ou mudanças de recursos.

Etapa6: Event Time, watermarks, janelas e checkpointing; não iniciada. Revisão independente aprovada, sem P1/P2 abertos; diff e scan de credenciais aprovados, registrados no ledger. Etapa 5 concluída.
