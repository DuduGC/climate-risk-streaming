# Revisão independente — Etapa 5 Data Quality

Revisor: stage5_review. Data local: 2026-10-09. Base: 0adc4f6.

Parecer final: aprovado, sem bloqueadores P1/P2 abertos no escopo da Etapa 5.

## Escopo e método

Revisão estática do brief, especificação, diff final, data_quality.py, flink_job.py, modelos/parser existentes, testes puros/SDK/integração e README. Nenhum código de produção, configuração operacional ou relatório anterior foi alterado pelo revisor. Não houve leitura de .env ou configuração expandida. Build, testes e provas live abaixo foram executados e informados pelo principal; não foram repetidos pelo revisor.

## Achados resolvidos

1. **P1 — tipo incorreto da opção de bundle QA**, em `src/climate_logistics/flink_job.py`, `build_pipeline`. A versão inicial atribuía a string `1000ms` a `python.fn-execution.bundle.time`. O principal confirmou `IllegalArgumentException`/`NumberFormatException` ao consultar `PythonOptions.MAX_BUNDLE_TIME_MILLS` no Java do runtime 1.20.3. A versão final usa `set_integer(..., 1000)` e `env.configure(execution_configuration)`. A regressão SDK lê a opção Java efetiva. Resolvido.
2. **P1 — marcador expirado retornado pelo cache Python**, em `src/climate_logistics/flink_job.py`, `build_pipeline`. Com TTL de 20 segundos e configuração padrão, o principal reproduziu uma cópia ainda tratada como duplicada cerca de 24,6 segundos após a primeira aceitação. A versão final desabilita `python.state.cache-size` em todos os jobs, inclusive o operacional. A regressão SDK verifica a opção Java tanto em QA quanto na configuração operacional; a integração live posterior comprovou expiração. Resolvido.

## Contrato e implementação conferidos

- A política compara diferenças entre instantes UTC, aceita epoch, cinco minutos e 24 horas inclusive e rejeita ultrapassagem. Evita somar tolerâncias ao datetime máximo. Replay usa ingestão original; não há limite por idade relativa ao início do job.
- O relógio é validado antes das regras do evento. Erro de relógio não vira DataQualityError; o wrapper captura somente erros conhecidos de parsing e qualidade. Eventos rejeitados não chegam ao estado.
- O parser e event_values fornecem timestamps UTC canônicos. key_by usa location_id e o MapState usa o instante como STRING, com BOOLEAN como marcador. Ingestão e revisão de payload não mudam a identidade; a primeira ocorrência válida vence.
- TTL padrão de 86.400 segundos, OnCreateAndWrite, NeverReturnExpired e cleanup incremental(10, True) estão configurados no descritor. Duplicatas somente consultam o marcador e incrementam contador; não executam put. Não existe histórico global Python de identidades nem varredura integral por registro.
- parsed_records conta o schema aceito antes da política; invalid_records inclui temporalidade; os contadores de decisão são incrementados nos respectivos caminhos. Logs de erro/auditoria têm campos controlados, sem corpo completo. INFO por duplicata está restrito à auditoria QA.
- execute valida nome/grupo exclusivos antes de construir/submeter um job com flags QA. O TTL curto exige auditoria, faixa 2–60 e identidade QA correspondente. Não há override por ambiente. Bundle reduzido e source latest são exclusivos de QA não bounded; operacional e bounded preservam earliest. A mudança de source QA está registrada como decisão de isolamento do backlog.
- Schema/Row de 15 campos e diagnóstico de sucesso foram preservados. Estado/keyBy atendem à deduplicação; não foram antecipados Event Time, watermarks, janelas, checkpoint, Redis ou sinks de negócio.

## Integração e evidência final

O bounded de qualidade usa localidades exclusivas, inclui rejeitado temporal antes da cópia válida da mesma identidade, offset equivalente com ingestão/payload revisados, outra localidade e outro instante. Exige FINISHED e compara multiconjuntos dos JSON completos do PrintSink e das decisões auditadas, cobrindo exatamente três aceitos, uma rejeição temporal e uma duplicata. A fixture de JSON inválido também tem evidência controlada de descarte.

O teste TTL usa fixture histórica de 2020, TTL de 20 segundos, observação da aceitação antes de programar a duplicata e margens explícitas para a segunda aceitação. Assim, não mede TTL a partir da submissão. O cleanup atua somente no JobID próprio, com redescoberta limitada pelo nome exclusivo em falha. Jobs alheios exigem coordenação; skips não foram considerados prova de integração pelo principal.

Resultados informados pelo principal na imagem final: suíte leve Python 3.11.14 com **361 passed, 15 skipped**, SDK PyFlink 1.20.3 com **8 passed**, Ruff aprovado, build atualizado e integrações reais com **3 passed em 74,65 s**. Os skips da suíte leve não substituíram as execuções SDK/live.

Prova TTL, em 2026-10-10 UTC: aceitação às 00:17:44.368, duplicata às 00:17:56.444 e nova aceitação às 00:18:09.865. A última ocorreu 25,497 segundos após a primeira e 13,421 segundos após a duplicata: depois do prazo original de 20 segundos e antes do prazo que uma renovação criaria. Essa evidência fecha a falha de cache e a ausência de refresh no cenário real.

Prova operacional informada: fixture `QA-OP-88606be90ad4` com uma saída e duplicate_records de 41 para 42 na segunda cópia. Job `27e5e15139ad82f8beb22c40025c95bf` RUNNING com TTL 86.400; TaskManager cerca de 710 MiB, JobManager cerca de 292 MiB, serviços healthy e execução como flink sem credenciais PostgreSQL.

O README descreve razões/métricas, replay, primeira válida vence, TTL em processing time, limpeza best effort, perda de marcadores em restart e ausência de teto rígido de memória. A revisão não interpreta a carga pequena como prova de capacidade geral. Não resta achado concreto bloqueante na entrega revisada; fechamento documental/commit ficam com o principal.
