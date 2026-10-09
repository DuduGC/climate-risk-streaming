# Revisão independente — Etapa 4 Kafka → Flink

Revisor: agente stage4_review. Data: 2026-10-09. Base informada: c6b07a5.

## Escopo e método

Inspeção read-only do brief, especificação aprovada, weather_parser.py, flink_job.py, modelos existentes, Dockerfile/checksums, requirements-flink.txt, compose.yaml, .dockerignore, testes unitários/integração e README. A implementação estava sendo finalizada em paralelo; lacunas documentais transitórias não foram tratadas como defeitos. Nenhum código de produção, configuração expandida ou segredo foi alterado/lido nesta revisão.

Também foi inspecionada, sem submissão de jobs, a implementação instalada de StreamExecutionEnvironment.execute e JobExecutionResult.get_job_id no container Python 3.11. O principal é responsável por build, suíte/Ruff e provas de cluster, evitando duplicação de jobs/testes. Após a falha real abaixo, a revisão adicional ficou restrita à API de métricas e aos ajustes de assert/cleanup da integração.

## Achado P1 resolvido — API de métricas impedia inicialização do worker

Em src/climate_logistics/flink_job.py, _ParseWeatherEvent.open chamou runtime_context.get_metric_group(), que não existe no StreamingRuntimeContext do PyFlink 1.20.3. A API instalada é get_metrics_group(), no plural. Todo job que inicia esse operador falha antes de processar registros; a aprovação por testes puros do parser não cobre esse caminho.

Evidência reportada pelo principal: job bounded 13277560fd9bc8de45a98b16feda94d5 terminou FAILED com erro no open do worker. A introspecção do RuntimeContext instalado confirmou o método plural. O principal mediu cerca de 456 MB no TaskManager e 441 MB no JobManager; essa falha foi de API, sem evidência de OOM.

Correção confirmada na releitura final: a linha 47 de flink_job.py usa get_metrics_group(), correspondente à API instalada inspecionada pelo revisor. O principal informou regressão no runtime real aprovada (1 passed) e integração bounded real aprovada (1 passed em 18,06 s), com FINISHED, diagnóstico completo da fixture e log estruturado de invalid_records. O P1 está resolvido.

## Resultado da inspeção restante

- O parser exige exatamente os 15 campos do wire schema, aplica limite de 16 KiB, converte timestamps aware para UTC e reutiliza WeatherEvent. Exceções conhecidas de decode/contrato viram WeatherParseError; erros inesperados da construção continuam propagando.
- As strings decodificadas também passam por validação UTF-8, cobrindo surrogates JSON escapados antes da serialização do worker. Os dois testes adicionados cobrem city/location_id.
- Ordem dos 15 campos e tipos ROW_NAMED são explícitos. Coordenadas/medições são DOUBLE; códigos/intervalo/versão são INT; identidade/source/timestamps são STRING. Os modelos garantem limites dos inteiros e números finitos.
- O wrapper captura somente WeatherParseError e registra JSON mínimo sem payload; erros de programação permanecem fatais. A saída de sucesso contém apenas o resumo diagnóstico definido.
- KafkaSource usa tópico explícito, earliest, auto-commit desabilitado, paralelismo 1 e no_watermarks. Não foram introduzidos estado temporal, janelas, deduplicação, JDBC ou sink de negócio.
- JM/TM compartilham imagem, Python/PyFlink e caminhos de executáveis. CPython e conector são verificados por SHA-256 antes do uso. O JAR é instalado uma vez em /opt/flink/lib, sem add_jars duplicado. O runtime termina com USER flink e não recebe credenciais PostgreSQL.
- A integração usa host como coordenador, argumentos estruturados e stdin para payloads; não monta socket nem apaga tópicos, registros, volumes ou offsets. Nome/group de teste são exclusivos; jobs existentes provocam skip para coordenação explícita.

## Correções acompanhadas e releitura limitada

1. Submissão inicial por python -m podia executar ambiente local em vez do cluster de sessão. O teste usa agora /opt/flink/bin/flink run -d -py; o principal comprovou submissão detached e obtenção do JobID nativo/REST no job acima. A falha posterior do worker não invalida essa prova de submissão.
2. Atendido na releitura: o teste extrai objetos JSON dos logs, exige o prefixo weather-ingestion do PrintSink e compara o diagnóstico completo da fixture, incluindo component, operation, status, location_id, timestamps e versão. A evidência de erro inválido também é verificada como JSON estruturado.
3. Atendido na releitura: o helper preserva o ID observado em submission antes de possíveis falhas. O finally prefere esse ID para cancelamento e só redescobre por nome exclusivo se ausente, com espera limitada a 10 segundos. Os comandos de cancelamento continuam restritos ao job próprio. Indisponibilidade do REST ou submissão remota além dessa janela ainda exige inspeção operacional após falha; matar docker exec no host não garante encerrar o processo remoto.
4. O teste deliberadamente não cancela/restaura automaticamente o job operacional. Essa coordenação cabe ao principal/operador no único slot.

A releitura final do README confirmou build da imagem compartilhada, inspeção prévia de jobs, submissão detached como usuário flink com nome/grupo operacionais, consulta por JobID, cancelamento por ID identificado, limitação de um slot e coordenação explícita da integração. A documentação explica earliest, releitura sem checkpoint e ausência de sinks de negócio/JDBC. Os comandos são coerentes com a submissão comprovada no cluster.

Revisão de código aprovada: nenhum achado P1/P2 permanece aberto. A suíte leve/Ruff informada pelo implementador foi 346 passed, 6 skipped e Ruff sem erros; a verificação final independente e o aceite operacional continuam sob responsabilidade do principal.

## Evidências necessárias para fechamento

A integração bounded e a regressão de runtime já foram informadas como aprovadas pelo principal. Para fechar a etapa, o principal ainda registra sua suíte/Ruff independente, confirmação de processos não-root, ausência de secrets nos nós Flink, recursos e um único job operacional RUNNING processando evento posterior à submissão. A aprovação desta revisão não antecipa o aceite operacional final da etapa.