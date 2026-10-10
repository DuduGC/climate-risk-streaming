# Revisão independente — Etapa 6, Tarefa 1

Data: 2026-10-10. Base da revisão: `b6fe7ec`. Escopo: Tarefa 1 do plano temporal, spec aprovado, relatório do implementador, pacote `.venv/stage6-task1-review.txt` e arquivos atuais correspondentes. Revisão estática; este revisor não executou Docker, testes de cluster, commits nem leu `.env`.

## Parecer

Na revisão inicial foram registrados P1 e P2; a segunda execução real revelou outro P1 de perda do timestamp assigner após aplicar idleness. Os três achados estão endereçados no código atual, com regressões específicas. A terceira rodada real aprovou bounded e avançou o unbounded até uma leitura prematura do contador lazy. A última re-review foi limitada à sincronização dessa leitura e à preservação da prova final de atraso parcial/total, sem mudança de produção. Não há achado remanescente nesse escopo. A nova execução real após o ajuste do teste é responsabilidade do principal e ainda não é contabilizada como aprovada. Checkpoints e recuperação pertencem à Tarefa 2 e não são lacunas desta revisão.

## Achado

### [P1] Passar allowed_lateness como inteiro em milissegundos — endereçado

Arquivo: `src/climate_logistics/flink_temporal.py`, linha 113.

O grafo chama `.allowed_lateness(Time.milliseconds(0))`, mas o operador Python usa esse valor diretamente na soma `window.max_timestamp() + self.allowed_lateness`. Na execução real pelo principal, os dois jobs temporais ficaram FAILED com `TypeError: int + Time` em `PythonWindowOperator.cleanup_time`. A construção serializável do grafo no teste SDK não executa esse caminho; por isso os testes SDK atuais não detectam a falha. Todo evento que cria uma janela pode derrubar o job, impedindo qualquer agregado operacional.

Correção solicitada: usar `.allowed_lateness(0)`, mantendo a política aprovada de zero milissegundos, e acrescentar regressão SDK que confira o tipo/valor efetivo utilizado pelo operador. Reexecutar bounded/unbounded no runtime real após o ajuste. A falha de offsets observada durante o teste não deve ser contornada enfraquecendo o aceite: o job havia falhado no operador temporal.

Re-review: a linha 113 agora usa `.allowed_lateness(0)`. `test_attach_passes_integer_zero_allowed_lateness_to_the_real_sdk`, em `tests/unit/test_flink_temporal_sdk.py:98`, exige `type(milliseconds) is int`, delega ao método original do SDK e confirma valor único `[0]`. O principal informou RED em 4,21 segundos com o argumento anterior `Time`; esta revisão não repetiu o teste. O wrapper preserva a chamada real, portanto verifica o contrato de entrada que originou a falha.

### [P2] Preservar o JobID antes da espera de startup do teste unbounded — endereçado no código

Arquivo: `tests/integration/test_flink_temporal.py`, linhas 324–337, 658 e 838–840.

`_submit_unbounded_qa_job` captura o JobID localmente e depois aguarda o estado RUNNING por até 30 segundos. O chamador só recebe esse ID quando o helper retorna. Se uma consulta REST falhar ou o prazo terminar enquanto o job permanece INITIALIZING/RESTARTING, a atribuição na linha 658 não ocorre e `job_id` permanece `None`. O `finally` deixa então o job QA ativo, ocupando o único slot e impedindo os testes seguintes ou a restauração operacional. Uma falha/timeout do CLI após submissão tem o mesmo risco.

Correção solicitada: compartilhar um holder de submissão com o chamador, registrar o ID imediatamente quando conhecido e, no `finally`, recuperar o ID pelo nome exclusivo do próprio job caso necessário, seguindo o padrão já usado pelo bounded. Cancelar somente esse ID. Cobrir uma exceção após a captura do ID e uma submissão cujo CLI falhe antes de devolver o ID; a verificação pode usar mocks sem cluster.

Re-review: o helper recebe holder externo e grava `submission["job_id"]` antes do polling. `_cleanup_unbounded_qa_job` usa o ID retornado ou guardado e sempre chama `_wait_for_own_job_id(job_name, known_job_id)`, recuperando pelo nome exclusivo se o CLI tiver falhado antes da captura. O `finally` unbounded chama esse cleanup mesmo quando a atribuição de retorno não aconteceu. O cancelamento continua limitado ao ID próprio.

Re-review da segunda wave: `tests/unit/test_flink_qa_helpers.py` agora contém dois testes que executam submit/cleanup em `try/finally`, um com ID capturado antes da falha de startup e outro com erro do CLI antes de qualquer ID. O segundo usa o lookup real por nome, com `_jobs_overview` interceptado no módulo que define esse helper, e inclui um job alheio para exigir cancelamento apenas do próprio. Os mocks antigos foram removidos da integração; não permanece a observação de cobertura anterior. Esta revisão não repetiu a execução.

### [P1] Preservar o timestamp assigner após aplicar idleness — endereçado

Arquivo: `src/climate_logistics/flink_temporal.py`, factory `_weather_watermark_strategy`.

Na segunda execução real, o principal informou duas integrações reprovadas em 119,69 segundos. A configuração efetiva tinha `_timestamp_assigner is None`: `with_idleness` retorna uma nova `WatermarkStrategy` e não conserva o assigner Python configurado anteriormente. Assim, o pipeline usa timestamps de origem Kafka em vez de `event_timestamp`; o bounded viola a fronteira exclusiva esperada e o unbounded avança watermark pelo relógio de publicação.

Correção solicitada: chamar `.with_idleness(...)` antes de `.with_timestamp_assigner(...)`, deixando o assigner por último. A regressão adicionada `test_watermark_strategy_keeps_event_timestamp_assigner_after_idleness` inspeciona a factory real, exige `WeatherTimestampAssigner` presente e confere a extração UTC/milissegundos contra `record_timestamp=99`. Não remover nem enfraquecer as asserções bounded/unbounded para acomodar timestamps Kafka.

Re-review: a factory atual aplica `.with_idleness(Duration.of_seconds(60))` e depois `.with_timestamp_assigner(WeatherTimestampAssigner())` como última chamada. O teste SDK verifica diretamente `_timestamp_assigner` da estratégia final, sua classe e resultado de extração; assim cobre a perda de configuração que os testes isolados do assigner não detectavam. O principal/relatório do implementador informam RED causal antes da troca de ordem. O contrato e as asserções reais de fronteira e watermark foram preservados. Não há achado remanescente nesta correção estática.

## Contratos e caminhos conferidos

- `window_metric` produz exatamente os 17 campos aprovados, sem soma interna de temperatura ou metadados arbitrários. A média usa soma/contagem por observação; chuva usa máximo e projeção individual `precipitation × 3600 / measurement_interval_seconds`, sem acumulado de janela/hora.
- `MetricAccumulator` é uma dataclass frozen com 11 escalares. Adição e merge devolvem acumuladores sem listas de eventos; entrada não é modificada, vazio é identidade de merge e finalização vazia é recusada.
- `timestamp_millis` usa datetime aware convertido para UTC e aritmética inteira de timedelta. Trunca frações inferiores a milissegundo; as fronteiras da janela continuam inteiros, sem formatar datetime fora do ano 9999. Deduplicação conserva a precisão original no ramo anterior.
- A atribuição temporal ocorre depois de validação/dedup e antes do novo keyBy por `location_id`. O strategy configura desordem de 120 segundos e idleness de 60 segundos; o ambiente configura emissão de 200 ms. Paralelismo permanece 1, com watermark do fluxo combinado.
- O grafo usa `SlidingEventTimeWindows` real de 1800000/300000 ms e agregação incremental com acumulador pickled. A política de `allowed_lateness=0` é a aprovada, com tipo corrigido na re-review P1. O ProcessWindowFunction recebe o agregado e as fronteiras, sem bufferizar eventos crus. A limpeza de estado permanece no lifecycle da janela Flink; não foi acrescentado TTL de janela.
- O side output usa o tipo Row do stream existente. Seu consumidor incrementa `temporal_stream.late_records`, devolve apenas BOOLEAN e alcança o DiscardingSink Java. O contador não cria tópico nem registra o payload completo. A distinção entre atraso parcial e totalmente atrasado está documentada e testada.
- As cinco UIDs novas correspondem ao plano; as UIDs anteriores, Row15, esquema raw, validação/dedup, cache de estado Python desabilitado e ramo `weather-ingestion` foram preservados. As flags/identidades QA existentes não sofreram alteração.
- Os testes puros verificam contrato completo, merge, entradas intactas, UTC, precisão e timestamp máximo. Os testes SDK usam o assigner real para seis janelas e limite exclusivo, inspecionam parâmetros reais do watermark e constroem grafo serializável com ambos os sinks.
- A integração bounded usa localidades exclusivas e oracle completo, seis participações, duplicata revisada, desordem e fronteira exclusiva. A unbounded usa RUNNING/offsets, ACK de publicação, watermark REST, deadlines, ociosidade de 65 segundos, sentinel em outra localidade, atraso parcial e totalmente atrasado sem revisão de resultados. Ambos os cleanups agora preservam o ID antes da espera e recuperam o job próprio pelo nome exclusivo quando necessário.
- Os helpers e cenários legados de `test_kafka_flink.py` permanecem intactos. O publisher temporal descartável permite fixtures com o Producer persistente parado; a renomeação do teste SDK elimina a colisão de basename sem mudar a coleta global. README descreve limites e unidades temporais de forma consistente.

## Evidências de execução atribuídas ao principal

O principal informou execução fresh aprovada de 8 testes puros, 15 testes no runtime SDK Python 3.11/PyFlink 1.20.3 e suíte leve completa com 369 testes aprovados, além de Ruff aprovado. Os dez avisos SDK são de dependências já instaladas, conforme relatório do implementador. Esta revisão não repetiu essas execuções.

O principal concluiu as duas integrações temporais reais: **2 failed em 75,37 segundos**, com ambos os jobs FAILED por `PythonWindowOperator.cleanup_time` somar inteiro e `Time`. Essa evidência fundamenta o P1 acima e prevalece sobre a aprovação da construção de grafo SDK. Após as correções P1/P2, o principal deve registrar a regressão de lateness, a verificação do cleanup e o resultado real bounded/unbounded antes do gate da Tarefa 1.

A segunda execução real terminou **2 failed em 119,69 segundos**, segundo o principal, e fundamentou o achado da factory de watermark; os dois mocks de helper presentes naquela rodada passaram. A re-review final confirmou a correção da ordem da factory e os dois failpaths no arquivo unitário próprio. O relatório do implementador registra 2/2 testes de cleanup locais sem serviços e RED causal da retenção do assigner. GREEN SDK/suíte leve/Ruff, rebuild e nova rodada real após essas mudanças ainda serão registrados pelo principal. Esta revisão não repetiu execução de testes ou serviços.

## Re-review da sincronização do contador lazy — terceira rodada

O principal informou **1 passed, 1 failed em 118,44 segundos** na terceira execução real: bounded comprovou a fronteira exclusiva de C; unbounded observou watermark de Event Time e 65 segundos ociosos sem métricas. O restante falhou ao consultar `late_records` antes da primeira entrada do consumidor Python registrar a métrica. A revisão desta wave ficou restrita a `tests/integration/test_flink_temporal.py` e ao registro correspondente no relatório do implementador.

O teste atual remove apenas as leituras antecipadas de contador zero durante idle/após C. `_wait_for_late_count` preserva deadline de 30 segundos e valor exato esperado; tolera somente erros temporários de registro/valor ausente, sem converter ausência em zero, sem skip e sem retornar sucesso no timeout. Erros de outro tipo continuam propagando; uma métrica que não aparece ou não chega a 1 reprova o teste com o último valor/erro observado.

A prova de C permanece no oracle completo `expected_all`: C produz exatamente a janela ainda aberta, com todos os campos esperados, enquanto as janelas antigas permanecem inalteradas. Depois de D, a asserção exige contador real igual a 1. Como o contador vale para todo o job QA, esse total também reprova contagem indevida do parcialmente atrasado C. A comparação completa antes/depois de D exige ausência de reabertura/revisão das janelas fechadas. As dimensões temporais, fontes, ACKs, watermarks REST e cleanup próprio permanecem iguais.

Não há achado remanescente na sincronização ou na prova final inspecionada. O aceite completo da integração unbounded depende da próxima execução do principal; este revisor não repetiu testes, Docker nem investigação de produção.
