# Etapa 3 — revisão independente

Data: 2026-10-08. Base informada: efdd57e. Revisão de produto somente leitura, seguida de reavaliação limitada às correções solicitadas.

## Parecer final

APROVADO na revisão de código da Etapa 3. Os cinco achados R1–R5 estão resolvidos; não resta achado impeditivo no escopo revisado. A aprovação de código não substitui a conclusão, pelo principal, do rebuild e dos testes finais de operação contínua/SIGTERM, que estavam em curso no encerramento desta revisão.

## Escopo e evidências

Lidos brief/spec aprovados, publisher, CLI, novos testes unitários e integração, Dockerfile, Compose, requirements, .dockerignore e README. Nenhum segredo/.env foi lido e nenhuma configuração Docker interpolada foi exibida. Não houve edição de produto pelo revisor.

A revisão inicial conferiu ACK versus enqueue, callbacks de erro atendidos por poll, contagem por ocorrência (inclusive mesmo ID), limpeza de estado por lote, prazos de Admin/fila/flush, topologia sem mutações destrutivas, HTTP parcial, stop/event wait/finally, configuração pública e isolamento Docker.

Na revisão inicial, reprodução offline independente em container python:3.11.14-slim-bookworm, rede desabilitada e workspace somente leitura, confirmou R4: após BufferError, poll avançava o relógio injetado de 0 para 6 e uma segunda tentativa era aceita. Também foi constatado logger INFO desabilitado sem configuração externa. Esses resultados descrevem a versão anterior às correções.

Na reavaliação foram inspecionados os trechos corrigidos e seus testes de regressão. O principal comunicou evidência de Python 3.11: 329 testes aprovados, 4 skips opt-in e Ruff exit 0. Também comunicou 3 testes de integração aprovados em 13,30 s: fixture Kafka, falha real do SDK em porta TCP fechada e CLI/API Open-Meteo com consumo dos três eventos. Essas execuções são do principal; não foram duplicadas pelo revisor.

## Achados e resolução

| ID | Prioridade original | Local | Problema original e correção verificada | Estado |
| --- | --- | --- | --- | --- |
| R1 | P1 | src/climate_logistics/kafka_publisher.py, ensure_topics | Snapshot artificial de um único tópico era passado ao helper que exigia ambos. A correção usa _verify_topic para cada tópico existente e _verify_topics para o conjunto final. O teste existing_compatible_topics_are_left_untouched cobre reinício sem mutação; a integração repetida informada pelo principal também passou. | RESOLVIDO |
| R2 | P2 | src/climate_logistics/producer.py, main | CLI descartava logs INFO. main agora chama _configure_logging, instala StreamHandler de stdout com nível INFO e mantém mensagens JSON sem prefixo. test_main_emits_parseable_json_logs_to_stdout chama main sem caplog forçando INFO e exige resumo de sucesso parseável. | RESOLVIDO |
| R3 | P2 | README.md, comando de ciclo único; docker/producer/Dockerfile, CMD | --once substituía todo o CMD sem ENTRYPOINT. README agora usa docker compose run --rm --no-deps producer python -m climate_logistics.producer --once, coerente com o CMD exec da imagem. | RESOLVIDO |
| R4 | P2 | src/climate_logistics/kafka_publisher.py, KafkaPublisher.enqueue | Retry ocorria depois do deadline consumido por poll. A correção confere relógio antes de cada retry. test_queue_full_does_not_retry_after_poll_passes_deadline avança relógio para 6 s e exige apenas uma tentativa e nenhuma mensagem aceita. | RESOLVIDO |
| R5 | P2 | src/climate_logistics/kafka_publisher.py, KafkaPublishError/_flush; src/climate_logistics/producer.py, run_cycle | Resumo perdia ACKs anteriores ao NACK. KafkaPublishError agora transporta delivered_count; erros de flush preservam self._delivered e run_cycle usa essa contagem no resumo failed. Testes conferem duas confirmações e uma falha tanto no publisher quanto na saída JSON da CLI com exit 1. | RESOLVIDO |

## Demais conclusões do código examinado

- Callback NACK fixa erro do lote e não é apagado ao terminar poll; flush zero sem callback não conta como sucesso. A regressão de NACK atendido durante poll também foi inspecionada.
- O estado usa contadores por lote, sem histórico crescente; duas ocorrências da mesma identidade recebem callbacks separados.
- Admin passa prazo restante global para metadata/create/futures; a verificação corrigida rejeita partições/RF incompatíveis antes de criar tópicos ausentes. Não há delete ou alteração de partições.
- O publisher usa acks=all, idempotência, gzip, linger de 20 ms, batch de 32768 bytes e delivery.timeout de 10000 ms. Flush por ciclo e final usa 12 s. Não há reenvio manual após timeout de delivery.
- O ciclo continua após erro HTTP isolado; Kafka fatal encerra. finally fecha HTTP e publisher, mantendo saída de falha. Event.wait permite interromper o intervalo.
- Docker usa Python 3.11.14, apenas requirements runtime, UID/GID 10001, CMD exec e cópia explícita de src. Compose não adiciona portas ao Producer, limita 256 MiB/0,5 CPU e reinicia on-failure:3 com 75 s de grace period; environment explicitamente público.
- Integração fixture captura high watermarks e usa assign com commit automático desabilitado. O observer corrigido atualiza metadata e repete apenas erros transitórios de tópico/líder dentro de prazo global de 15 s compartilhado pelas três partições. Erros não transitórios são propagados.
- Smoke real verifica ingestion_timestamp dentro da janela do subprocess e CreateTime. A parada do producer contínuo antes desse smoke é requisito operacional acompanhado pelo principal.

## Limites

Reavaliação restrita à onda acordada de correções e ao observer de offsets. Sem expansão para hardening, transações, exactly-once, frameworks ou etapas 4–10. Build final, encerramento real por sinal, atualização do progresso e aceite completo da etapa permanecem sob responsabilidade do principal.