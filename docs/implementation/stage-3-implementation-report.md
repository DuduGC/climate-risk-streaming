# Etapa 3 — relatório de implementação

Status: implementação, integração Kafka/API→Kafka e build aprovados pelo principal. Verificações operacionais finais e commit local permanecem com o principal; este relatório não declara a Etapa 3 concluída.

## Entrega

- Implementados o publisher Kafka e o CLI contínuo/--once, reutilizando WeatherEvent, LOCATIONS e OpenMeteoClient da Etapa 2.
- O publisher serializa o schema existente em JSON compacto UTF-8, valida 16 KiB e usa ingestion_timestamp como CreateTime. Callbacks por mensagem mantêm contagens de ACK, inclusive para identidades repetidas; nack e falta de callback tornam o lote uma falha.
- Startup cria somente tópicos ausentes e valida partições/fator de replicação dos dois tópicos sob um prazo compartilhado de 15 segundos. Retry de fila é limitado por deadline monotônico de cinco segundos; flush de ciclo e fechamento usam até 12 segundos.
- Adicionados container Python 3.11 não-root e serviço Producer no Compose, mantendo os quatro serviços existentes, sem porta nem credenciais PostgreSQL.
- Dependências runtime e de desenvolvimento foram separadas. README e .env.example documentam defaults públicos e execução operacional.

## TDD e verificação offline

- RED inicial observado: 32 testes unitários falharam porque as interfaces Etapa 3 ainda não existiam.
- GREEN inicial observado: 32 testes unitários passaram após implementação.
- RED da revisão observado: três regressões reproduzidas — retry após poll além do deadline, perda da contagem de entregas em nack parcial e resumo CLI incorreto para dois ACKs e um nack. Os três passaram após as correções; os testes também verificam nack servido por poll.
- Verificação final em Python 3.11.14: python -m pytest -q — 329 passed, 4 skipped.
- Verificação final Ruff: python -m ruff check . — All checks passed.
- Os testes de integração Kafka/CLI são opt-in e foram pulados nessa execução padrão.

## Aceite de integração executado pelo principal

- Fixture Kafka real, teste do SDK contra porta TCP fechada e smoke da API Open-Meteo via CLI --once foram executados com consumo e offsets verificados: 3 passed em 13,30 segundos.
- Build final da imagem Producer passou e o serviço contínuo foi iniciado.
- Permanecem com o principal as verificações de parada/reinício, usuário não-root, ausência de credenciais PostgreSQL no container e commit local.
Nenhum arquivo .env real foi lido ou alterado.