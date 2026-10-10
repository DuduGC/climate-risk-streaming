# Etapa 5 — relatório de implementação

Status desta versão: implementação, suíte offline, integração real e verificação operacional concluídas; revisão/aceite final do principal pendentes.

## Entrega

- Adicionada a política temporal pura com quatro razões estáveis e identidade por `location_id` + timestamp UTC canônico.
- O parser valida o tempo antes de emitir a `Row`; um erro temporal incrementa `invalid_records` e `temporal_rejected_records`. Erros de relógio/configuração continuam propagando como `ValueError`.
- O fluxo agrupa por localidade e usa `MapState` com timestamp UTC e marcador booleano, TTL por entrada e leitura sem escrita para duplicatas. O diagnóstico continua após a deduplicação.
- Adicionados os counters `accepted_records`, `duplicate_records` e `temporal_rejected_records`, além da auditoria JSON mínima e flags QA restritas a nomes/grupos exclusivos.
- Integrações bounded e TTL real foram estendidas com eventos inválidos, replay histórico, identidade revisada em offset equivalente e verificação de expiração sem refresh.

## RED/GREEN e verificação

- Política temporal: RED com 14 falhas porque o módulo ainda não existia; GREEN com 15 testes aprovados em `python:3.11.14-slim-bookworm`.
- Operador SDK: os testes RED inicialmente falharam pela ausência do operador/contrato. A suíte final passou com 8 testes na imagem `climate-logistics-flink:local` (CPython 3.11.14, PyFlink 1.20.3).
- A opção QA `python.fn-execution.bundle.time` foi corrigida para o valor numérico `1000` em milissegundos; o teste lê `PythonOptions.MAX_BUNDLE_TIME_MILLS` na configuração Java real.
- A leitura Java real mostrou `PythonOptions.STATE_CACHE_SIZE=1000` por padrão. O pipeline agora configura `python.state.cache-size=0` para execuções operacionais e QA. O teste SDK confirma o valor zero nas duas configurações. A prova live inicial mostrou uma identidade de TTL 20 s ainda duplicada após 24,6 s com o cache padrão; após desativar o cache, a repetição confirmou a expiração e a aceitação seguinte.
- Suíte leve final após a configuração de cache: **361 passed, 15 skipped**, em Python 3.11.14-slim-bookworm. Os skips incluem os testes SDK que requerem PyFlink e as integrações opt-in. Os 346 testes anteriores permanecem aprovados; os 15 novos testes puros também passaram.
- Ruff: **All checks passed**. `git diff --check` passou sem erros de whitespace; Git emitiu apenas avisos de conversão LF/CRLF nos arquivos tocados.

## Integração real e pontos pendentes

A primeira rodada bounded/TTL concluiu 2 testes e falhou na asserção de janela do teste TTL: o audit da duplicata chegou a 9,47 s quando a prova original de 10 s exigia no máximo 9 s. A cópia foi descartada corretamente. A janela foi ampliada para TTL de 20 s, duplicata programada em 10 s e replay após 23 s, preservando pelo menos três segundos até a expiração original e dois segundos antes do deadline hipotético com refresh.

Após rebuild com `python.state.cache-size=0`, os **3 testes de integração reais passaram em 74,65 s**. Em 2026-10-10 UTC, o teste TTL aceitou um evento histórico às 00:17:44.368, rejeitou a cópia às 00:17:56.444 (12,076 s depois) e aceitou a republicação às 00:18:09.865 (25,497 s após a primeira aceitação e 13,421 s após a duplicata). A republicação ocorreu depois do deadline original de 20 s e 6,579 s antes do deadline que a duplicata criaria se renovasse o estado. A auditoria também confirmou o bounded de qualidade; a integração compara os diagnósticos e as decisões por identidade.

Na prova operacional, o fixture `QA-OP-88606be90ad4` produziu uma saída; `duplicate_records` passou de 41 para 42 após a segunda cópia. O job `27e5e15139ad82f8beb22c40025c95bf` permaneceu `RUNNING` com TTL operacional de 86.400 segundos.

Limites da entrega: o TTL usa processing time e limita a janela lógica, sem prometer remoção física imediata ou teto rígido de memória. Sem checkpoint, reiniciar o job perde os marcadores. Após o TTL, uma identidade pode ser aceita novamente. Event Time, watermarks, janelas e checkpointing continuam fora desta etapa.
