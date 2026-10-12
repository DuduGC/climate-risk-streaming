# Etapa 9 — Task 1 report

## Implementação

- `business_records.py` ganhou `parse_alert`, `alert_fingerprint` e `AlertIdCache`. O parser aplica o limite UTF-8 de 16 KiB, rejeita chaves JSON duplicadas e valida o contrato de 22 campos reutilizando `_validate_assessment`, `make_alert` e a identidade/hash existentes.
- O fingerprint usa JSON compacto ordenado e normaliza somente os seis campos meteorológicos para `float`. O cache FIFO retém apenas ID e fingerprint; duplicatas não renovam a idade e divergências não substituem o valor válido.
- `alert_consumer.py` escreve JSON em uma linha, inclui timestamp UTC e faz flush sem suprimir falhas. O handler verifica tópico, erro Kafka, chave UTF-8, catálogo e payload; emite um evento seguro antes do commit e só lembra alertas novos depois do commit. Eventos de alertas válidos usam o mesmo campo `alert` para expor ID, localidade, janela e risco em recebidos, duplicados e divergentes. `commit_message` rejeita resultados sem a confirmação de topic/partition/offset+1 e qualquer `.error` retornado.
- Os testes novos cobrem contratos válidos/inválidos, equivalência numérica e de ordem JSON, divergência/evicção, segurança dos eventos, falha de emissão e confirmação parcial do commit.

## Evidência TDD e verificações

- RED inicial: `python -m pytest tests/unit/test_alert_records.py tests/unit/test_alert_consumer.py -q` — 37 falhas esperadas por ausência das interfaces da Task1.
- RED de regressão: `python -m pytest tests/unit/test_alert_consumer.py::test_emit_failure_on_invalid_key_is_not_retried_or_committed -q` — falhou com duas chamadas a `emit` em vez de uma; o fluxo foi ajustado para deixar a falha propagar sem reemitir.
- RED do formato observável: `python -m pytest tests/unit/test_alert_consumer.py -q` — 3 falhas esperadas por falta do timestamp UTC e do campo `alert` nos eventos duplicado/divergente; ambos agora seguem o mesmo formato dos eventos recebidos.
- GREEN final, com `.venv/flink-control/Scripts/python.exe` (Python 3.12.14): `python -m pytest tests/unit/test_alert_records.py tests/unit/test_alert_consumer.py tests/unit/test_business_records.py tests/unit/test_risk_engine.py -q` — 239 passed.
- Teste isolado do consumer: `python -m pytest tests/unit/test_alert_consumer.py -q` — 16 passed.
- Ruff: `ruff check src/climate_logistics/business_records.py src/climate_logistics/alert_consumer.py tests/unit/test_alert_records.py tests/unit/test_alert_consumer.py` — All checks passed.
- Root executou o foco em Python 3.11: 127 passed e 1 falha para JSON profundamente aninhado, que levantava `RecursionError`. A correção captura essa exceção apenas em `parse_alert` e a converte para `BusinessRecordError`; `parse_assessment` permanece inalterado. Reexecução em Python 3.11 solicitada ao Root.

## Observação

O ambiente local não tem Python 3.11; o launcher `py` também não está instalado. Os testes foram executados em Python 3.12.14. Root deve completar a validação em Python 3.11, conforme o preflight combinado. Docker, rede e commits não foram usados.
