# Relatório — Etapa 2 / Tarefa 1

Status: concluída.

## Red/green

- RED inicial no Docker python:3.11.14-slim-bookworm: teste falhou porque climate_logistics.models ainda não existia.
- Modelos: 105 testes passaram após implementar models.py.
- Localidades: 20 casos passaram e o primeiro uso de LOCATIONS falhou porque locations.py ainda não existia; após a implementação, 22 passaram.
- Normalização: falhou porque open_meteo.py ainda não existia.
- GREEN final: python -m pytest -q — 240 passaram.

## Checks

- python -m ruff check . — todos os checks passaram.
- Execução em Python 3.11.14; dependências de desenvolvimento instaladas de requirements-dev.txt.

## Escopo e observações

Implementados modelos congelados com validação também na construção direta, configuração de localidades e normalização pura do bloco current. A fixture é manual, não contém dados privados e cobre unidades, timestamp Unix e identidade configurada.

Nenhuma pendência neste escopo. A tarefa não inclui cliente HTTP nem acesso à rede.

## Correção da revisão round 1 — limites UTC

- RED: quatro casos reproduziram a aceitação de datetime aware cuja conversão para UTC sai do domínio representável; o caso datetime.min/datetime.max já em UTC passou.
- Correção: a construção agora valida a conversão para UTC e transforma OverflowError em WeatherValidationError.
- GREEN: python -m pytest -q — 245 passaram.
- python -m ruff check . — todos os checks passaram.
