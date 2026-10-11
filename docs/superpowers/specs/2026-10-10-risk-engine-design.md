# Etapa 7 — Risk Engine

Estado: desenho aprovado pelo usuário em 2026-10-10; plano preparado para revisão, implementação não iniciada.
Data: 2026-10-10. Base: 7ae024e, branch foundation.

## Objetivo e contexto

Transformar as métricas temporais da etapa 6 em uma avaliação determinística de risco climático por localidade e janela. Produzir risk_score inteiro de0 a100, risk_level LOW/MEDIUM/HIGH/CRITICAL e explicação dos fatores. Preservar janelas30/5, watermarks2 minutos, dedup, checkpoints e os contratos/UIDs existentes.

Implementadores GPT-6 Luna com esforço Max; principal coordena revisão e QA real. O resultado desta etapa prepara os sinks da etapa8. Sem novos serviços, tópicos, bibliotecas, ML ou mudança de recursos.

## Alternativas e escolha proposta

1. Máximo da severidade dos quatro fatores: recomendado para este MVP. Uma condição extrema domina a avaliação; cálculo pequeno, transparente e monotônico. Dois fatores moderados não recebem amplificação adicional nessa versão.
2. Soma ponderada com pisos para condições extremas: oferece mais gradações, mas exige pesos e regras de combinação; média simples poderia diluir um fator extremo.
3. Regras compostas específicas para cenários: expressivas, mas aumentam o número de combinações e exigem mais conhecimento operacional.

Escolha proposta: máximo, com tabela versionada em código. Os valores abaixo são hipóteses explícitas da política MVP, para aprovação; não representam calibração empírica de perdas logísticas nem uma probabilidade percentual.

## Entradas e unidades

Consumir os quatro campos do agregado já produzido:

- precipitation_rate_max_mm_per_hour: projeção de intensidade por observação emmm/h, calculada na etapa6 a partir de precipitação e intervalo; não tratar como chuva acumulada de uma hora ou da janela;
- wind_speed_max e wind_gust_max: km/h;
- visibility_min: metros.

Usar os extremos da janela já definidos. Temperatura, weather_code, volume de observações e duração não entram no score desta versão. Os metadados/intervalos continuam no registro para interpretação.

## Política mvp-1 proposta

Para precipitação e vento, selecionar o maior limiar atingido (>=). Para visibilidade, aplicar limites estritos (<); valor exatamente no limiar permanece na faixa menos severa. Valores fora da primeira faixa recebem score0.

| Fator | Score25 | Score50 | Score75 | Score100 |
| --- | --- | --- | --- | --- |
| Intensidade de chuva, mm/h | >=2 | >=20 | >=30 | >=60 |
| Vento máximo, km/h | >=20 | >=40 | >=60 | >=100 |
| Rajada máxima, km/h | >=30 | >=50 | >=80 | >=120 |
| Visibilidade mínima, m | <5000 | <2000 | <1000 | <200 |

risk_score = max(rain_score, wind_score, gust_score, visibility_score).

Classificação: LOW0–24; MEDIUM25–49; HIGH50–74; CRITICAL75–100. O cálculo atual produz0/25/50/75/100; a função de classificação cobre todo o domínio inteiro0–100 para manter o contrato claro.

Exemplos: chuva0/vento5/rajada10/visibilidade10000 →0 LOW; chuva2 com outros fatores baixos →25 MEDIUM; vento40 →50 HIGH; rajada80 →75 CRITICAL; visibilidade199 →100 CRITICAL. Dois fatores com score50 mantêm score50, conforme política de máximo. Visibilidade200 fica em75; visibilidade1000 fica em50.

As bandas20/30/60mm/h e40/60km/h têm contexto em avisos e boletins públicos do INMET, mas os scores, faixas, diferenças entre vento/rajada e limites de visibilidade desta tabela são decisões próprias do MVP. As classes internas não são uma reprodução das categorias oficiais. Fonte de contexto: https://portal.inmet.gov.br/uploads/boletimtempo/SITUA%C3%87%C3%83O-METEOROL%C3%93GICA_INMET_20211027.pdf e https://portal.inmet.gov.br/?lang=pt_BR (consultados em2026-10-10).

## Contratos e organização

Módulo puro risk_engine.py, sem imports de Kafka/Flink:

- RiskInputs: dataclass frozen com os quatro valores numéricos, unidades acima. Recusar bool, ausência/None, NaN, infinito e negativos com ValueError específico de domínio; não substituir dado ausente por zero nem duplicar a matriz de ranges raw da etapa5.
- RiskAssessment: resultado imutável com score, nível e quatro scores componentes; versão fixa mvp-1. A serialização cria novo dict, sem expor estrutura mutável interna.
- Funções puras de cálculo e classificação. Tabelas de limiares em constantes imutáveis, sem override por ambiente/CLI. Alterar política exige versão e testes.
- Helper puro para extrair RiskInputs do contrato de métricas e acrescentar a avaliação a uma cópia do registro; nunca modificar o input.

Novo JSON risk_assessment preserva os 17 campos da métrica, trocando somente operation para risk_assessment, e acrescenta:

- risk_score: int0–100;
- risk_level: string uppercase;
- risk_components: objeto com precipitation_rate, wind_speed, wind_gust e visibility, cada um contendo seu score inteiro;
- risk_rules_version: mvp-1.

Total21 campos; schema_version1 identifica a primeira versão desse novo tipo de mensagem, distinguido por operation. A saída window_aggregate v1 de17 campos continua intacta no ramo atual.

Chave lógica de avaliação: location_id + window_start_ms + window_end_ms + risk_rules_version. Mesmo input/política produz o mesmo resultado, inclusive em reprocessamento. Não incluir hora atual, números aleatórios, consulta HTTP ou dependência da ordem dos fatores.

## Integração Flink

Após o stream de agregados JSON, adicionar MapFunction adaptadora em flink_risk.py, que chama somente o módulo puro e produz JSON STRING com allow_nan=False. Novo ramo PrintSink weather-risk. UIDs novas weather-risk-assessment e weather-risk; manter todas as UIDs anteriores e ramo de métricas/ingestão.

Métrica risk_engine.assessed_windows incrementada após cálculo válido. É contador de processamento, pode reiniciar/recontar após recuperação; não é medida transacional de avaliações únicas.

JSON malformado, envelope/source schema inesperado ou fator inválido nesse ponto representam violação do contrato interno: falhar visivelmente, sem emitir LOW fictício ou descarte silencioso. Dados externos continuam passando pelo parsing/qualidade antes de chegar a esse ramo. Preservar restart limitado e checkpointing.

Não criar estado adicional para um cálculo puro. Checkpoints recuperam as janelas e offsets existentes; o novo ramo stateless pode ser adicionado preservando UIDs. A compatibilidade será comprovada com restauração explícita de checkpoint real da etapa6, sem allowNonRestoredState.

## Aceite

1. Unitários para cada limiar exato e vizinhos, visibilidade com comparações estritas, zero, extremo, empates e várias condições simultâneas; score0–100/nível correto e sem diluição de condição extrema.
2. Monotonicidade de cada fator, determinismo, imutabilidade e ausência de relógio/RNG/I/O no módulo puro. Recusar bool/None/missing/NaN/inf/negativos; classificar limites24/25/49/50/74/75/100 e rejeitar scores fora do domínio/tipo.
3. Contrato completo21 campos, componentes e versão corretos; entrada17 campos e seus valores preservados. Adaptador SDK real, JSON válido/erro interno visível e contador correto.
4. Suítes anteriores/Ruff e SDK aprovados emPython3.11/PyFlink1.20.3; skips não substituem testes reais.
5. Integração Kafka→Flink com localidades/janelas exclusivas que produzam as quatro classes, duplicata raw revisada sem inflação e condição extrema isolada. Comparar JSON completo, componentes e chave por janela; preservar ramo window_aggregate.
6. Restaurar checkpoint válido da etapa6 com o grafo ampliado, confirmar checkpoint restaurado e gerar avaliação determinística após replay/fechamento. Apenas JobIDs próprios; sem apagar dados/checkpoints.
7. Operacional ativo com métricas+risco e checkpoints60s, Producer funcional, recursos/backpressure observados sob carga pequena, revisão independente e scan de credenciais.
8. Registrar problemas/correções/limites, atualizar docs e commit local; só então ETAPA7 CONCLUÍDA. Etapa8 permanece pendente.

## Limites deliberados

Score heurístico ordinal do MVP, sem calibração por tipo de veículo, rota, vulnerabilidade ou prejuízo financeiro. Máximos/mínimos da janela podem vir de observações diferentes; score descreve a condição mais severa observada, sem afirmar simultaneidade. Não há amplificação de múltiplos fatores moderados. Risk rules versionada permite calibrar/alterar depois mantendo rastreabilidade.

Nesta etapa a avaliação é emitida no diagnóstico do Flink. Persistência e publicação em weather.alerts pertencem à etapa8. Checkpoints não tornam PrintSink transacional; logs/avaliações podem repetir após recovery, mantendo identidade e resultado determinístico.
