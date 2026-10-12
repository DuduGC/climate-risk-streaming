# Etapa 9 — revisão independente da Task 2

Data: 2026-10-12. Base: `0750ed9`; fonte revisada: working tree recebido para revisão, incluindo os arquivos novos de saúde e integração. Revisão estática independente, sem executar testes, Docker ou rede e sem alterar produto. O gate de execução pertence ao Root. Material de contrato/cache da Task 1 já aprovado.

## Parecer

**Aprovada por inspeção após rechecagem focada: nenhum P1/P2 aberto.** O fluxo central de consumo/offsets e o healthcheck estão coerentes por inspeção. O único P2 identificado inicialmente foi corrigido; as evidências de execução permanecem sob o gate do Root.

### P2 fechado — emitir os eventos de ciclo de vida previstos na spec

Local: `src/climate_logistics/alert_consumer.py:442` (callback de atribuição), `:449` (início do runtime), `:486` (falha) e `:489` (encerramento). Evidência: `on_assign` apenas inicializa posições; `run_consumer` somente chama `emit` indiretamente no handler de mensagens, e seu `finally` apenas invalida a saúde e fecha o cliente. `_report_runtime_failure`, em `:408`, escreve somente `alert consumer failed` em stderr.

A spec aprovada, `docs/superpowers/specs/2026-10-12-alert-consumer-design.md:25`, exige logs de início, atribuição, erro e encerramento, além dos registros por mensagem. Na execução normal sem alertas, stdout fica vazio por toda a vida do processo. Não é possível observar pelos logs se o processo iniciou, obteve assignment ou encerrou normalmente; uma falha também perde o envelope JSON de componente/operação/status/timestamp. O arquivo de saúde não substitui esse histórico operacional.

Correção mínima: emitir eventos JSON seguros de início, atribuição e encerramento com o emissor existente, e um evento genérico de erro quando o emissor estiver funcional; manter fallback genérico em stderr para falha de stdout. Não registrar configurações, payload/key brutos ou detalhes de exceções. Preservar a ordem log da mensagem → commit e o encerramento sem commit global. Verificar esses eventos com testes focados, inclusive execução ociosa e falha de escrita.

Rechecagem focada em 2026-10-12: **fechado**. A fonte corrigida acrescenta `_emit_lifecycle` em `src/climate_logistics/alert_consumer.py:416`, com timestamp/componente/operação/status, e `log_error` em `:445`, que emite um erro genérico uma vez e mantém fallback seguro em stderr se o emissor falhar. `consumer_started` aparece em `:464`; `consumer_assigned`, em `:473`, somente depois de a inicialização de offsets ter sucesso. O caminho de exceção em `:514` chama `log_error`. O `finally` invalida a saúde (`:518`) e fecha o cliente (`:529`) antes de `consumer_stopped` (`:533`), cujo status reflete o resultado. Falha do log final retorna erro sem impedir o fechamento já concluído. Nenhum detalhe de configuração, exceção ou payload foi introduzido nesses eventos; o tratamento/commit por mensagem permanece intacto.

Os testes focados foram inspecionados e agora verificam lifecycle no runtime ocioso, erro operacional, falha do callback de assignment, falha de escrita do alerta sem commit e falha do log stopped preservando ready=false/close. Não executei testes ou Docker nessa rechecagem e não reabri áreas intocadas.

## Evidência favorável por inspeção

- `ConsumerSettings.from_env` limita as duas configurações públicas. A configuração do cliente usa `enable.auto.commit=False`, `enable.auto.offset.store=False`, `latest` e os timeouts previstos; `subscribe` recebe lista de tópicos.
- `initialize_assignment` consulta offsets confirmados, mantém os válidos e usa watermarks capturados somente para partições sem commit. `assign` e commit síncrono dos baselines ocorrem dentro do callback antes que `poll` retorne ao tratamento. Resultado do commit é verificado por chave, offset e atributo nativo `error`; falha do callback impede tratar a mensagem retornada.
- O loop conserva uma instância e um cache; o handler confirma após emitir/flush e atualiza o cache somente após confirmação bem-sucedida. EOF não é tratado como alerta; outros erros interrompem o runtime. Não há commit global no loop ou no fechamento.
- Metadata é validada inicialmente e a cada 15 s com timeout de 5 s. Assignment não vazio e heartbeat recente permitem saúde ociosa. O healthcheck local valida PID e timestamps monotônicos finitos/coerentes, sem criar cliente Kafka; o arquivo usa replace atômico. O `finally` invalida ready antes de `close`.
- Compose reutiliza imagem/Dockerfile, recursos/restart/grace previstos, configuração pública apenas e dependência de Kafka saudável. README explica latest inicial, inválidos confirmados, cache em memória e a garantia at-least-once.
- Integração opt-in aguarda saúde antes de publicar, identifica fixtures por ACK de tópico/partição/offset e hash independente, verifica recebido/duplicata/inválido, ausência do marcador bruto, commits e retomada do mesmo grupo. Containers QA têm nomes exclusivos registrados antes da espera e cleanup em `finally`; observer consulta commits sem subscribe e não apaga grupos/topics/dados. Não há comandos para parar/recriar serviços de pipeline.

## Limites do parecer

Não se declara sucesso de suíte, build, execução do cliente nativo 2.12.2, integração Kafka ou limites reais de shutdown nesta revisão. Esses pontos dependem das evidências de QA do Root. Não foram propostos novos componentes ou garantias além do MVP aprovado.
