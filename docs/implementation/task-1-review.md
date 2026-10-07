# Revisão técnica independente — tarefa 1

## Veredito após correção

**Aprovado no escopo revisado; nenhum finding acionável pendente.**

Revisão da correção confirmou `taskmanager.memory.process.size: 1280m` em `compose.yaml:79` e `mem_limit: 1536m` em `compose.yaml:72`. Aplicando os defaults já verificados, sobram 832 MiB para memória Flink; framework heap/off-heap somam 256 MiB, managed memory usa 332,8 MiB e rede usa 83,2 MiB, deixando aproximadamente 160 MiB para task heap. O limite do container ainda oferece 256 MiB além do orçamento do processo.

O principal informou validação runtime com `docker compose up -d --wait --wait-timeout 180` encerrando com exit 0, quatro serviços saudáveis e REST `/taskmanagers` contendo exatamente um registro e um slot livre. Essa evidência é atribuída ao principal; o revisor não repetiu comandos runtime. A revisão da correção ficou limitada ao orçamento de memória.

## Finding resolvido

**[P1] Memória total do TaskManager incompatível com os defaults do Flink 1.20** — `compose.yaml:79`.

`taskmanager.memory.process.size: 768m` deixa apenas 320 MiB de memória Flink após os defaults de metaspace (256 MiB) e overhead mínimo da JVM (192 MiB). Framework heap (128 MiB), framework off-heap (128 MiB), rede mínima (64 MiB) e managed memory (40% de 320 MiB = 128 MiB) já somam 448 MiB, antes da task heap. A inferência a partir desses defaults é que o TaskManager abortará a derivação da memória na inicialização, impedindo registro e saúde do cluster.

Correção: aumentar o orçamento de processo e o limite do container de forma compatível, ou configurar explicitamente os componentes para caber no orçamento local. Confirmar por inicialização real e registro REST do TaskManager; a configuração YAML válida não comprova essa condição.

Fonte dos defaults: [configuração oficial do Flink 1.20](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/deployment/config/#memory-configuration).

Revisão estática dos cinco arquivos autorizados e do brief. Nenhuma leitura de `.env` ou configuração expandida. Runtime não foi executado por este revisor.
