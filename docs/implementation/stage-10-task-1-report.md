# Etapa 10 — Task 1

Criado `.github/workflows/ci.yml` com jobs independentes `quality`, `python-kafka` e `flink-sdk`, nos eventos push, pull request e acionamento manual. O fluxo usa Python 3.11.14, suíte existente e Ruff; constrói as imagens atuais; cria `weather.alerts` antes da integração; e verifica Python 3.11.14, `confluent-kafka` 2.12.2, UID 10001, PyFlink 1.20.3 e que os quatro módulos SDK terminem sem skips.

O job Kafka usa `COMPOSE_PROJECT_NAME` derivado do run e attempt, valores fictícios para interpolar a configuração, e um override temporário em `RUNNER_TEMP` com tag de imagem própria. Inicia apenas Kafka e, em `if: always()`, remove volumes e containers desse projeto. O job Flink instala dependências dev no container temporário com acesso à rede; o checkout fica montado como somente leitura, e os módulos não iniciam cluster/job nem acessam banco ou broker. O guard também falha em skips de testes e de coleta.

Actions verificadas em fontes oficiais e fixadas por SHA completo:

- `actions/checkout` v7.0.0: [`9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0`](https://github.com/actions/checkout/commit/9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0) ([release](https://github.com/actions/checkout/releases/tag/v7.0.0)).
- `actions/setup-python` v7.0.0: [`5fda3b95a4ea91299a34e894583c3862153e4b97`](https://github.com/actions/setup-python/commit/5fda3b95a4ea91299a34e894583c3862153e4b97) ([release](https://github.com/actions/setup-python/releases/tag/v7.0.0)).

Não executei Docker, instalações nem testes neste checkout. A QA local e a execução/revisão do workflow ficam com o principal, conforme o brief; não há execução GitHub a reportar.
