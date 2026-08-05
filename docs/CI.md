# CI — como funciona, por que é assim e como reproduzir localmente

O workflow ([`.github/workflows/ci.yml`](../.github/workflows/ci.yml)) roda a cada push na `main` e em todo pull request. Ele **não substitui rodar o projeto na sua máquina** — ele prova, numa máquina descartável do GitHub, que o projeto sobe do zero sem intervenção humana. O badge verde no README significa exatamente isso.

## Filosofia: do mais barato pro mais caro

Os três estágios são encadeados (`needs:`) em ordem crescente de custo. Erro trivial quebra em segundos no primeiro estágio, sem gastar os minutos do estágio de integração:

| Estágio | O que prova | Duração típica |
|---|---|---|
| `static` | O código está bem-formado (fmt/validate do Terraform, lint do Python) | ~20s |
| `unit` | A lógica de negócio está certa (pytest do contrato de dados, Spark local, zero infra) | ~1min |
| `integration` | A infraestrutura sobe do zero (LocalStack + `terraform apply` + smoke test) | ~2min |

## Decisões do workflow (e por quê)

**Versões pinadas em tudo que o CI instala.** Terraform `1.9.5`, ruff `0.16.1`, PySpark `3.5.1`, LocalStack `3.8`. CI que instala "a versão mais nova" de uma ferramenta quebra sozinho quando a ferramenta muda de comportamento — aconteceu neste repositório (ver histórico abaixo): o ruff sem pin passou a aplicar regras novas de ordenação de import e derrubou um build sem nenhuma mudança de código. Pin não é conservadorismo, é reprodutibilidade: o mesmo commit dá o mesmo resultado hoje e daqui a seis meses. Atualizar versão vira uma decisão explícita, num commit próprio.

**`terraform fmt -check -diff`.** O `-diff` imprime no log exatamente o que está fora do padrão. Sem ele, o CI só diz "arquivo X está mal formatado" e obriga quem lê a adivinhar — erro de CI tem que ser acionável direto do log.

**LocalStack como *service container* com o socket do Docker montado.**

```yaml
services:
  localstack:
    image: localstack/localstack:3.8
    ports: ["4566:4566"]
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
```

A Lambda do LocalStack executa cada função num container próprio, criado via API do Docker. Sem o socket do runner montado, `CreateFunction` termina em estado `Failed` (`Docker not available`) e o `terraform apply` quebra. Montar o socket é o padrão recomendado pelo próprio LocalStack para CI.

**`terraform destroy` com `if: always()`.** O destroy roda mesmo quando o apply ou o smoke test falham. No emulador o custo de vazar recurso é zero, mas o hábito é o que se leva pra AWS real — onde recurso órfão de pipeline quebrado é exatamente a classe de incidente que aparece na fatura.

**Smoke test separado dos testes unitários.** [`ci/smoke_test.py`](../ci/smoke_test.py) não testa lógica: verifica que a infraestrutura *provisionada* é a esperada (buckets existem, máquina de estados existe, Lambda responde — inclusive no caminho de erro, que também é contrato). Teste unitário valida código; smoke test valida o resultado do provisionamento.

## Reproduzindo cada estágio localmente

```bash
# estágio static
terraform -chdir=infra fmt -check -diff -recursive
terraform -chdir=infra init -backend=false && terraform -chdir=infra validate
pip install ruff==0.16.1 && ruff check jobs ingest lambda ci tests

# estágio unit (precisa de Java 17+ e Python 3.12)
pip install pyspark==3.5.1 pytest
python -m pytest tests -q

# estágio integration (precisa do Docker)
docker compose up -d
terraform -chdir=infra init && terraform -chdir=infra apply -auto-approve
pip install boto3 && python ci/smoke_test.py --endpoint http://localhost:4566
terraform -chdir=infra destroy -auto-approve
```

## Problemas reais que o CI pegou (e as correções)

Este repositório nasceu de um roteiro em que o código existia só como texto num README. Ao materializar os arquivos e ligar o CI, **quatro problemas reais que estavam invisíveis apareceram na primeira semana** — cada um com seu commit de correção no histórico:

1. **Lint: variável `l` ambígua** (`ci/smoke_test.py`, regra E741). Em fonte com serifa, `l`, `1` e `I` se confundem — renomeada para `camada`.
2. **Drift de versão do linter.** O workflow instalava `ruff` sem pin; uma versão nova passou a exigir ordenação de imports diferente e o build quebrou sem mudança de código. Correção dupla: aplicar as regras novas **e** pinar a versão.
3. **`F.try_cast` não existe no PySpark 3.5** (`jobs/contract.py`). A função só entrou no módulo Python no PySpark 4; no 3.5, nem como método de Column (o acesso ao atributo devolve uma Column de campo — e chamá-la dá `TypeError: 'Column' object is not callable`). Correção: a **função SQL** `try_cast`, que existe no Spark desde o 3.2, via `F.expr("try_cast(amount_cents AS long)")` — mesma semântica: cast inválido vira `NULL` em vez de derrubar o job. Os 4 testes do contrato quebravam antes de qualquer asserção; nenhum teria passado despercebido com o CI ligado desde o começo.
4. **`Docker not available` na criação da Lambda.** O service container do LocalStack não tinha o socket do Docker do runner — ver a decisão documentada acima.
5. **`ValidateStateMachineDefinition` não implementada no LocalStack community.** A partir do provider AWS 5.67, o Terraform valida a definição da máquina de estados chamando essa API antes de criar — e o emulador responde 501, quebrando o apply mesmo com a definição correta. O `~> 5.60` original permitia o provider subir até aí; a correção é a trava explícita `>= 5.60.0, < 5.67.0` em `required_providers`, com o motivo comentado no código.

A lição que vale entrevista: **código que nenhuma ferramenta executa é código não verificado**, por mais revisado que pareça. Os quatro erros existiam desde a primeira versão do roteiro e só apareceram quando o repositório virou software executável com verificação automática.

## Custo

Zero em repositório público (Actions ilimitado). Em repositório privado, consome do free tier de 2.000 min/mês — este pipeline gasta ~4 min por push.
