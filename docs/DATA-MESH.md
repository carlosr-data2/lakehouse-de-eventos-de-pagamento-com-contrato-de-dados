# Este lakehouse como um data product — a leitura Data Mesh

Data Mesh é uma resposta **organizacional** a um problema de escala: quando
um time central de dados vira gargalo de todos os domínios da empresa, a
proposta é inverter — cada domínio passa a ser dono dos dados que produz,
publicando-os como produto. Este repositório implementa um único domínio
(pagamentos), mas foi construído de um jeito que já satisfaz o que o Mesh
exige de cada produto. Este documento mapeia essa correspondência — e diz
com a mesma franqueza o que faltaria para um Mesh de verdade.

## Os 4 pilares, mapeados no que existe aqui

**1. Domínio dono dos dados.** O pipeline inteiro pertence a um domínio de
negócio: eventos de pagamento. Quem conhece a semântica de `status`,
`amount` e estorno é quem mantém o contrato — não um time central que
recebe o dado sem contexto.

**2. Dado como produto.** Um produto tem interface, garantia de qualidade e
consumidor definido:

| Atributo de produto | Onde vive neste repo |
|---|---|
| Interface de entrada (o que o produtor pode enviar) | `jobs/contract.py` — schema explícito + regras nomeadas |
| Garantia de qualidade | quarentena com motivo + quality gate no plano de controle |
| Interface de saída (o que o consumidor pode ler) | gold `merchant_daily` — schema estável, documentado em [`GOVERNANCA.md`](GOVERNANCA.md) |
| SLO observável | métricas por execução (DynamoDB + CloudWatch) e alarme de taxa de rejeição |
| Descoberta | Glue Data Catalog (atrás de flag no emulador; nativo na AWS real) |

O **contrato de dados é a fronteira do data product** — essa frase resume o
documento inteiro. O que o Mesh chama de "product API", este pipeline já
implementa como contrato + quarentena + gold estável.

**3. Plataforma self-service.** O que permitiria outro domínio criar o
próprio produto sem pedir permissão a um time central: aqui, o template é o
próprio repositório — Terraform modular, jobs portáveis, CI que valida tudo
do zero. Numa empresa, isso viraria um template de plataforma (scaffolding),
e é exatamente o papel que o repo cumpre para os outros projetos derivados
dele.

**4. Governança federada.** Regras globais (formato de catálogo, padrão de
qualidade, classificação de dado sensível) decididas centralmente e
aplicadas localmente por cada domínio. É o pilar que um repositório sozinho
**não pode** demonstrar — governança federada só existe com mais de um
domínio. O que existe aqui é a metade local: qualidade mensurável, catálogo
plugável e ownership claro — o domínio "pagamentos" já chegaria pronto numa
federação.

## Quando Data Mesh NÃO compensa (a crítica honesta)

Mesh resolve um problema de **gente e escala organizacional**, não de
tecnologia. Com um ou dois domínios produtores e um time de dados que dá
conta, adotar Mesh adiciona custo de coordenação sem devolver nada — um
lakehouse central bem governado (exatamente este desenho) é mais simples,
mais barato e mais rápido de operar. O sinal de virada é o gargalo: quando
o backlog do time central é dominado por pedidos de domínios que entendem
do próprio dado melhor que o time central, o Mesh começa a se pagar.

## Integração com IA (ML/GenAI) — o produto de dados como insumo

A razão prática de uma casa de investimentos exigir dados com contrato,
qualidade e catálogo é que **IA amplifica a qualidade do insumo — nos dois
sentidos**. Três integrações concretas que a gold deste pipeline suportaria
sem mudança estrutural:

1. **Features para ML clássico.** `merchant_daily` já é uma feature table:
   taxa de aprovação, ticket médio, share de PIX e variação D-1 por
   estabelecimento são entradas naturais de um modelo de risco/score de
   merchant. O caminho: gold → feature store (SageMaker Feature Store ou a
   própria gold via Athena), com o contrato garantindo que a feature não
   muda de semântica silenciosamente — deriva de schema em feature de
   modelo é bug de produção com cara de "o modelo piorou sozinho".

2. **RAG corporativo com dado governado.** Um assistente interno que
   responde "por que o GMV do merchant X caiu ontem?" precisa recuperar
   contexto de duas naturezas: numérico (a gold, via SQL) e documental
   (runbooks como `OPERACAO.md`, decisões como `DECISOES.md`). A lição dos
   meus projetos pessoais de RAG/MCP se aplica direto: a qualidade da
   resposta é limitada pela qualidade e pela **procedência** do contexto
   recuperado — RAG sobre dado sem contrato é alucinação com citação.

3. **Agentes com acesso a dados via MCP.** O padrão emergente para dar a um
   LLM acesso a dados corporativos é expor ferramentas tipadas (Model
   Context Protocol) em vez de acesso cru ao banco: o agente consulta a
   gold por uma interface com permissão, auditoria e escopo — as mesmas
   propriedades que a governança já exige para consumidores humanos. Dado
   como produto e ferramenta de agente são o mesmo desenho com
   consumidores diferentes.

O fio condutor: **plataforma de IA não substitui engenharia de dados — ela
a pressupõe.** Cada caso acima consome exatamente o que este pipeline
produz: dado validado, com dono, com contrato e com métrica de qualidade.
