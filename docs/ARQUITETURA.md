# Arquitetura — Lakehouse de Eventos de Pagamento com Contrato de Dados, Step Functions e CI/CD

Pipeline de eventos de pagamento em arquitetura medallion com quarentena: ingestão na bronze, contrato de dados na passagem pra silver, agregação analítica na gold — processamento em PySpark, orquestração serverless por máquina de estados e infraestrutura 100% declarada em Terraform, executável ponta a ponta em ambiente local.

## Princípios

O pipeline é tratado como um produto de software, não como um conjunto de scripts. Isso significa três separações explícitas:

- **Plano de controle × plano de dados** — quem orquestra (Step Functions) não é quem processa (Spark). O orquestrador valida pré-condições, mede resultados, decide se aprova ou reprova e notifica; o motor de processamento só transforma dados.
- **Lógica de negócio × infraestrutura de execução** — as transformações são funções puras que recebem e devolvem DataFrame, testáveis com pytest sem subir cluster nenhum.
- **Definição × criação de recursos** — tudo que existe na nuvem é declarado em Terraform, versionado no Git e aplicado por pipeline; nenhum recurso nasce de comando solto no terminal.

## Modelo de camadas

Bronze/silver/gold com uma quarta área de quarentena:

- **Bronze** — dado bruto, imutável, exatamente como chegou, particionado por data de ingestão.
- **Silver** — dado validado contra um contrato explícito: tipagem correta, deduplicação, regras de negócio aplicadas.
- **Quarentena** — o que reprova no contrato não é descartado nem corrigido no escuro: vai pra quarentena com o motivo da rejeição gravado.
- **Gold** — dado modelado pra consumo analítico, agregado e enriquecido, pronto pra Athena, Redshift Spectrum ou carga no Redshift.

## Visão geral

```mermaid
flowchart LR
    subgraph ingestao["Ingestão"]
        fonte["Gerador de<br/>eventos sintéticos"]
    end

    subgraph bronze_col["Bronze"]
        bronze["S3<br/>evt-lakehouse-bronze"]
    end

    subgraph proc1["Processamento"]
        job1["Job PySpark<br/>bronze -> silver"]
    end

    subgraph silver_col["Silver / Quarentena"]
        silver["S3<br/>evt-lakehouse-silver"]
        quarantine["S3<br/>evt-lakehouse-quarantine"]
    end

    subgraph proc2["Processamento"]
        job2["Job PySpark<br/>silver -> gold"]
    end

    subgraph gold_col["Gold"]
        gold["S3<br/>evt-lakehouse-gold"]
    end

    subgraph orquestracao["Orquestração"]
        sfn["Step Functions<br/>evt-lakehouse-daily-pipeline"]
        ops{{"Lambda<br/>evt-lakehouse-pipeline-ops"}}
    end

    subgraph estado["Estado & Scripts"]
        metrics[("DynamoDB<br/>evt-lakehouse-run-metrics")]
        artifacts["S3<br/>evt-lakehouse-artifacts"]
    end

    subgraph alertas["Observabilidade & Alertas"]
        logs["CloudWatch Logs<br/>evt-lakehouse-daily-pipeline"]
        sns(["SNS<br/>evt-lakehouse-pipeline-alerts"])
        sqs[["SQS<br/>evt-lakehouse-alerts-inbox"]]
    end

    subgraph seguranca["Segurança"]
        role["IAM<br/>evt-lakehouse-pipeline-role"]
    end

    fonte -->|eventos| bronze
    fonte -.->|dimensão| gold
    bronze --> job1
    job1 -->|aprovado| silver
    job1 -->|reprovado| quarantine
    job1 -.->|métricas| artifacts
    silver --> job2
    job2 -->|agregações| gold
    job2 -.->|métricas| artifacts

    sfn -->|orquestra| job1
    sfn -->|orquestra| job2
    sfn -.->|valida/checkpoint| ops
    ops -.->|lê/grava| metrics
    sfn -.->|log| logs
    sfn -.->|alerta| sns
    sns -.->|assinatura| sqs

    role -.->|assume| sfn
    role -.->|assume| ops
```

## Por componente

### Provisionamento (parte 1): fundação do lakehouse — storage, IAM e tabela de métricas

```mermaid
flowchart LR
    subgraph datalake["Data Lake"]
        bronze["S3<br/>evt-lakehouse-bronze"]
        silver["S3<br/>evt-lakehouse-silver"]
        gold["S3<br/>evt-lakehouse-gold"]
        quarantine["S3<br/>evt-lakehouse-quarantine"]
        artifacts["S3<br/>evt-lakehouse-artifacts"]
    end

    subgraph estado["Estado"]
        metrics[("DynamoDB<br/>evt-lakehouse-run-metrics")]
    end

    subgraph seguranca["Segurança"]
        role["IAM<br/>evt-lakehouse-pipeline-role"]
    end

    role -.->|acessa| bronze
    role -.->|acessa| silver
    role -.->|acessa| gold
    role -.->|acessa| quarantine
    role -.->|acessa| artifacts
    role -.->|acessa| metrics
    classDef novo fill:#dbeafe,stroke:#1d4ed8,stroke-width:2px,color:#1e3a5f
    classDef existente fill:#f1f5f9,stroke:#94a3b8,stroke-width:1px,stroke-dasharray:4 3,color:#334155
    class bronze,silver,gold,quarantine,artifacts,metrics,role novo
```

### Provisionamento (parte 2): plano de controle — Lambda, SNS/SQS e a máquina de estados

```mermaid
flowchart LR
    subgraph orquestracao["Orquestração"]
        ops{{"Lambda<br/>evt-lakehouse-pipeline-ops"}}
        sfn["Step Functions<br/>evt-lakehouse-daily-pipeline"]
    end

    subgraph alertas["Observabilidade & Alertas"]
        logs["CloudWatch Logs<br/>/aws/vendedlogs/states/evt-lakehouse-daily-pipeline"]
        sns(["SNS<br/>evt-lakehouse-pipeline-alerts"])
        sqs[["SQS<br/>evt-lakehouse-alerts-inbox"]]
    end

    subgraph estado["Estado"]
        metrics[("DynamoDB<br/>evt-lakehouse-run-metrics")]
    end

    subgraph seguranca["Segurança"]
        role["IAM<br/>evt-lakehouse-pipeline-role"]
    end

    role -.->|assume| ops
    role -.->|assume| sfn
    sfn -->|valida landing| ops
    ops -->|checkpoint por estágio| metrics
    sfn -->|grava execução| logs
    sfn -->|notifica falha/quality gate| sns
    sns -->|assinatura| sqs
    classDef novo fill:#dbeafe,stroke:#1d4ed8,stroke-width:2px,color:#1e3a5f
    classDef existente fill:#f1f5f9,stroke:#94a3b8,stroke-width:1px,stroke-dasharray:4 3,color:#334155
    class ops,sfn,logs,sns,sqs novo
    class metrics,role existente
```

### Geração e ingestão dos eventos brutos na camada bronze

```mermaid
flowchart LR
    fonte["Gerador de<br/>eventos sintéticos"]
    bronze["S3<br/>evt-lakehouse-bronze"]
    gold["S3<br/>evt-lakehouse-gold"]
    fonte -->|grava eventos de pagamento,<br/>particionado por data| bronze
    fonte -->|grava dimensão de estabelecimentos| gold
    classDef novo fill:#dbeafe,stroke:#1d4ed8,stroke-width:2px,color:#1e3a5f
    classDef existente fill:#f1f5f9,stroke:#94a3b8,stroke-width:1px,stroke-dasharray:4 3,color:#334155
    class bronze,gold existente
```

### Job PySpark bronze para silver: contrato de dados, quarentena e testes unitários

```mermaid
flowchart LR
    subgraph datalake["Data Lake"]
        bronze["S3<br/>evt-lakehouse-bronze"]
        silver["S3<br/>evt-lakehouse-silver"]
        quarantine["S3<br/>evt-lakehouse-quarantine"]
        artifacts["S3<br/>evt-lakehouse-artifacts"]
    end

    subgraph processamento["Processamento"]
        job1["Job PySpark bronze->silver"]
    end

    subgraph orquestracao["Orquestração"]
        ops{{"Lambda<br/>evt-lakehouse-pipeline-ops"}}
    end

    bronze -->|lê| job1
    job1 -->|contrato de dados OK, Parquet| silver
    job1 -->|reprovou contrato| quarantine
    job1 -->|publica métricas do estágio| artifacts
    job1 -.->|valida checkpoint| ops
    classDef novo fill:#dbeafe,stroke:#1d4ed8,stroke-width:2px,color:#1e3a5f
    classDef existente fill:#f1f5f9,stroke:#94a3b8,stroke-width:1px,stroke-dasharray:4 3,color:#334155
    class bronze,silver,quarantine,artifacts,ops existente
```

### Job silver para gold: SQL analítico avançado, broadcast join e exposição para Redshift

```mermaid
flowchart LR
    silver["S3<br/>evt-lakehouse-silver"]
    artifacts["S3<br/>evt-lakehouse-artifacts"]
    job2["Job PySpark silver->gold"]
    gold["S3<br/>evt-lakehouse-gold"]
    silver -->|lê eventos validados| job2
    gold -->|lê dimensão de<br/>estabelecimentos, broadcast join| job2
    job2 -->|agregações por estabelecimento/dia| gold
    job2 -->|publica métricas do estágio| artifacts
    classDef novo fill:#dbeafe,stroke:#1d4ed8,stroke-width:2px,color:#1e3a5f
    classDef existente fill:#f1f5f9,stroke:#94a3b8,stroke-width:1px,stroke-dasharray:4 3,color:#334155
    class silver,artifacts,gold existente
```

### Orquestração ponta a ponta: executar, quebrar de propósito e comparar com Airflow

```mermaid
flowchart LR
    subgraph orquestracao["Orquestração"]
        sfn["Step Functions<br/>evt-lakehouse-daily-pipeline"]
        ops{{"Lambda<br/>evt-lakehouse-pipeline-ops"}}
    end

    subgraph alertas["Alertas"]
        sns(["SNS<br/>evt-lakehouse-pipeline-alerts"])
        sqs[["SQS<br/>evt-lakehouse-alerts-inbox"]]
    end

    subgraph estado["Estado"]
        metrics[("DynamoDB<br/>evt-lakehouse-run-metrics")]
    end

    sfn -->|valida landing/checkpoint| ops
    ops -->|lê/grava estado da execução| metrics
    sfn -->|Retry/Catch em falha| sns
    sns -->|assinatura| sqs
    classDef novo fill:#dbeafe,stroke:#1d4ed8,stroke-width:2px,color:#1e3a5f
    classDef existente fill:#f1f5f9,stroke:#94a3b8,stroke-width:1px,stroke-dasharray:4 3,color:#334155
    class sfn,ops,metrics,sns,sqs existente
```

### CI/CD, FinOps e fechamento: transformar o projeto em software entregável

```mermaid
flowchart LR
    ci["Pipeline CI/CD"]
    infraTemp["Infra efêmera (Terraform<br/>apply no runner)"]
    testes["Testes de integração"]
    destroy["Terraform destroy (always)"]
    ci -->|apply| infraTemp
    infraTemp -->|valida pipeline ponta a ponta| testes
    testes -->|sempre executa, mesmo se falhar| destroy
```

---

O porquê de cada decisão estrutural está em [`DECISOES.md`](DECISOES.md); os limites assumidos e o caminho de evolução, em [`LIMITACOES.md`](LIMITACOES.md).

