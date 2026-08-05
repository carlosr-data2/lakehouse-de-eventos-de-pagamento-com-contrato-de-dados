# Arquitetura — Lakehouse de Eventos de Pagamento com Contrato de Dados, Step Functions e CI/CD

Este projeto nasceu dos requisitos de uma vaga real de Engenheiro de Dados Sênior. Peguei a descrição, parafraseei o que era pedido e transformei em um sistema que roda de ponta a ponta na minha máquina: arquitetura em camadas (medallion), processamento distribuído em PySpark, orquestração serverless com máquina de estados, contrato de dados com quarentena, testes automatizados e pipeline de CI. Nada de tutorial genérico — o escopo veio do que o mercado está pedindo.

A ideia central é tratar o pipeline como um produto de software, não como um conjunto de scripts. Isso significa três separações explícitas. Primeiro, separação entre plano de controle e plano de dados: quem orquestra (Step Functions) não é quem processa (Spark). O orquestrador valida pré-condições, mede resultados, decide se aprova ou reprova e notifica; o motor de processamento só transforma dados. Segundo, separação entre lógica de negócio e infraestrutura de execução: as transformações são funções puras que recebem e devolvem DataFrame, então podem ser testadas com pytest sem subir cluster nenhum. Terceiro, separação entre definição e criação de recursos: tudo que existe na nuvem é declarado em Terraform, versionado no Git, aplicado por pipeline — nenhum recurso nasce de comando solto no terminal.

O modelo de camadas usado aqui é bronze/silver/gold com uma quarta área de quarentena. Bronze é o dado bruto, imutável, exatamente como chegou, particionado por data de ingestão. Silver é o dado validado contra um contrato explícito: tipagem correta, deduplicação, regras de negócio aplicadas. O que reprova não é descartado nem corrigido no escuro — vai para a quarentena com o motivo da rejeição gravado, porque dado descartado silenciosamente é o jeito mais rápido de perder confiança no lakehouse. Gold é o dado modelado para consumo analítico, agregado e enriquecido, pronto para ser lido por Athena, Redshift Spectrum ou carregado para dentro do Redshift.

Uma decisão de arquitetura importante e consciente: os jobs são PySpark puro, sem GlueContext e sem DynamicFrame. O custo disso é abrir mão de alguns recursos específicos do Glue (bookmarks nativos, resolveChoice). O ganho é portabilidade total — o mesmo arquivo roda em Glue, em EMR, em EMR Serverless, em Databricks e no container local, sem reescrita. Em ambiente com múltiplos motores de execução e pressão de FinOps, poder mover uma carga de Glue para EMR Serverless (ou vice-versa) só mudando o submit é uma alavanca de custo real, não teórica.

Para orquestração, a escolha é Step Functions em vez de Airflow — e vale entender o trade-off, porque não é consenso. Step Functions é serverless, tem retry/backoff/catch declarativos, integração nativa com serviços AWS, custo por transição de estado e zero infraestrutura para manter. Airflow tem ecossistema de operadores muito maior, backfill nativo, UI superior para diagnóstico e expressividade Python real. A regra prática que uso: pipeline majoritariamente AWS-nativo que precisa de resiliência com baixo custo operacional pede Step Functions; pipeline com muitas integrações heterogêneas, dependências complexas entre DAGs e necessidade constante de backfill pede Airflow. O projeto entrega a implementação em Step Functions e, ao final, a DAG equivalente em Airflow, para deixar a comparação concreta em vez de retórica.

Tudo roda em LocalStack. Isso não é atalho — é uma prática de DataOps: ambiente descartável, idêntico ao de produção na forma dos recursos, com custo zero e ciclo de feedback de segundos. O mesmo Terraform que aponta para o LocalStack aponta para a AWS real trocando o bloco de endpoints, e o mesmo pipeline de CI que valida aqui valida lá. Onde a emulação tem limite, o limite é codificado em variável e documentado, nunca improvisado.

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

