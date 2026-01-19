
---

# gov_credit

`gov_credit` é uma biblioteca de **governança e qualidade de dados** para ambientes Spark, projetada para funcionar **com a mesma base de código** em:

* **Databricks** (Metastore / Unity Catalog)
* **Google Colab / Spark local** (Delta Lake por path)

A biblioteca registra automaticamente **datasets**, **colunas** e aplica **regras de qualidade** carregadas dinamicamente em tempo de execução.

---

## 🎯 Objetivos da biblioteca

* Governança automática sem acoplamento ao pipeline
* Registro incremental de metadados (datasets e colunas)
* Regras externas, versionáveis e dinâmicas
* Compatível com Databricks e Colab
* Baseada em **Delta Lake**
* Arquitetura extensível (Data Mesh / Lakehouse)

---

## 📁 Estrutura do projeto

```
gov_credit/
├── core/
│   └── engine.py
│
├── adapters/
│   ├── base.py
│   ├── databricks.py
│   └── colab.py
│
├── sql/
│   ├── databricks_create_dq_tables.sql
│   └── colab_create_dq_tables.py
│
├── examples/
│   ├── databricks_example.py
│   └── colab_example.py
│
├── setup.py
└── README.md
```

---

## 🧠 Arquitetura (conceito)

A biblioteca é dividida em três camadas:

1. **Core (agnóstico de ambiente)**
2. **Adapters (implementação por ambiente)**
3. **Camada de metadados (tabelas DQ)**

O **core nunca sabe onde está rodando**.
Quem decide como ler e escrever metadados é o **adapter**.

---

## 📂 `core/engine.py`

### Responsabilidade

Orquestrar o fluxo de governança e qualidade de dados.

### O que ele faz

1. Garante que o dataset esteja registrado
2. Garante que colunas novas sejam registradas
3. Carrega regras ativas
4. Aplica regras (ou ignora se não existirem)

### Funções principais

#### `DataQualityEngine.__init__(adapter)`

* Recebe um adapter (Databricks ou Colab)
* Injeta o Spark correto
* Desacopla a lógica do ambiente

#### `DataQualityEngine.run(df, dataset_name)`

* Ponto único de entrada da lib
* Deve ser chamado **antes do write**
* Nunca quebra pipeline por falta de regra

---

## 📂 `adapters/base.py`

### Responsabilidade

Definir o **contrato mínimo** que qualquer ambiente precisa implementar.

### Por que existe

* Evita `if colab / if databricks`
* Permite novos ambientes no futuro (EMR, Glue, Synapse)

### Métodos obrigatórios

* `ensure_dataset`
* `ensure_columns`
* `load_rules`

---

## 📂 `adapters/databricks.py`

### Responsabilidade

Implementar governança usando:

* Metastore
* Tabelas gerenciadas
* SQL nativo do Databricks

### Comportamento

* Usa `INSERT ... WHERE NOT EXISTS`
* Nunca duplica registros
* Trabalha com `dq.datasets`, `dq.columns`, `dq.rules`

### Quando usar

* Produção
* Ambientes corporativos
* Unity Catalog

---

## 📂 `adapters/colab.py`

### Responsabilidade

Implementar governança usando:

* Delta Lake por **path**
* Spark local
* Sem metastore

### Comportamento

* Cria tabelas Delta se não existirem
* Registra apenas colunas novas
* Funciona com S3, GCS, ADLS ou filesystem local

### Quando usar

* Provas de conceito
* Estudos
* Laboratório
* Desenvolvimento local

---

## 📂 `sql/databricks_create_dq_tables.sql`

### Por que este script é necessário

A biblioteca **não cria tabelas automaticamente**.

Isso é proposital.

Criar estruturas de governança é:

* decisão de plataforma
* decisão de segurança
* decisão organizacional

### O que ele cria

* `dq.datasets` → catálogo de datasets
* `dq.columns` → catálogo de colunas
* `dq.rules` → regras de qualidade

### Quando executar

* Uma única vez por workspace
* Ou via pipeline de bootstrap

---

## 📂 `sql/colab_create_dq_tables.py`

### Por que existe

No Colab:

* Não existe metastore
* Não existe SQL DDL persistente

Este script cria **os mesmos contratos**, porém:

* via DataFrame vazio
* usando Delta Lake por path

### Resultado

```
/content/delta/dq/
├── datasets/
├── columns/
└── rules/
```

Esses paths são consumidos diretamente pelo `ColabAdapter`.

---

## 📂 `examples/`

### Objetivo

Mostrar **uso real**, não toy examples.

#### `databricks_example.py`

* Usa Spark do Databricks
* Usa `DatabricksAdapter`

#### `colab_example.py`

* Cria Spark local
* Usa `ColabAdapter`
* Lê e escreve Delta por path

---

## 📦 `setup.py`

### Responsabilidade

Permitir:

* instalação via Git
* uso em Databricks (`%pip install`)
* uso em Colab (`pip install`)

---

## 🔁 Fluxo de execução resumido

1. Executa script SQL de bootstrap
2. Pipeline cria DataFrame
3. Chama `engine.run(df, dataset)`
4. Governança acontece automaticamente
5. Pipeline segue normalmente

---

## 🧠 Decisões arquiteturais importantes

* ❌ A lib não cria tabelas

* ❌ A lib não impõe regras

* ❌ A lib não quebra pipelines sem regra

* ✅ Contrato fixo

* ✅ Regras externas

* ✅ Evolução segura

---
