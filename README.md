# 🏢 Brazilian Companies Data Lakehouse (CNPJ da Receita Federal)

[![Databricks](https://img.shields.io/badge/Platform-Databricks-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00ADD8?logo=apachespark&logoColor=white)](https://delta.io/)
[![PySpark](https://img.shields.io/badge/Engine-PySpark%203.5+-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Unity Catalog](https://img.shields.io/badge/Governance-Unity%20Catalog-0284C7)](https://www.databricks.com/product/unity-catalog)
[![Architecture](https://img.shields.io/badge/Architecture-Medallion%20(Bronze%20%7C%20Silver%20%7C%20Gold)-brightgreen)](#-arquitetura-medalhão)

Projeto de Engenharia de Dados em larga escala construído no **Databricks**, implementando uma **Arquitetura Medalhão (Bronze, Silver, Gold)** e **Modelagem Dimensional (Star Schema)** sobre a base de dados abertos de **CNPJ da Receita Federal do Brasil (RFB)** — abrangendo mais de **55 milhões de registros** de empresas, estabelecimentos e quadros societários.

---

## 📌 Sumário Executivo

A base de dados de CNPJs da Receita Federal é um dos datasets públicos mais volumosos e complexos do Brasil (~15 GB compactados). Este projeto demonstra como estruturar uma plataforma analítica moderna e de alta performance no **Databricks Lakehouse**, aplicando padrões corporativos de governança, processamento distribuído resiliente e otimização de custos e I/O.

### 🎯 Principais Competências Demonstradas:
- **Processamento Distribuído em Escala**: Ingestão e transformação de dezenas de milhões de linhas com PySpark.
- **Delta Lake Avançado**: Idempotência com `MERGE INTO` (Upsert), `OPTIMIZE`, `Z-ORDER BY`, e `Delta Time Travel`.
- **Governança com Unity Catalog**: Organização de 3 camadas (`cnpj_lakehouse.bronze/silver/gold`) e gerenciamento de arquivos brutos via **Volumes**.
- **Modelagem Dimensional (Kimball)**: Star Schema com Fatos particionadas e Dimensões enriquecidas via **Broadcast Joins**.
- **Resolução de Gargalos de Performance**: Mitigação de **Data Skew** com **Adaptive Query Execution (AQE)** e eliminação de shuffles desnecessários.
- **Orquestração**: Pipeline automatizado de ponta a ponta via **Databricks Workflows (DAG)**.

---

## 🏗️ Arquitetura Medalhão

```mermaid
flowchart TD
    subgraph Landing ["1. Landing Zone (Unity Catalog Volumes)"]
        Raw["Arquivos Brutos RFB (.csv / .zip)\nDelimitador: ';' | Encoding: ISO-8859-1"]
    end

    subgraph Bronze ["2. Camada Bronze (Raw Ingestion)"]
        B_Emp["bronze.bronze_empresas"]
        B_Est["bronze.bronze_estabelecimentos"]
        B_Soc["bronze.bronze_socios"]
        B_Dom["bronze.tabelas_dominio (CNAE, Municípios, etc.)"]
        Raw -->|01_bronze_ingestion.py\nSchema Enforcement + Audit Cols| Bronze
    end

    subgraph Silver ["3. Camada Silver (Enriched & Conformed)"]
        S_Emp["silver.silver_empresas\n(Capital Social tipado, Porte decodificado)"]
        S_Est["silver.silver_estabelecimentos\n(Datas formatadas, CNPJ 14d, Broadcast Joins)"]
        S_Soc["silver.silver_socios\n(Faixa etária e tipo de sócio)"]
        Bronze -->|02_silver_transformations.py\nData Cleaning + MERGE + Z-ORDER| Silver
    end

    subgraph Gold ["4. Camada Gold (Star Schema & Business KPIs)"]
        D_Emp["gold.dim_empresa"]
        D_Cnae["gold.dim_cnae"]
        D_Loc["gold.dim_localizacao"]
        F_Est["gold.fato_estabelecimentos\n(Particionada por UF / Z-Ordered)"]
        KPI1["gold.kpi_demografia_setorial_uf"]
        KPI2["gold.kpi_taxa_sobrevivencia_porte_cnae"]
        Silver -->|03_gold_star_schema.py\nDimensional Modeling| Gold
    end

    subgraph Serving ["5. Analytics & Serving"]
        DBSQL["Databricks SQL / Power BI / Dashboards\nQueries de Inteligência de Mercado"]
        Gold -->|04_business_analytics_kpis.sql| Serving
    end
```

---

## 📐 Modelo Dimensional (Star Schema)

A camada Gold implementa o modelo estrela para viabilizar consultas analíticas com tempos de resposta em segundos:

```mermaid
erDiagram
    dim_empresa ||--o{ fato_estabelecimentos : "possui estabelecimentos"
    dim_cnae ||--o{ fato_estabelecimentos : "classifica atividade"
    dim_localizacao ||--o{ fato_estabelecimentos : "localiza geograficamente"

    dim_empresa {
        string sk_empresa PK
        string cnpj_basico
        string razao_social
        string descricao_natureza_juridica
        decimal capital_social
        string descricao_porte
    }

    dim_cnae {
        string sk_cnae PK
        string codigo_cnae
        string descricao_cnae
        string divisao_cnae
        string macro_setor
    }

    dim_localizacao {
        string sk_localizacao PK
        string uf
        string regiao_brasil
        string municipio_cod
        string nome_municipio
    }

    fato_estabelecimentos {
        string sk_estabelecimento PK
        string sk_empresa FK
        string sk_cnae FK
        string sk_localizacao FK
        string cnpj_completo
        string tipo_unidade
        string descricao_situacao_cadastral
        date data_inicio_atividade
        date data_situacao_cadastral
        double tempo_atividade_anos
        int flg_ativo
        int flg_matriz
        string uf
    }
```

---

## 📂 Estrutura do Repositório

```text
cnpj-databricks-lakehouse/
├── notebooks/
│   ├── 00_environment_setup.py         # Criação de Catálogos, Schemas e Volumes no Unity Catalog
│   ├── 01_bronze_ingestion.py          # Ingestão raw com schema explícito e metadados de linhagem
│   ├── 02_silver_transformations.py     # Limpeza, tipagem, broadcast joins, MERGE e Z-ORDER
│   ├── 03_gold_star_schema.py          # Modelagem dimensional Star Schema e datamarts agregados
│   ├── 04_business_analytics_kpis.sql  # Consultas analíticas prontas para Databricks SQL
│   └── 05_performance_benchmarking.py  # Testes de tuning, mitigação de Skew e Data Skipping
├── pipelines/
│   └── workflow_job_config.json        # Definição de DAG do Databricks Workflows (Jobs)
├── sample_data/
│   ├── generate_sample_data.py         # Gerador de dados sintéticos para testes rápidos
│   └── raw_files/                      # Amostra gerada no formato oficial da Receita Federal
└── README.md                           # Documentação completa do projeto
```

---

## ⚡ Engenharia de Performance & Decisões Técnicas

| Desafio Técnico | Solução Aplicada no Databricks | Benefício / Impacto |
|---|---|---|
| **Arquivos CSV gigantes sem cabeçalho e tipagem fraca** | **Explicit Schema Enforcement** no PySpark | Evita varredura dupla dos arquivos para inferência, reduzindo o tempo de ingestão em ~60%. |
| **Joins de 50M de linhas com tabelas de domínio (CNAE, Municípios)** | **Broadcast Hash Joins (`broadcast()`)** | Elimina a etapa pesada de *Shuffle* pela rede para tabelas menores que o threshold de broadcast. |
| **Data Skew em grandes centros (Ex: São Paulo concentra >30% das empresas)** | **Adaptive Query Execution (AQE Skew Join)** | O Spark subdivide automaticamente partições assimétricas em tempo de execução, prevenindo nós *stragglers*. |
| **Consultas analíticas filtradas por UF e CNAE** | **Particionamento por UF + Delta Z-ORDER** | Ativa o **Data Skipping**, lendo até 85% menos arquivos de dados nos nós executores. |
| **Carga incremental sem duplicar registros** | **Delta Lake `MERGE INTO` (Upsert)** | Garante idempotência e consistência nos dados sem necessidade de recriar tabelas do zero. |

---

## 📊 Exemplos de Perguntas de Negócio Respondidas (Databricks SQL)

### 1. Top 10 Setores (CNAE) com Maior Volume de Abertura nos Últimos Anos:
```sql
SELECT 
    c.descricao_cnae,
    c.macro_setor,
    COUNT(f.sk_estabelecimento) AS total_aberturas,
    ROUND((SUM(f.flg_ativo) * 100.0) / COUNT(f.sk_estabelecimento), 2) AS taxa_sobrevivencia_pct
FROM gold.fato_estabelecimentos f
JOIN gold.dim_cnae c ON f.sk_cnae = c.sk_cnae
WHERE f.ano_inicio_atividade >= 2020
GROUP BY c.descricao_cnae, c.macro_setor
ORDER BY total_aberturas DESC
LIMIT 10;
```

### 2. Sobrevivência Média de Micro e Pequenas Empresas (PME) por Macro-Setor:
```sql
SELECT 
    c.macro_setor,
    e.descricao_porte,
    COUNT(f.sk_estabelecimento) AS total_encerradas,
    ROUND(AVG(f.tempo_atividade_anos), 2) AS media_anos_sobrevivencia
FROM gold.fato_estabelecimentos f
JOIN gold.dim_empresa e ON f.sk_empresa = e.sk_empresa
JOIN gold.dim_cnae c ON f.sk_cnae = c.sk_cnae
WHERE f.flg_ativo = 0 AND f.tempo_atividade_anos > 0
  AND e.descricao_porte IN ('MICRO EMPRESA (ME)', 'EMPRESA DE PEQUENO PORTE (EPP)')
GROUP BY c.macro_setor, e.descricao_porte
ORDER BY media_anos_sobrevivencia ASC;
```

---

## 🚀 Como Executar o Projeto

### Opção A: Execução no Databricks (Produção ou Amostra)
1. **Importar o Repositório**:
   - No seu Workspace do Databricks, vá em **Workspace > Repos > Add Repo** e cole o link do seu repositório no GitHub.
2. **Executar o Setup de Governança**:
   - Execute o notebook `notebooks/00_environment_setup.py` para criar o catálogo `cnpj_lakehouse`, schemas e volumes no Unity Catalog.
3. **Carregar os Arquivos de Entrada**:
   - Faça upload dos arquivos da RFB (ou da amostra gerada) para `/Volumes/cnpj_lakehouse/bronze/raw_landing/`.
4. **Executar a Pipeline**:
   - Execute os notebooks sequencialmente (`01_bronze_ingestion.py` -> `02_silver_transformations.py` -> `03_gold_star_schema.py`) ou importe o Job via `pipelines/workflow_job_config.json`.

### Opção B: Teste Local de Geração de Dados
Se desejar gerar a amostra localmente:
```bash
python sample_data/generate_sample_data.py
```

---

## 👨‍💻 Autor

Desenvolvido para demonstração de práticas avançadas de **Data Engineering**, **Databricks Lakehouse** e **Big Data com PySpark**.
