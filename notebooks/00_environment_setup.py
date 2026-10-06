"""
================================================================================
00_ENVIRONMENT_SETUP.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Plataforma: Databricks / Unity Catalog
Objetivo: Configuração do ambiente, criação do catálogo, schemas e volumes.
================================================================================
"""

# Databricks Notebook Source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🏛️ 00 - Configuração do Ambiente e Governança (Unity Catalog)
# MAGIC 
# MAGIC Este notebook inicializa a infraestrutura de governança do Lakehouse no Databricks utilizando **Unity Catalog**:
# MAGIC - Criação do Catálogo `cnpj_lakehouse`
# MAGIC - Criação dos Schemas da Arquitetura Medalhão (`bronze`, `silver`, `gold`)
# MAGIC - Criação do Volume para recepção dos arquivos brutos (Landing Zone)
# MAGIC - Configurações recomendadas de Spark para otimização de processamento

# COMMAND ----------
# 1. Configurações de Spark para Big Data & Otimização Delta Lake
spark.conf.set("spark.sql.streaming.schemaInference", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Definir shuffle partitions adequado para o cluster (ajuste conforme o tamanho do cluster)
# 200 é o default do Spark; para datasets grandes (50M+ linhas), 200 a 400 é uma boa faixa
spark.conf.set("spark.sql.shuffle.partitions", "200")

print("✅ Configurações de Spark e Delta Lake aplicadas com sucesso.")

# COMMAND ----------
# 2. Definição de Variáveis de Governança (Unity Catalog)
CATALOG_NAME = "cnpj_lakehouse"
SCHEMAS = ["bronze", "silver", "gold"]
VOLUME_NAME = "raw_landing"

# COMMAND ----------
# 3. Criação do Catálogo
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME} COMMENT 'Lakehouse de Dados Abertos de CNPJ da Receita Federal'")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
print(f"✅ Catálogo '{CATALOG_NAME}' criado/ativado.")

# COMMAND ----------
# 4. Criação dos Schemas (Camadas da Arquitetura Medalhão)
for schema in SCHEMAS:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}
        COMMENT 'Camada {schema.upper()} da Arquitetura Medalhão'
    """)
    print(f"✅ Schema '{CATALOG_NAME}.{schema}' criado/verificado.")

# COMMAND ----------
# 5. Criação do Volume no Unity Catalog para Arquivos Brutos (Landing Zone)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.bronze.{VOLUME_NAME}
    COMMENT 'Volume de armazenamento para arquivos brutos da RFB (.csv, .zip)'
""")

volume_path = f"/Volumes/{CATALOG_NAME}/bronze/{VOLUME_NAME}"
print(f"✅ Volume criado em: {volume_path}")

# COMMAND ----------
# 6. Exibir status da governança
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}"))
