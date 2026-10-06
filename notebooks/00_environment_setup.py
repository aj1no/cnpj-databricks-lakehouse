"""
================================================================================
00_ENVIRONMENT_SETUP.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Plataforma: Databricks / Unity Catalog
Objetivo: Configuracao do ambiente, criacao do catalogo, schemas e volumes.
================================================================================
"""

# Databricks Notebook Source
# COMMAND ----------
# MAGIC %md
# MAGIC # 00 - Configuracao do Ambiente e Governanca (Unity Catalog)
# MAGIC 
# MAGIC Este notebook inicializa a infraestrutura de governanca do Lakehouse no Databricks utilizando Unity Catalog:
# MAGIC - Criacao do Catalogo `cnpj_lakehouse`
# MAGIC - Criacao dos Schemas da Arquitetura Medalhao (`bronze`, `silver`, `gold`)
# MAGIC - Criacao do Volume para recepcao dos arquivos brutos (Landing Zone)
# MAGIC - Configuracoes recomendadas de Spark para otimizacao de processamento

# COMMAND ----------
# 1. Configuracoes de Spark para Big Data & Otimizacao Delta Lake
spark.conf.set("spark.sql.streaming.schemaInference", "true")
spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Definir shuffle partitions adequado para o cluster (ajuste conforme o tamanho do cluster)
# 200 e o default do Spark; para datasets grandes (50M+ linhas), 200 a 400 e uma boa faixa
spark.conf.set("spark.sql.shuffle.partitions", "200")

print("[INFO] Configuracoes de Spark e Delta Lake aplicadas com sucesso.")

# COMMAND ----------
# 2. Definicao de Variaveis de Governanca (Unity Catalog)
CATALOG_NAME = "cnpj_lakehouse"
SCHEMAS = ["bronze", "silver", "gold"]
VOLUME_NAME = "raw_landing"

# COMMAND ----------
# 3. Criacao do Catalogo
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG_NAME} COMMENT 'Lakehouse de Dados Abertos de CNPJ da Receita Federal'")
spark.sql(f"USE CATALOG {CATALOG_NAME}")
print(f"[INFO] Catalogo '{CATALOG_NAME}' criado/ativado.")

# COMMAND ----------
# 4. Criacao dos Schemas (Camadas da Arquitetura Medalhao)
for schema in SCHEMAS:
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG_NAME}.{schema}
        COMMENT 'Camada {schema.upper()} da Arquitetura Medalhao'
    """)
    print(f"[INFO] Schema '{CATALOG_NAME}.{schema}' criado/verificado.")

# COMMAND ----------
# 5. Criacao do Volume no Unity Catalog para Arquivos Brutos (Landing Zone)
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {CATALOG_NAME}.bronze.{VOLUME_NAME}
    COMMENT 'Volume de armazenamento para arquivos brutos da RFB (.csv, .zip)'
""")

volume_path = f"/Volumes/{CATALOG_NAME}/bronze/{VOLUME_NAME}"
print(f"[INFO] Volume criado em: {volume_path}")

# COMMAND ----------
# 6. Exibir status da governanca
display(spark.sql(f"SHOW SCHEMAS IN {CATALOG_NAME}"))
