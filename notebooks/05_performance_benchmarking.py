"""
================================================================================
05_PERFORMANCE_BENCHMARKING.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Plataforma: Databricks / PySpark / Delta Lake Engine
Objetivo: Demonstracao de Tecnicas Avancadas de Otimizacao e Tuning em Big Data.
================================================================================
"""

# Databricks Notebook Source
# COMMAND ----------
# MAGIC %md
# MAGIC # 05 - Engenharia de Performance & Tuning no Databricks
# MAGIC 
# MAGIC Este notebook demonstra tecnicas avancadas de otimizacao de consultas e processamento distribuido em cenarios de Big Data (50M+ registros):
# MAGIC 
# MAGIC 1. Broadcast Hash Join vs Sort-Merge Shuffle Join
# MAGIC 2. Adaptive Query Execution (AQE) & Tratamento de Data Skew (Desbalanceamento de dados em UFs e capitais como SP)
# MAGIC 3. Data Skipping com Delta Lake Z-ORDER
# MAGIC 4. Analise de Planos Fisicos de Execucao (`explain(True)`)

# COMMAND ----------
import time
from pyspark.sql.functions import col, broadcast

spark.sql("USE CATALOG cnpj_lakehouse")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1. Otimizacao de Joins: Broadcast Hash Join

# COMMAND ----------
# MAGIC %md
# MAGIC Em tabelas de dimensoes pequenas (ex: CNAE com ~1.300 linhas ou Municipios com ~5.500 linhas), 
# MAGIC o Spark pode evitar o Shuffle de 50 milhoes de linhas da tabela fato enviando uma copia em broadcast para cada executor.

df_fato = spark.table("gold.fato_estabelecimentos")
df_cnae = spark.table("gold.dim_cnae")

# 1.1 Consulta com Broadcast Explicito
t0 = time.time()
df_com_broadcast = df_fato.join(broadcast(df_cnae), "sk_cnae").count()
t1 = time.time()
tempo_broadcast = t1 - t0
print(f"[BENCHMARK] Tempo com Broadcast Join: {tempo_broadcast:.2f} segundos")

# 1.2 Exibir Plano de Execucao Fisico
print("\n[PLAN] Plano de Execucao Fisico (BroadcastHashJoin):")
df_fato.join(broadcast(df_cnae), "sk_cnae").explain()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Adaptive Query Execution (AQE) & Tratamento de Skew

# COMMAND ----------
# MAGIC %md
# MAGIC O estado de Sao Paulo (SP) e cidades grandes concentram mais de 30% de todas as empresas do pais, gerando Data Skew (particoes lentas que travam o cluster).
# MAGIC 
# MAGIC No Databricks moderno, habilitamos o AQE Skew Join para que o Spark particione automaticamente as chaves sobrecarregadas:

spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "64MB")

print("[INFO] Adaptive Query Execution configurado para mitigacao automatica de Data Skew.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3. Eficiencia de I/O com Z-ORDER (Data Skipping)

# COMMAND ----------
# MAGIC %md
# MAGIC O Z-ORDER agrupa os dados fisicamente nos arquivos Parquet/Delta seguindo uma curva de preenchimento de espaco.
# MAGIC Quando filtramos por `uf` e `cnae_fiscal_principal`, o Delta Lake le apenas uma fracao dos arquivos do disco.

query_seletiva = """
    SELECT 
        cnpj_completo,
        tipo_unidade,
        data_inicio_atividade
    FROM silver.silver_estabelecimentos
    WHERE uf = 'SP' AND cnae_fiscal_principal = '6201501' -- Desenvolvimento de programas de computador
"""

t0 = time.time()
resultado = spark.sql(query_seletiva).collect()
t1 = time.time()
print(f"[BENCHMARK] Tempo de execucao da consulta filtrada: {t1 - t0:.2f} segundos (Registros retornados: {len(resultado):,})")
