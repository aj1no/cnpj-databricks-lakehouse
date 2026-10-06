"""
================================================================================
03_GOLD_STAR_SCHEMA.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Plataforma: Databricks / PySpark / Delta Lake
Objetivo: Modelagem Dimensional (Star Schema / Kimball) e Criação de Datamarts Analíticos na Camada Gold.
================================================================================
"""

# Databricks Notebook Source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🥇 03 - Modelagem Dimensional (Star Schema) e Datamarts na Camada Gold
# MAGIC 
# MAGIC ### Arquitetura da Camada Gold:
# MAGIC 
# MAGIC ```
# MAGIC             ┌─────────────────────────┐
# MAGIC             │       dim_empresa       │
# MAGIC             └────────────┬────────────┘
# MAGIC                          │
# MAGIC ┌─────────────────┐      │      ┌─────────────────────┐
# MAGIC │    dim_cnae     ├──────┼──────┤   dim_localizacao   │
# MAGIC └────────┬────────┘      │      └──────────┬──────────┘
# MAGIC          │               │                 │
# MAGIC          └───────────────┼─────────────────┘
# MAGIC                          ▼
# MAGIC             ┌─────────────────────────┐
# MAGIC             │  fato_estabelecimentos  │
# MAGIC             └─────────────────────────┘
# MAGIC                          │
# MAGIC          ┌───────────────┴───────────────┐
# MAGIC          ▼                               ▼
# MAGIC ┌─────────────────────────┐ ┌─────────────────────────┐
# MAGIC │kpi_demografia_setorial  │ │kpi_sobrevivencia_cnae   │
# MAGIC └─────────────────────────┘ └─────────────────────────┘
# MAGIC ```

# COMMAND ----------
from pyspark.sql.functions import (
    col, sha2, concat_ws, when, year, datediff, current_date,
    count, sum as _sum, avg, round as _round, lit
)

CATALOG_NAME = "cnpj_lakehouse"
spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql("USE SCHEMA gold")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📐 1. Criação das Dimensões (Kimball)

# COMMAND ----------
# 1.1 Dimensão Localização (UF, Município, Região Geográfica do Brasil)
print("⏳ Construindo 'dim_localizacao'...")

df_silver_est = spark.table("silver.silver_estabelecimentos")

df_dim_loc = (
    df_silver_est
    .select("uf", "municipio_cod", "nome_municipio")
    .distinct()
    .withColumn(
        "regiao_brasil",
        when(col("uf").isin("AM", "PA", "AC", "RO", "RR", "AP", "TO"), "Norte")
        .when(col("uf").isin("MA", "PI", "CE", "RN", "PB", "PE", "AL", "SE", "BA"), "Nordeste")
        .when(col("uf").isin("MT", "MS", "GO", "DF"), "Centro-Oeste")
        .when(col("uf").isin("SP", "RJ", "MG", "ES"), "Sudeste")
        .when(col("uf").isin("PR", "SC", "RS"), "Sul")
        .otherwise("Exterior / Não Identificado")
    )
    .withColumn("sk_localizacao", sha2(concat_ws("||", col("uf"), col("municipio_cod")), 256))
)

df_dim_loc.write.format("delta").mode("overwrite").saveAsTable("gold.dim_localizacao")
print("✅ 'dim_localizacao' criada.")

# COMMAND ----------
# 1.2 Dimensão CNAE (Atividade Econômica e Grandes Setores)
print("⏳ Construindo 'dim_cnae'...")

df_bronze_cnae = spark.table("bronze.bronze_cnae")

df_dim_cnae = (
    df_bronze_cnae
    .withColumnRenamed("codigo", "codigo_cnae")
    .withColumnRenamed("descricao", "descricao_cnae")
    # Derivação dos primeiros 2 dígitos para Divisão CNAE
    .withColumn("divisao_cnae", col("codigo_cnae").substr(1, 2))
    # Mapeamento de Macro-Setores
    .withColumn(
        "macro_setor",
        when(col("divisao_cnae").cast("int").between(1, 3), "Agropecuária")
        .when(col("divisao_cnae").cast("int").between(5, 39), "Indústria")
        .when(col("divisao_cnae").cast("int").between(41, 43), "Construção")
        .when(col("divisao_cnae").cast("int").between(45, 47), "Comércio")
        .when(col("divisao_cnae").cast("int").between(49, 99), "Serviços")
        .otherwise("Outros")
    )
    .withColumn("sk_cnae", sha2(col("codigo_cnae"), 256))
)

df_dim_cnae.write.format("delta").mode("overwrite").saveAsTable("gold.dim_cnae")
print("✅ 'dim_cnae' criada.")

# COMMAND ----------
# 1.3 Dimensão Empresa
print("⏳ Construindo 'dim_empresa'...")

df_silver_emp = spark.table("silver.silver_empresas")

df_dim_emp = (
    df_silver_emp
    .withColumn("sk_empresa", sha2(col("cnpj_basico"), 256))
)

df_dim_emp.write.format("delta").mode("overwrite").saveAsTable("gold.dim_empresa")
print("✅ 'dim_empresa' criada.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📊 2. Criação da Tabela Fato: `fato_estabelecimentos`

# COMMAND ----------
print("⏳ Construindo 'fato_estabelecimentos'...")

df_fato = (
    df_silver_est
    .withColumn("sk_estabelecimento", sha2(col("cnpj_completo"), 256))
    .withColumn("sk_empresa", sha2(col("cnpj_basico"), 256))
    .withColumn("sk_cnae", sha2(col("cnae_fiscal_principal"), 256))
    .withColumn("sk_localizacao", sha2(concat_ws("||", col("uf"), col("municipio_cod")), 256))
    # Flags e Métricas
    .withColumn("flg_ativo", when(col("situacao_cadastral") == "02", 1).otherwise(0))
    .withColumn("flg_matriz", when(col("tipo_unidade") == "MATRIZ", 1).otherwise(0))
    .withColumn("ano_inicio_atividade", year(col("data_inicio_atividade")))
    .withColumn(
        "tempo_atividade_anos",
        _round(datediff(
            when(col("flg_ativo") == 1, current_date()).otherwise(col("data_situacao_cadastral")),
            col("data_inicio_atividade")
        ) / 365.25, 2)
    )
    .select(
        "sk_estabelecimento",
        "sk_empresa",
        "sk_cnae",
        "sk_localizacao",
        "cnpj_completo",
        "cnpj_basico",
        "tipo_unidade",
        "situacao_cadastral",
        "descricao_situacao_cadastral",
        "data_inicio_atividade",
        "data_situacao_cadastral",
        "ano_inicio_atividade",
        "tempo_atividade_anos",
        "flg_ativo",
        "flg_matriz",
        "uf"
    )
)

(
    df_fato.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("uf")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.fato_estabelecimentos")
)

spark.sql("OPTIMIZE gold.fato_estabelecimentos ZORDER BY (ano_inicio_atividade, situacao_cadastral)")
print("✅ 'fato_estabelecimentos' gravada e otimizada!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📈 3. Datamarts & KPIs Analíticos de Negócio

# COMMAND ----------
# Datamart 1: Demografia de Empresas por Macro-Setor e UF
print("⏳ Gerando 'kpi_demografia_setorial_uf'...")

df_kpi_demografia = spark.sql("""
    SELECT 
        loc.regiao_brasil,
        f.uf,
        cnae.macro_setor,
        f.ano_inicio_atividade,
        COUNT(f.sk_estabelecimento) AS total_estabelecimentos,
        SUM(f.flg_ativo) AS total_ativos,
        SUM(f.flg_matriz) AS total_matrizes,
        ROUND((SUM(f.flg_ativo) * 100.0) / COUNT(f.sk_estabelecimento), 2) AS taxa_atividade_pct
    FROM gold.fato_estabelecimentos f
    JOIN gold.dim_cnae cnae ON f.sk_cnae = cnae.sk_cnae
    JOIN gold.dim_localizacao loc ON f.sk_localizacao = loc.sk_localizacao
    WHERE f.ano_inicio_atividade >= 2000
    GROUP BY 
        loc.regiao_brasil,
        f.uf,
        cnae.macro_setor,
        f.ano_inicio_atividade
""")

df_kpi_demografia.write.format("delta").mode("overwrite").saveAsTable("gold.kpi_demografia_setorial_uf")
print("✅ 'kpi_demografia_setorial_uf' criada.")

# COMMAND ----------
# Datamart 2: Tempo Médio de Sobrevivência por Porte e CNAE
print("⏳ Gerando 'kpi_taxa_sobrevivencia_porte_cnae'...")

df_kpi_sobrevivencia = spark.sql("""
    SELECT 
        emp.descricao_porte,
        cnae.macro_setor,
        cnae.descricao_cnae,
        COUNT(f.sk_estabelecimento) AS volume_empresas,
        ROUND(AVG(f.tempo_atividade_anos), 2) AS media_anos_sobrevivencia,
        SUM(f.flg_ativo) AS qtd_ativas,
        ROUND((SUM(f.flg_ativo) * 100.0) / COUNT(f.sk_estabelecimento), 2) AS taxa_sobrevivencia_pct
    FROM gold.fato_estabelecimentos f
    JOIN gold.dim_empresa emp ON f.sk_empresa = emp.sk_empresa
    JOIN gold.dim_cnae cnae ON f.sk_cnae = cnae.sk_cnae
    GROUP BY 
        emp.descricao_porte,
        cnae.macro_setor,
        cnae.descricao_cnae
    HAVING COUNT(f.sk_estabelecimento) >= 100
""")

df_kpi_sobrevivencia.write.format("delta").mode("overwrite").saveAsTable("gold.kpi_taxa_sobrevivencia_porte_cnae")
print("✅ 'kpi_taxa_sobrevivencia_porte_cnae' criada.")
