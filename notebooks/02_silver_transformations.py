"""
================================================================================
02_SILVER_TRANSFORMATIONS.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Plataforma: Databricks / PySpark / Delta Lake
Objetivo: Limpeza profunda, padronização, tipagem, enriquecimento e deduplicação na camada Silver.
================================================================================
"""

# Databricks Notebook Source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🥈 02 - Transformação e Qualidade na Camada Silver
# MAGIC 
# MAGIC ### Desafios e Técnicas Aplicadas:
# MAGIC 1. **Data Cleaning & Typing**: Conversão de strings de data (`AAAAMMDD` -> `DateType`), capital social com vírgula para `Decimal(16,2)` e formatação de CNPJ de 14 dígitos.
# MAGIC 2. **Broadcast Joins de Alta Performance**: Enriquecimento de tabelas de dezenas de milhões de linhas com tabelas dimensionais de domínio em memória sem shuffle.
# MAGIC 3. **Decodificação de Regras de Negócio**: Tradução de códigos oficiais da RFB (Porte, Matriz/Filial, Situação Cadastral, Faixa Etária).
# MAGIC 4. **Idempotência com Delta Lake `MERGE INTO`**: Upsert seguro para suportar re-processamento sem duplicar dados.
# MAGIC 5. **Otimização Delta (`OPTIMIZE` e `Z-ORDER`)**: Indexação multidimensional para acelerar consultas analíticas.
# MAGIC 
# MAGIC # COMMAND ----------
from pyspark.sql.functions import (
    col, trim, upper, lpad, concat, to_date, regexp_replace,
    when, current_timestamp, broadcast
)
from pyspark.sql.types import DecimalType
from delta.tables import DeltaTable

# COMMAND ----------
CATALOG_NAME = "cnpj_lakehouse"
spark.sql(f"USE CATALOG {CATALOG_NAME}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🏢 1. Transformação: Tabela `silver_empresas`

# COMMAND ----------
print("⏳ Processando 'silver_empresas'...")

df_bronze_emp = spark.table("bronze.bronze_empresas")
df_naturezas = spark.table("bronze.bronze_naturezas_juridicas")

df_silver_emp = (
    df_bronze_emp
    .withColumn("cnpj_basico", lpad(trim(col("cnpj_basico")), 8, "0"))
    .withColumn("razao_social", upper(trim(col("razao_social"))))
    .withColumn("natureza_juridica", lpad(trim(col("natureza_juridica")), 4, "0"))
    # Tratamento de Capital Social: de '1500,00' para Decimal(16, 2)
    .withColumn(
        "capital_social",
        regexp_replace(col("capital_social_str"), ",", ".").cast(DecimalType(16, 2))
    )
    # Decodificação do Porte da Empresa
    .withColumn(
        "descricao_porte",
        when(col("porte_empresa") == "01", "MICRO EMPRESA (ME)")
        .when(col("porte_empresa") == "03", "EMPRESA DE PEQUENO PORTE (EPP)")
        .when(col("porte_empresa") == "05", "DEMAIS")
        .otherwise("NÃO INFORMADO")
    )
    .withColumn("ente_federativo_responsavel", trim(col("ente_federativo_responsavel")))
    # Broadcast Join com Natureza Jurídica (Tabela pequena de lookup)
    .join(
        broadcast(df_naturezas.select(
            col("codigo").alias("nat_cod"),
            col("descricao").alias("descricao_natureza_juridica")
        )),
        col("natureza_juridica") == col("nat_cod"),
        "left"
    )
    .drop("nat_cod", "capital_social_str")
    .withColumn("_updated_at", current_timestamp())
)

# Escrita Delta na Camada Silver com MERGE INTO (Upsert por CNPJ Básico)
target_table_emp = "silver.silver_empresas"

if not spark.catalog.tableExists(target_table_emp):
    (
        df_silver_emp.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(target_table_emp)
    )
    print(f"✅ Tabela '{target_table_emp}' criada inicialmente.")
else:
    delta_target = DeltaTable.forName(spark, target_table_emp)
    (
        delta_target.alias("t")
        .merge(df_silver_emp.alias("s"), "t.cnpj_basico = s.cnpj_basico")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"✅ MERGE executado com sucesso em '{target_table_emp}'.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📍 2. Transformação: Tabela `silver_estabelecimentos` (Particionamento & Z-Order)

# COMMAND ----------
print("⏳ Processando 'silver_estabelecimentos'...")

df_bronze_est = spark.table("bronze.bronze_estabelecimentos")
df_cnae = spark.table("bronze.bronze_cnae")
df_municipios = spark.table("bronze.bronze_municipios")

df_silver_est = (
    df_bronze_est
    # Criação do CNPJ Completo Formatado (14 dígitos)
    .withColumn("cnpj_basico", lpad(trim(col("cnpj_basico")), 8, "0"))
    .withColumn("cnpj_ordem", lpad(trim(col("cnpj_ordem")), 4, "0"))
    .withColumn("cnpj_dv", lpad(trim(col("cnpj_dv")), 2, "0"))
    .withColumn(
        "cnpj_completo",
        concat(col("cnpj_basico"), col("cnpj_ordem"), col("cnpj_dv"))
    )
    # Identificador Matriz / Filial
    .withColumn(
        "tipo_unidade",
        when(col("identificador_matriz_filial") == "1", "MATRIZ")
        .when(col("identificador_matriz_filial") == "2", "FILIAL")
        .otherwise("OUTRO")
    )
    .withColumn("nome_fantasia", upper(trim(col("nome_fantasia"))))
    # Decodificação da Situação Cadastral
    .withColumn(
        "descricao_situacao_cadastral",
        when(col("situacao_cadastral") == "01", "NULA")
        .when(col("situacao_cadastral") == "02", "ATIVA")
        .when(col("situacao_cadastral") == "03", "SUSPENSA")
        .when(col("situacao_cadastral") == "04", "INAPTA")
        .when(col("situacao_cadastral") == "08", "BAIXADA")
        .otherwise("OUTRA")
    )
    # Parsing de Datas (AAAAMMDD -> DateType)
    .withColumn("data_situacao_cadastral", to_date(col("data_situacao_cadastral"), "yyyyMMdd"))
    .withColumn("data_inicio_atividade", to_date(col("data_inicio_atividade"), "yyyyMMdd"))
    .withColumn("cnae_fiscal_principal", lpad(trim(col("cnae_fiscal_principal")), 7, "0"))
    .withColumn("cep", lpad(regexp_replace(trim(col("cep")), "[^0-9]", ""), 8, "0"))
    .withColumn("uf", upper(trim(col("uf"))))
    .withColumn("municipio_cod", lpad(trim(col("municipio")), 4, "0"))
    # Broadcast Joins com CNAE e Município
    .join(
        broadcast(df_cnae.select(
            col("codigo").alias("cnae_cod"),
            col("descricao").alias("descricao_cnae_principal")
        )),
        col("cnae_fiscal_principal") == col("cnae_cod"),
        "left"
    )
    .join(
        broadcast(df_municipios.select(
            col("codigo").alias("mun_cod"),
            col("descricao").alias("nome_municipio")
        )),
        col("municipio_cod") == col("mun_cod"),
        "left"
    )
    .drop("cnae_cod", "mun_cod", "municipio")
    .withColumn("_updated_at", current_timestamp())
)

# Gravação com Particionamento por UF para otimizar queries regionais
target_table_est = "silver.silver_estabelecimentos"

(
    df_silver_est.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("uf")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table_est)
)

print(f"✅ Tabela '{target_table_est}' gravada e particionada por UF.")

# Otimização Delta Lake com Z-ORDER nos campos mais filtrados em queries analíticas
print("⏳ Executando OPTIMIZE com Z-ORDER...")
spark.sql(f"""
    OPTIMIZE {target_table_est}
    ZORDER BY (cnpj_basico, cnae_fiscal_principal, situacao_cadastral)
""")
print("✅ Z-ORDER concluído com sucesso!")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 👥 3. Transformação: Tabela `silver_socios`

# COMMAND ----------
print("⏳ Processando 'silver_socios'...")

df_bronze_soc = spark.table("bronze.bronze_socios")
df_qualif = spark.table("bronze.bronze_qualificacoes_socios")

df_silver_soc = (
    df_bronze_soc
    .withColumn("cnpj_basico", lpad(trim(col("cnpj_basico")), 8, "0"))
    .withColumn(
        "tipo_socio",
        when(col("identificador_socio") == "1", "PESSOA JURÍDICA")
        .when(col("identificador_socio") == "2", "PESSOA FÍSICA")
        .when(col("identificador_socio") == "3", "ESTRANGEIRO")
        .otherwise("NÃO INFORMADO")
    )
    .withColumn("nome_socio", upper(trim(col("nome_socio"))))
    .withColumn("data_entrada_sociedade", to_date(col("data_entrada_sociedade"), "yyyyMMdd"))
    .withColumn("qualificacao_socio", lpad(trim(col("qualificacao_socio")), 2, "0"))
    # Decodificação de Faixa Etária
    .withColumn(
        "descricao_faixa_etaria",
        when(col("faixa_etaria") == "1", "0 a 12 anos")
        .when(col("faixa_etaria") == "2", "13 a 20 anos")
        .when(col("faixa_etaria") == "3", "21 a 30 anos")
        .when(col("faixa_etaria") == "4", "31 a 40 anos")
        .when(col("faixa_etaria") == "5", "41 a 50 anos")
        .when(col("faixa_etaria") == "6", "51 a 60 anos")
        .when(col("faixa_etaria") == "7", "61 a 70 anos")
        .when(col("faixa_etaria") == "8", "71 a 80 anos")
        .when(col("faixa_etaria") == "9", "Mais de 80 anos")
        .otherwise("Não informado")
    )
    # Broadcast Join com Qualificação
    .join(
        broadcast(df_qualif.select(
            col("codigo").alias("qualif_cod"),
            col("descricao").alias("descricao_qualificacao_socio")
        )),
        col("qualificacao_socio") == col("qualif_cod"),
        "left"
    )
    .drop("qualif_cod")
    .withColumn("_updated_at", current_timestamp())
)

target_table_soc = "silver.silver_socios"
(
    df_silver_soc.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table_soc)
)

print(f"✅ Tabela '{target_table_soc}' gravada com sucesso.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔎 4. Validação das Tabelas Silver

# COMMAND ----------
display(spark.sql("""
    SELECT 
        'silver_empresas' as tabela, count(*) as total_linhas FROM silver.silver_empresas
    UNION ALL
    SELECT 
        'silver_estabelecimentos' as tabela, count(*) as total_linhas FROM silver.silver_estabelecimentos
    UNION ALL
    SELECT 
        'silver_socios' as tabela, count(*) as total_linhas FROM silver.silver_socios
"""))
