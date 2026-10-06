"""
================================================================================
01_BRONZE_INGESTION.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Plataforma: Databricks / PySpark / Delta Lake
Objetivo: Ingestão de dados brutos (CSV/ISO-8859-1) para tabelas Delta na camada Bronze com Schema Enforcement e Metadados.
================================================================================
"""

# Databricks Notebook Source
# COMMAND ----------
# MAGIC %md
# MAGIC # 🥉 01 - Ingestão Camada Bronze (Raw -> Delta Lake)
# MAGIC 
# MAGIC ### Objetivos:
# MAGIC 1. Ingerir os arquivos públicos da Receita Federal (formato delimitado por ponto-e-vírgula `;`, sem cabeçalho, encoding `ISO-8859-1`).
# MAGIC 2. Aplicar **Schema Enforcement** explícito para evitar inferências lentas e erros em Big Data (50M+ linhas).
# MAGIC 3. Adicionar **metadados de auditoria** (`_ingest_timestamp`, `_source_file`).
# MAGIC 4. Salvar como tabelas **Delta Lake** na camada `cnpj_lakehouse.bronze`.

# COMMAND ----------
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, input_file_name, col

# COMMAND ----------
# 1. Configurações de Caminho
CATALOG_NAME = "cnpj_lakehouse"
VOLUME_PATH = f"/Volumes/{CATALOG_NAME}/bronze/raw_landing"
BRONZE_SCHEMA = f"{CATALOG_NAME}.bronze"

spark.sql(f"USE CATALOG {CATALOG_NAME}")
spark.sql(f"USE SCHEMA bronze")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 📋 2. Definição Explícita de Schemas (Padrão Receita Federal)

# COMMAND ----------
# Schema: EMPRESAS (~50M+ registros)
schema_empresas = StructType([
    StructField("cnpj_basico", StringType(), False),
    StructField("razao_social", StringType(), True),
    StructField("natureza_juridica", StringType(), True),
    StructField("qualificacao_responsavel", StringType(), True),
    StructField("capital_social_str", StringType(), True), # Formato '0,00'
    StructField("porte_empresa", StringType(), True),
    StructField("ente_federativo_responsavel", StringType(), True)
])

# Schema: ESTABELECIMENTOS (~55M+ registros)
schema_estabelecimentos = StructType([
    StructField("cnpj_basico", StringType(), False),
    StructField("cnpj_ordem", StringType(), False),
    StructField("cnpj_dv", StringType(), False),
    StructField("identificador_matriz_filial", StringType(), True),
    StructField("nome_fantasia", StringType(), True),
    StructField("situacao_cadastral", StringType(), True),
    StructField("data_situacao_cadastral", StringType(), True),
    StructField("motivo_situacao_cadastral", StringType(), True),
    StructField("nome_cidade_exterior", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("data_inicio_atividade", StringType(), True),
    StructField("cnae_fiscal_principal", StringType(), True),
    StructField("cnae_fiscal_secundaria", StringType(), True),
    StructField("tipo_logradouro", StringType(), True),
    StructField("logradouro", StringType(), True),
    StructField("numero", StringType(), True),
    StructField("complemento", StringType(), True),
    StructField("bairro", StringType(), True),
    StructField("cep", StringType(), True),
    StructField("uf", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("ddd_1", StringType(), True),
    StructField("telefone_1", StringType(), True),
    StructField("ddd_2", StringType(), True),
    StructField("telefone_2", StringType(), True),
    StructField("ddd_fax", StringType(), True),
    StructField("fax", StringType(), True),
    StructField("correio_eletronico", StringType(), True),
    StructField("situacao_especial", StringType(), True),
    StructField("data_situacao_especial", StringType(), True)
])

# Schema: SÓCIOS (~25M+ registros)
schema_socios = StructType([
    StructField("cnpj_basico", StringType(), False),
    StructField("identificador_socio", StringType(), True),
    StructField("nome_socio", StringType(), True),
    StructField("cnpj_cpf_socio", StringType(), True),
    StructField("qualificacao_socio", StringType(), True),
    StructField("data_entrada_sociedade", StringType(), True),
    StructField("pais", StringType(), True),
    StructField("representante_legal", StringType(), True),
    StructField("nome_representante", StringType(), True),
    StructField("qualificacao_representante", StringType(), True),
    StructField("faixa_etaria", StringType(), True)
])

# Schema: Tabelas de Domínio / Lookup (CNAEs, Municípios, Naturezas Jurídicas, etc.)
schema_lookup = StructType([
    StructField("codigo", StringType(), False),
    StructField("descricao", StringType(), True)
])

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔄 3. Função Genérica de Ingestão para Delta Bronze

# COMMAND ----------
def ingest_raw_to_bronze(file_pattern: str, schema: StructType, table_name: str, delimiter: str = ";", encoding: str = "ISO-8859-1"):
    """
    Lê arquivos delimitados brutos, adiciona metadados de auditoria e grava como Delta Table.
    """
    input_path = f"{VOLUME_PATH}/{file_pattern}"
    print(f"⏳ Iniciando ingestão de '{file_pattern}' para '{BRONZE_SCHEMA}.{table_name}'...")
    
    df_raw = (
        spark.read
        .format("csv")
        .option("sep", delimiter)
        .option("encoding", encoding)
        .option("quote", "\"")
        .option("escape", "\"")
        .option("header", "false")
        .schema(schema)
        .load(input_path)
    )
    
    # Adicionar colunas de auditoria/linhagem
    df_bronze = (
        df_raw
        .withColumn("_ingest_timestamp", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )
    
    # Salvar em Delta Lake
    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{BRONZE_SCHEMA}.{table_name}")
    )
    
    count = spark.table(f"{BRONZE_SCHEMA}.{table_name}").count()
    print(f"✅ Tabela '{BRONZE_SCHEMA}.{table_name}' gravada com sucesso! Total de registros: {count:,}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🚀 4. Execução da Ingestão

# COMMAND ----------
# Ingestão das Tabelas Principais
# Observação: em produção, substitua os padrões pelos arquivos correspondentes (ex: *EMPRECSV*, *ESTABELE*, *SOCIOCSV*)
datasets_to_ingest = [
    ("empresas/*.csv", schema_empresas, "bronze_empresas"),
    ("estabelecimentos/*.csv", schema_estabelecimentos, "bronze_estabelecimentos"),
    ("socios/*.csv", schema_socios, "bronze_socios"),
    ("cnaes/*.csv", schema_lookup, "bronze_cnae"),
    ("municipios/*.csv", schema_lookup, "bronze_municipios"),
    ("naturezas/*.csv", schema_lookup, "bronze_naturezas_juridicas"),
    ("qualificacoes/*.csv", schema_lookup, "bronze_qualificacoes_socios"),
    ("motivos/*.csv", schema_lookup, "bronze_motivos_situacao_cadastral")
]

for file_pattern, schema, table_name in datasets_to_ingest:
    try:
        ingest_raw_to_bronze(file_pattern, schema, table_name)
    except Exception as e:
        print(f"⚠️ Aviso: Arquivo '{file_pattern}' não encontrado ou erro na ingestão: {str(e)}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 🔎 5. Verificação da Camada Bronze

# COMMAND ----------
display(spark.sql("SHOW TABLES IN cnpj_lakehouse.bronze"))
