-- ================================================================================
-- 04_BUSINESS_ANALYTICS_KPIS.SQL
-- Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
-- Plataforma: Databricks SQL / Analytics Engine
-- Objetivo: Consultas de inteligencia de mercado, demografia empresarial e benchmarking.
-- ================================================================================

USE CATALOG cnpj_lakehouse;
USE SCHEMA gold;

-- ================================================================================
-- QUERY 1: Top 10 CNAEs com Maior Volume de Abertura de Empresas Pos-2020
-- Identifica os setores da economia que mais abriram novos negocios no Brasil recentemente.
-- ================================================================================
SELECT 
    c.codigo_cnae,
    c.descricao_cnae,
    c.macro_setor,
    COUNT(f.sk_estabelecimento) AS total_aberturas_pos_2020,
    SUM(f.flg_ativo) AS total_atualmente_ativas,
    ROUND((SUM(f.flg_ativo) * 100.0) / COUNT(f.sk_estabelecimento), 2) AS taxa_sobrevivencia_pct
FROM gold.fato_estabelecimentos f
JOIN gold.dim_cnae c ON f.sk_cnae = c.sk_cnae
WHERE f.ano_inicio_atividade >= 2020
GROUP BY c.codigo_cnae, c.descricao_cnae, c.macro_setor
ORDER BY total_aberturas_pos_2020 DESC
LIMIT 10;


-- ================================================================================
-- QUERY 2: Ranking de Estados (UF) por Densidade e Taxa de Atividade de Empresas
-- Avalia quais estados tem maior percentual de empresas ativas vs encerradas/baixadas.
-- ================================================================================
SELECT 
    l.regiao_brasil,
    f.uf,
    COUNT(f.sk_estabelecimento) AS total_empresas_historicas,
    SUM(f.flg_ativo) AS empresas_ativas,
    ROUND(SUM(f.flg_ativo) * 100.0 / COUNT(f.sk_estabelecimento), 2) AS percentual_empresas_ativas,
    ROUND(AVG(CASE WHEN f.flg_ativo = 1 THEN f.tempo_atividade_anos END), 1) AS media_idade_empresas_ativas_anos
FROM gold.fato_estabelecimentos f
JOIN gold.dim_localizacao l ON f.sk_localizacao = l.sk_localizacao
GROUP BY l.regiao_brasil, f.uf
ORDER BY percentual_empresas_ativas DESC;


-- ================================================================================
-- QUERY 3: Concentracao de Capital Social por Macro-Setor e Porte
-- Analisa onde esta concentrado o capital investido formalmente no Brasil.
-- ================================================================================
SELECT 
    c.macro_setor,
    e.descricao_porte,
    COUNT(DISTINCT e.cnpj_basico) AS total_empresas,
    ROUND(SUM(e.capital_social) / 1000000000.0, 2) AS capital_social_total_bilhoes_brl,
    ROUND(AVG(e.capital_social), 2) AS capital_social_medio_brl
FROM gold.dim_empresa e
JOIN gold.fato_estabelecimentos f ON e.sk_empresa = f.sk_empresa
JOIN gold.dim_cnae c ON f.sk_cnae = c.sk_cnae
WHERE f.flg_matriz = 1 AND f.flg_ativo = 1
GROUP BY c.macro_setor, e.descricao_porte
ORDER BY capital_social_total_bilhoes_brl DESC;


-- ================================================================================
-- QUERY 4: Sobrevivencia de PMEs (Micro e Pequeno Porte) por Setor
-- Compara quanto tempo em media dura uma microempresa antes de ser baixada/inativada.
-- ================================================================================
SELECT 
    c.macro_setor,
    e.descricao_porte,
    COUNT(f.sk_estabelecimento) AS total_empresas_encerradas,
    ROUND(AVG(f.tempo_atividade_anos), 2) AS media_anos_sobrevivencia
FROM gold.fato_estabelecimentos f
JOIN gold.dim_empresa e ON f.sk_empresa = e.sk_empresa
JOIN gold.dim_cnae c ON f.sk_cnae = c.sk_cnae
WHERE f.flg_ativo = 0 AND f.tempo_atividade_anos > 0
  AND e.descricao_porte IN ('MICRO EMPRESA (ME)', 'EMPRESA DE PEQUENO PORTE (EPP)')
GROUP BY c.macro_setor, e.descricao_porte
ORDER BY media_anos_sobrevivencia ASC;


-- ================================================================================
-- QUERY 5: Demonstracao de Delta Lake Time Travel & Historico de Auditoria
-- Permite consultar o historico de versoes e alteracoes da tabela Delta.
-- ================================================================================
-- 5.1 Ver historico de versoes da tabela Delta
DESCRIBE HISTORY silver.silver_estabelecimentos;

-- 5.2 Consultar uma versao especifica no tempo (Exemplo: Versao 0)
-- SELECT COUNT(*) FROM silver.silver_estabelecimentos VERSION AS OF 0;
