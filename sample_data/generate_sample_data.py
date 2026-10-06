"""
================================================================================
GENERATE_SAMPLE_DATA.PY
Projeto: CNPJ Data Lakehouse (Receita Federal do Brasil)
Objetivo: Gerador de dados sintéticos estruturados no padrão exato dos arquivos da Receita Federal (RFB).
Permite testar o pipeline completo localmente ou no Databricks Community Edition.
================================================================================
"""

import os
import sys
import csv
import random
from datetime import datetime, timedelta

# Garantir compatibilidade de saída UTF-8 no Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")

SAMPLE_DIR = os.path.join(os.path.dirname(__file__), "raw_files")

def ensure_dirs():
    subdirs = ["empresas", "estabelecimentos", "socios", "cnaes", "municipios", "naturezas", "qualificacoes", "motivos"]
    for d in subdirs:
        os.makedirs(os.path.join(SAMPLE_DIR, d), exist_ok=True)

def generate_cnaes():
    cnaes = [
        ("6201501", "Desenvolvimento de programas de computador sob encomenda"),
        ("6202300", "Desenvolvimento e licenciamento de programas de computador customizáveis"),
        ("6204000", "Consultoria em tecnologia da informação"),
        ("4711302", "Comércio varejista de mercadorias em geral com predominância de produtos alimentícios"),
        ("4751201", "Comércio varejista especializado de equipamentos e suprimentos de informática"),
        ("5611201", "Restaurantes e similares"),
        ("6920601", "Atividades de contabilidade"),
        ("7020400", "Atividades de consultoria em gestão empresarial"),
        ("8630503", "Atividade médica ambulatorial restrita a consultas"),
        ("4120400", "Construção de edifícios")
    ]
    filepath = os.path.join(SAMPLE_DIR, "cnaes", "cnaes.csv")
    with open(filepath, "w", encoding="ISO-8859-1", newline="") as f:
        writer = csv.writer(f, delimiter=";", quotechar='"')
        for cod, desc in cnaes:
            writer.writerow([cod, desc])
    print(f"✅ Gerado: {filepath}")

def generate_municipios():
    municipios = [
        ("7107", "SAO PAULO"),
        ("6001", "RIO DE JANEIRO"),
        ("4123", "BELO HORIZONTE"),
        ("5847", "PORTO ALEGRE"),
        ("7535", "CURITIBA"),
        ("9701", "BRASILIA"),
        ("3849", "SALVADOR"),
        ("1389", "RECIFE"),
        ("2587", "FORTALEZA"),
        ("0255", "MANAUS")
    ]
    filepath = os.path.join(SAMPLE_DIR, "municipios", "municipios.csv")
    with open(filepath, "w", encoding="ISO-8859-1", newline="") as f:
        writer = csv.writer(f, delimiter=";", quotechar='"')
        for cod, desc in municipios:
            writer.writerow([cod, desc])
    print(f"✅ Gerado: {filepath}")

def generate_naturezas():
    naturezas = [
        ("2062", "SOCIEDADE EMPRESARIA LIMITADA"),
        ("2054", "SOCIEDADE ANONIMA FECHADA"),
        ("2046", "SOCIEDADE ANONIMA ABERTA"),
        ("2135", "EMPRESARIO INDIVIDUAL"),
        ("2305", "EMPRESA INDIVIDUAL DE RESPONSABILIDADE LIMITADA (EIRELI)")
    ]
    filepath = os.path.join(SAMPLE_DIR, "naturezas", "naturezas.csv")
    with open(filepath, "w", encoding="ISO-8859-1", newline="") as f:
        writer = csv.writer(f, delimiter=";", quotechar='"')
        for cod, desc in naturezas:
            writer.writerow([cod, desc])
    print(f"✅ Gerado: {filepath}")

def generate_qualificacoes():
    qualificacoes = [
        ("49", "Sócio-Administrador"),
        ("22", "Sócio"),
        ("10", "Diretor"),
        ("05", "Administrador"),
        ("65", "Titular Pessoa Física Residente ou Domiciliado no Brasil")
    ]
    filepath = os.path.join(SAMPLE_DIR, "qualificacoes", "qualificacoes.csv")
    with open(filepath, "w", encoding="ISO-8859-1", newline="") as f:
        writer = csv.writer(f, delimiter=";", quotechar='"')
        for cod, desc in qualificacoes:
            writer.writerow([cod, desc])
    print(f"✅ Gerado: {filepath}")

def generate_companies_and_establishments(n=1000):
    ufs = [("SP", "7107"), ("RJ", "6001"), ("MG", "4123"), ("RS", "5847"), ("PR", "7535"), ("DF", "9701"), ("BA", "3849"), ("PE", "1389"), ("CE", "2587"), ("AM", "0255")]
    cnae_codes = ["6201501", "6202300", "6204000", "4711302", "4751201", "5611201", "6920601", "7020400", "8630503", "4120400"]
    portes = ["01", "03", "05"]
    situacoes = ["02", "02", "02", "02", "08", "04"] # mais ativas que baixadas
    
    emp_rows = []
    est_rows = []
    soc_rows = []
    
    first_names = ["SILVA", "SANTOS", "OLIVEIRA", "SOUZA", "RODRIGUES", "FERREIRA", "ALVES", "PEREIRA", "LIMA", "GOMES"]
    last_names = ["TECNOLOGIA", "CONSULTORIA", "COMERCIO", "SOLUCOES", "LOGISTICA", "ALIMENTOS", "ENGENHARIA", "SERVICOS"]

    for i in range(1, n + 1):
        cnpj_basico = f"{i:08d}"
        razao_social = f"{random.choice(first_names)} & {random.choice(first_names)} {random.choice(last_names)} LTDA"
        nat_jur = random.choice(["2062", "2054", "2135"])
        capital = f"{random.randint(5000, 5000000)},{random.choice(['00', '50'])}"
        porte = random.choice(portes)
        
        emp_rows.append([cnpj_basico, razao_social, nat_jur, "49", capital, porte, ""])
        
        # Gerar Matriz
        uf, mun_cod = random.choice(ufs)
        cnae = random.choice(cnae_codes)
        sit = random.choice(situacoes)
        
        start_year = random.randint(2005, 2024)
        start_month = random.randint(1, 12)
        start_day = random.randint(1, 28)
        data_inicio = f"{start_year:04d}{start_month:02d}{start_day:02d}"
        
        data_sit = data_inicio if sit == "02" else f"{(start_year + random.randint(1, 4)):04d}{start_month:02d}{start_day:02d}"
        
        est_rows.append([
            cnpj_basico, "0001", "90", "1", f"{razao_social.split()[0]} STORE", sit, data_sit,
            "00" if sit == "02" else "01", "", "", data_inicio, cnae, "", "RUA", f"AVENIDA PRINCIPAL {i}",
            str(random.randint(10, 9999)), "SALA 1", "CENTRO", f"{random.randint(10000000, 99999999)}",
            uf, mun_cod, "11", "987654321", "", "", "", "", "contato@empresa.com.br", "", ""
        ])
        
        # Gerar Sócios (1 a 3 sócios por empresa)
        for s_idx in range(random.randint(1, 3)):
            soc_name = f"{random.choice(first_names)} {random.choice(first_names)} {random.choice(first_names)}"
            soc_rows.append([
                cnpj_basico, "2", soc_name, f"***{random.randint(100000, 999999)}**", "49",
                data_inicio, "", "", "", "", str(random.randint(3, 7))
            ])

    # Gravar arquivos
    with open(os.path.join(SAMPLE_DIR, "empresas", "empresas.csv"), "w", encoding="ISO-8859-1", newline="") as f:
        csv.writer(f, delimiter=";", quotechar='"').writerows(emp_rows)
    with open(os.path.join(SAMPLE_DIR, "estabelecimentos", "estabelecimentos.csv"), "w", encoding="ISO-8859-1", newline="") as f:
        csv.writer(f, delimiter=";", quotechar='"').writerows(est_rows)
    with open(os.path.join(SAMPLE_DIR, "socios", "socios.csv"), "w", encoding="ISO-8859-1", newline="") as f:
        csv.writer(f, delimiter=";", quotechar='"').writerows(soc_rows)

    print(f"✅ Gerados {len(emp_rows)} empresas, {len(est_rows)} estabelecimentos e {len(soc_rows)} sócios.")

if __name__ == "__main__":
    ensure_dirs()
    generate_cnaes()
    generate_municipios()
    generate_naturezas()
    generate_qualificacoes()
    generate_companies_and_establishments(2000)
    print("\n🎉 Amostra de dados sintéticos da Receita Federal criada com sucesso!")
