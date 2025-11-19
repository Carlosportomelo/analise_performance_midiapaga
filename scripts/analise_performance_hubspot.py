#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
analise_performance_hubspot_CORRIGIDO_FIX.py

✅ CORREÇÕES CRÍTICAS:
    - Preservação do Status Original do negócio (sem limpar texto)
    - Mapeamento correto de matrículas usando o status original
    - Data de fechamento corretamente atribuída
    - Mantém todas as funcionalidades anteriores (IDs, investimento, etc)

💡 CORREÇÃO DE INVESTIMENTO:
    - Garante que as chaves de merge (Campanha/Termo) sejam normalizadas
      usando 'clean_text' em todos os DataFrames.
    - Altera o nome das abas de investimento de YoY para 'Completo' (granular).
    - ✅ CORREÇÃO FINAL: Ajusta a lista de keywords para encontrar as colunas de
      Campanha/Termo (e Investimento) nas bases Meta/Google.
"""

import pandas as pd
import numpy as np
import unicodedata
import re
import sys
import hashlib
from pathlib import Path
from datetime import datetime

print("="*80)
print("🚀 Iniciando Script de BLEND - VERSÃO CORRIGIDA FINAL")
print("="*80)

# --- 1. CONFIGURAÇÕES ---

# Define o BASE_DIR como o diretório raiz do projeto
try:
    BASE_DIR = Path(__file__).resolve().parent.parent
except NameError:
    BASE_DIR = Path.cwd()

# Ajustando caminhos para o ambiente de execução
DATA_DIR_HUBSPOT = BASE_DIR / "data" 
DATA_DIR_INVESTIMENTO = BASE_DIR / "outputs" 
OUTPUT_DIR = BASE_DIR / "output" 

OUTPUT_DIR.mkdir(exist_ok=True)

# Arquivos de entrada
HUBSPOT_FILE = DATA_DIR_HUBSPOT / "hubspot_dataset.csv" 
META_REPORT_FILE = DATA_DIR_INVESTIMENTO / "meta_dataset_dashboard.xlsx"
# 💡 CORREÇÃO CRÍTICA (NOME DA ABA): Mudando de YoY para Completo (granular)
META_SHEET_NAME = "Meta_Completo" 
GOOGLE_REPORT_FILE = DATA_DIR_INVESTIMENTO / "google_dashboard.xlsx"
# 💡 CORREÇÃO CRÍTICA (NOME DA ABA): Mudando de YoY para Completo (granular)
GOOGLE_SHEET_NAME = "Google_Completo" 

# Arquivo de saída
BLEND_BASE_NAME = "dataset_geral_melhorado"

# Configurações de Nome de Conta
META_ACCOUNT_OTHER_LABEL = "Red Balloon - Contas Meta"
GOOGLE_ACCOUNT_LABEL = "Google Ads"
AREA_GESTAO_DEFAULT = "Gestão Antiga" 

# Regras de Negócio
CANAL_FILTRO = [
    "social pago", 
    "pesquisa paga",
    "paid social",
    "facebook",
    "instagram",
    "linkedin",
    "cpc"
]
MATRICULA_KEYWORDS = ["MATRÍCULA CONCLUÍDA"] 
DEFAULT_NA_TEXT = "Não Mapeado"

# Mapeamento dos Nomes de Canal Finais
CANAL_MAP_FINAL = {
    'social pago': 'Social Pago',
    'facebook': 'Social Pago',
    'instagram': 'Social Pago',
    'linkedin': 'Social Pago',
    'paid social': 'Social Pago',
    
    'pesquisa paga': 'Pesquisa Paga',
    'cpc': 'Pesquisa Paga'
}

# Mapeamento usando o STATUS ORIGINAL do HubSpot
ETAPA_FUNIL_MAP = {
    "NOVO NEGÓCIO": "1. Novo Negócio",
    "NEGÓCIO EM QUALIFICAÇÃO": "2. Negócio em Qualificação",
    "VISITA AGENDADA": "3. Visita Agendada",
    "VISITA REALIZADA": "4. Visita Realizada",
    "LISTA DE ESPERA": "5. Lista de Espera",
    "NEGÓCIO EM PAUSA": "6. Negócio em Pausa",
    "NEGÓCIO PERDIDO": "7. Negócio Perdido",
    "MATRÍCULA CONCLUÍDA": "8. Matrícula Realizada"
}
MATRICULA_NOME_FINAL = "8. Matrícula Realizada"


# --- 2. FUNÇÕES UTILITÁRIAS ---

def read_any(path: Path, sheet_name=0, skiprows=0) -> pd.DataFrame:
    """Lê um arquivo CSV ou Excel com tratamento de erros."""
    
    print(f"    📂 Buscando arquivo: {path.resolve()}")
    if not path.exists():
        print(f"\n❌ ERRO FATAL: Arquivo '{path.name}' não encontrado.")
        print(f"    Caminho buscado: {path.resolve()}")
        sys.exit(1)
    
    suf = path.suffix.lower()
    try:
        if suf in (".xls", ".xlsx"):
            print(f"    📊 Lendo aba: {sheet_name}")
            return pd.read_excel(path, sheet_name=sheet_name, skiprows=skiprows)
        elif suf == ".csv" or not suf:
            encodings = ['utf-8', 'latin1', 'iso-8859-1', 'cp1252']
            for encoding in encodings:
                try:
                    # Tenta ler o CSV, usando sep=None para detecção automática
                    df = pd.read_csv(path, engine="python", sep=None, encoding=encoding, skiprows=skiprows)
                    print(f"    ✅ Arquivo lido com encoding: {encoding}")
                    return df
                except UnicodeDecodeError:
                    continue
            
            raise Exception("Não foi possível decodificar o arquivo com os encodings testados")
        else:
            print(f"❌ ERRO FINAL: Extensão de arquivo '{suf}' não suportada.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ ERRO FINAL: Não foi possível ler o arquivo {path.name}.")
        print(f"    Detalhe: {e}")
        sys.exit(1)

def clean_cols(df):
    """Limpa e normaliza os nomes das colunas de um DataFrame."""
    cols = []
    for c in df.columns:
        c_str = str(c).strip().lower()
        c_norm = unicodedata.normalize('NFKD', c_str)
        c_ascii = c_norm.encode('ascii', 'ignore').decode('utf-8')
        c_clean = re.sub(r'[^a-z0-9_ ]+', '', c_ascii)
        c_clean = re.sub(r'\s+', '_', c_clean)
        cols.append(c_clean)
    df.columns = cols
    return df

def clean_text(text):
    """Limpa e normaliza uma string de dados (remove acentos, espaços, caracteres especiais)."""
    try:
        text_str = str(text).strip().lower()
        text_norm = unicodedata.normalize('NFKD', text_str)
        text_ascii = text_norm.encode('ascii', 'ignore').decode('utf-8')
        # Limpar caracteres não alfanuméricos exceto espaços
        text_clean = re.sub(r'[^a-z0-9 ]+', '', text_ascii) 
        # Remover múltiplos espaços e espaços nas extremidades
        text_clean = re.sub(r'\s+', ' ', text_clean).strip() 
        return text_clean
    except Exception:
        return ""

def extract_status_base(status_full):
    """
    Extrai o status base do formato "STATUS (Pipeline)"
    Ex: "NOVO NEGÓCIO (Red Balloon - Unidades de Rua)" -> "NOVO NEGÓCIO"
    """
    try:
        if pd.isna(status_full):
            return DEFAULT_NA_TEXT
        
        # Remove tudo após o primeiro parêntese
        status_base = str(status_full).split('(')[0].strip().upper()
        return status_base if status_base else DEFAULT_NA_TEXT
    except:
        return DEFAULT_NA_TEXT

def find_col(df: pd.DataFrame, keywords: list, use_clean_cols=False) -> str:
    """Encontra a primeira coluna no DataFrame que corresponde a uma keyword."""
    target_cols = df.columns
    
    # 1. Busca por correspondência exata
    for k in keywords:
        if k in target_cols:
            return k
            
    # 2. Busca ignorando maiúsculas/minúsculas
    cols_map = {str(c).strip().lower(): str(c) for c in target_cols}
    for k in keywords:
        k_low = k.strip().lower()
        if k_low in cols_map:
            return cols_map[k_low]
            
    # 3. Heurística de substring
    for col_orig in target_cols:
        col_low = str(col_orig).strip().lower()
        for k in keywords:
            if k.strip().lower() in col_low:
                return str(col_orig)
    
    return None

def calcular_ciclo_captacao(date_series: pd.Series) -> pd.Series:
    """
    Calcula o ciclo de captação (YY.1 Alta ou YY.2 Baixa).
    """
    
    def get_ciclo(dt):
        if pd.isna(dt):
            return DEFAULT_NA_TEXT
        
        try:
            mes = dt.month
            ano = dt.year
            
            # Outubro a Dezembro = Alta do próximo ano
            if mes >= 10:
                ano_ciclo = ano + 1
                return f"{str(ano_ciclo)[2:]}.1 Alta"
            # Janeiro a Março = Alta do mesmo ano
            elif mes <= 3:
                return f"{str(ano)[2:]}.1 Alta"
            # Abril a Setembro = Baixa do mesmo ano
            else:
                return f"{str(ano)[2:]}.2 Baixa"
        except:
            return DEFAULT_NA_TEXT
    
    return date_series.apply(get_ciclo)

def generate_unique_id(df: pd.DataFrame) -> pd.DataFrame:
    """Gera IDs únicos e consistentes para cada negócio."""
    print("    🔑 Gerando IDs únicos e consistentes...")
    
    # 1. Criar a chave de agrupamento
    df['Chave_ID'] = df['Data'].dt.strftime('%Y%m%d') + '_' + \
                     df['Unidade'].astype(str) + '_' + \
                     df['Origem_Principal'].astype(str)
    
    # 2. Gerar a sequência de desempate
    df = df.sort_values(by=['Chave_ID', 'RVO'], ascending=[True, False])
    df['Sequencia_Desempate'] = df.groupby('Chave_ID').cumcount() + 1
    
    # 3. Gerar o ID Longo (ID_Negocio_Completo)
    def create_long_id(row):
        data_str = row['Data'].strftime('%Y%m%d')
        unidade_str = re.sub(r'[^A-Z]', '', unicodedata.normalize('NFKD', row['Unidade'].upper()).encode('ascii', 'ignore').decode('utf-8'))[:10].ljust(10, 'X')
        origem_str = re.sub(r'[^A-Z]', '', unicodedata.normalize('NFKD', row['Origem_Principal'].upper()).encode('ascii', 'ignore').decode('utf-8'))[:5].ljust(5, 'X')
        seq_str = str(row['Sequencia_Desempate']).zfill(3)
        
        stable_key = f"{row['Chave_ID']}_{seq_str}"
        hash_val = hashlib.sha1(stable_key.encode()).hexdigest()[:4].upper()
        
        return f"{data_str}_{unidade_str}_{origem_str}_{seq_str}_{hash_val}"

    df['ID_Negocio_Completo'] = df.apply(create_long_id, axis=1)
    
    # 4. Gerar o ID Curto (Lead_Key)
    def create_short_id(long_id):
        parts = long_id.split('_')
        data_part = parts[0][-2:]
        hash_part = parts[-1]
        seq_part = parts[-2]
        
        return f"{data_part}{hash_part}{seq_part}"

    df['Lead_Key'] = df['ID_Negocio_Completo'].apply(create_short_id)
    
    df = df.drop(columns=['Chave_ID', 'Sequencia_Desempate'])
    print("    ✅ IDs gerados com sucesso.")
    return df


# --- 3. LÓGICA PRINCIPAL ---

def main():
    
    print("\n📥 Carregando dados do HubSpot...")
    df_hub_raw = read_any(HUBSPOT_FILE)
    df_hub = clean_cols(df_hub_raw)
    print(f"    ✅ HubSpot carregado: {len(df_hub)} linhas")
    
    # --- 3.1. Preparar campos do HubSpot ---
    
    print("\n🔄 Preparando campos do HubSpot...")
    
    # Data de Criação
    col_data = find_col(df_hub, ['data', 'data_de_criacao', 'createdate', 'create_date'])
    if not col_data:
        print("❌ ERRO: Coluna de data de criação não encontrada.")
        sys.exit(1)
    df_hub['Data'] = pd.to_datetime(df_hub[col_data], errors='coerce').dt.normalize()
    
    # Data de Fechamento
    col_data_fechamento = find_col(df_hub, ['data_de_fechamento', 'closedate', 'close_date'])
    if col_data_fechamento:
        df_hub['Data_Fechamento'] = pd.to_datetime(df_hub[col_data_fechamento], errors='coerce').dt.normalize()
        print(f"    ✅ Coluna 'Data_Fechamento' criada: {df_hub['Data_Fechamento'].notna().sum()} registros com data")
    else:
        df_hub['Data_Fechamento'] = pd.NaT
        print("    ⚠️  Coluna de data de fechamento não encontrada - usando NaT")
    
    # Unidade
    col_unidade = find_col(df_hub, ['unidade_desejada', 'unidade'])
    df_hub['Unidade'] = df_hub[col_unidade].fillna(DEFAULT_NA_TEXT) if col_unidade else DEFAULT_NA_TEXT
    
    # Tipo (Pipeline)
    col_tipo = find_col(df_hub, ['pipeline', 'tipo'])
    df_hub['Tipo'] = df_hub[col_tipo].fillna(DEFAULT_NA_TEXT) if col_tipo else DEFAULT_NA_TEXT
    
    # Status Principal
    col_status = find_col(df_hub, ['etapa_do_negocio', 'dealstage', 'deal_stage', 'status'])
    if not col_status:
        print("❌ ERRO: Coluna de status/etapa não encontrada.")
        sys.exit(1)
    
    # Preservar o status original antes de qualquer processamento
    df_hub['Status_Original'] = df_hub[col_status].fillna(DEFAULT_NA_TEXT)
    
    # Extrair o status base (sem o pipeline entre parênteses)
    df_hub['Status_Base'] = df_hub['Status_Original'].apply(extract_status_base)
    
    # Mapear para o formato final usando o status base
    df_hub['Status_Principal'] = df_hub['Status_Base'].map(ETAPA_FUNIL_MAP).fillna(DEFAULT_NA_TEXT)
    
    print(f"    ✅ Status mapeados:")
    print(df_hub['Status_Principal'].value_counts())
    
    # RVO
    col_rvo = find_col(df_hub, ['valor_na_moeda_da_empresa', 'rvo', 'amount'])
    df_hub['RVO'] = pd.to_numeric(df_hub[col_rvo], errors='coerce').fillna(0) if col_rvo else 0
    
    # Fonte de Tráfego
    col_fonte = find_col(df_hub, ['fonte_original_do_trafego', 'original_source'])
    df_hub['Fonte_Original_do_Trafego'] = df_hub[col_fonte].fillna(DEFAULT_NA_TEXT) if col_fonte else DEFAULT_NA_TEXT
    df_hub['Fonte_Original_do_Trafego_clean'] = df_hub['Fonte_Original_do_Trafego'].apply(clean_text)
    
    # Detalhamentos
    col_det1 = find_col(df_hub, ['detalhamento_da_fonte_original_do_trafego_1', 'detalhamento_fonte_original_1', 'hs_analytics_source_data_1'])
    df_hub['Detalhamento_fonte_original_1'] = df_hub[col_det1].fillna(DEFAULT_NA_TEXT) if col_det1 else DEFAULT_NA_TEXT
    # 💡 CORREÇÃO CRÍTICA: Limpeza para o merge de investimento do Meta (1)
    df_hub['Merge_Key_Meta'] = df_hub['Detalhamento_fonte_original_1'].apply(clean_text)

    col_det2 = find_col(df_hub, ['detalhamento_da_fonte_original_do_trafego_2', 'detalhamento_fonte_original_2', 'hs_analytics_source_data_2'])
    df_hub['Detalhamento_fonte_original_2'] = df_hub[col_det2].fillna(DEFAULT_NA_TEXT) if col_det2 else DEFAULT_NA_TEXT
    # 💡 CORREÇÃO CRÍTICA: Limpeza para o merge de investimento do Google (2)
    df_hub['Merge_Key_Google'] = df_hub['Detalhamento_fonte_original_2'].apply(clean_text)
    
    # --- 3.2. Mapeamento de Canais e Filtro ---
    
    print("\n    🗺️  Mapeando canais e aplicando filtros...")
    
    # Mapeamento de Origem Principal
    df_hub['Origem_Principal'] = df_hub['Fonte_Original_do_Trafego_clean'].map(CANAL_MAP_FINAL).fillna(DEFAULT_NA_TEXT)
    
    # Filtro de Canais (apenas canais de mídia paga)
    df_hub_filtrado = df_hub[df_hub['Origem_Principal'].isin(CANAL_MAP_FINAL.values())].copy()
    
    print(f"    ✅ Filtro aplicado: {len(df_hub_filtrado)} registros de mídia paga")
    
    # Mapeamento de Nome_Conta_Final
    def map_nome_conta(origem):
        if origem == 'Social Pago':
            return META_ACCOUNT_OTHER_LABEL
        elif origem == 'Pesquisa Paga':
            return GOOGLE_ACCOUNT_LABEL
        else:
            return DEFAULT_NA_TEXT
    
    df_hub_filtrado['Nome_Conta_Final'] = df_hub_filtrado['Origem_Principal'].apply(map_nome_conta)
    df_hub_filtrado['Area_Gestao_RVO'] = AREA_GESTAO_DEFAULT
    
    # --- 3.3. Calcular Métricas de Negócios ---
    
    print("\n🔄 Calculando métricas de negócios...")
    
    # Ciclo de Captação (baseado na Data de Criação)
    df_hub_filtrado['Ciclo_Captacao'] = calcular_ciclo_captacao(df_hub_filtrado['Data'])
    
    # Ciclo de Captação de Fechamento (baseado na Data de Fechamento)
    df_hub_filtrado['Ciclo_Captacao_Fechamento'] = calcular_ciclo_captacao(df_hub_filtrado['Data_Fechamento'])
    
    # Total de Negócios
    df_hub_filtrado['Total_Negocios'] = 1
    
    # Matrículas usando o Status_Principal mapeado
    df_hub_filtrado['Matriculas'] = np.where(
        df_hub_filtrado['Status_Principal'] == MATRICULA_NOME_FINAL,
        1, 0
    )
    
    print(f"    ✅ Total de matrículas identificadas: {df_hub_filtrado['Matriculas'].sum()}")
    print(f"    ✅ Matrículas com Data de Fechamento: {df_hub_filtrado[df_hub_filtrado['Matriculas']==1]['Data_Fechamento'].notna().sum()}")
    
    # Colunas de Matrícula por Ciclo (usando Ciclo_Captacao - data de criação)
    ciclos_unicos = sorted(df_hub_filtrado['Ciclo_Captacao'].unique())
    ciclos_unicos = [c for c in ciclos_unicos if c != DEFAULT_NA_TEXT]
    
    for ciclo in ciclos_unicos:
        col_name = f"Matriculas_{ciclo.replace('.', '_').replace(' ', '_')}"
        df_hub_filtrado[col_name] = np.where(
            (df_hub_filtrado['Matriculas'] == 1) & (df_hub_filtrado['Ciclo_Captacao'] == ciclo),
            1, 0
        )
    
    # --- 3.4. Carregar e Preparar Dados de Investimento ---
    
    print("\n📥 Carregando dados de investimento...")
    
    # Meta Ads
    if META_REPORT_FILE.exists():
        df_meta_raw = read_any(META_REPORT_FILE, sheet_name=META_SHEET_NAME)
        df_meta = clean_cols(df_meta_raw)
        
        col_data_meta = find_col(df_meta, ['data', 'date', 'day'])
        # ✅ CORREÇÃO FINAL: Adicionar mais sinônimos para investimento, incluindo variações de nomes de colunas que contenham "valor"
        col_inv_meta = find_col(df_meta, ['investimento', 'spend', 'amount_spent', 'valor_usado_brl', 'valor_usado', 'valor'])
        # ✅ CORREÇÃO FINAL: Adicionar mais sinônimos para campanha, incluindo 'nome_da_campanha' (normalizado de 'Nome da Campanha') e 'campanha' (mais simples)
        col_campanha_meta = find_col(df_meta, ['campanha', 'campaign', 'campaign_name', 'nome_da_campanha', 'nome_campanha'])
        
        if col_data_meta and col_inv_meta and col_campanha_meta:
            print(f"    ✅ Colunas Meta encontradas: Data='{col_data_meta}', Investimento='{col_inv_meta}', Campanha='{col_campanha_meta}'")
            df_meta['Data'] = pd.to_datetime(df_meta[col_data_meta], errors='coerce').dt.normalize()
            df_meta['Investimento_Meta'] = pd.to_numeric(df_meta[col_inv_meta], errors='coerce').fillna(0)
            
            df_meta['Campanha'] = df_meta[col_campanha_meta].fillna(DEFAULT_NA_TEXT)
            # 💡 CORREÇÃO CRÍTICA: Limpeza da chave de merge do Meta
            df_meta['Campanha_Merge_Key'] = df_meta['Campanha'].apply(clean_text)
            
            df_meta_agg = df_meta.groupby(['Data', 'Campanha_Merge_Key'], dropna=False)['Investimento_Meta'].sum().reset_index()
            print(f"    ✅ Meta Ads carregado: {len(df_meta_agg)} linhas agregadas")
        else:
            print(f"    ❌ ERRO FATAL: Uma ou mais colunas de investimento Meta não foram encontradas no sheet '{META_SHEET_NAME}'.")
            print("    -> Verifique se as colunas 'Data', 'Investimento' e 'Campanha' estão na base.")
            df_meta_agg = pd.DataFrame(columns=['Data', 'Campanha_Merge_Key', 'Investimento_Meta'])
            # Se colunas cruciais não forem encontradas, podemos parar o script ou retornar vazio.
            # Decidindo manter o fluxo para tentar processar o Google Ads, mas o merge resultará em 0.
    else:
        print("    ⚠️  Arquivo Meta Ads não encontrado")
        df_meta_agg = pd.DataFrame(columns=['Data', 'Campanha_Merge_Key', 'Investimento_Meta'])
    
    # Google Ads
    if GOOGLE_REPORT_FILE.exists():
        df_google_raw = read_any(GOOGLE_REPORT_FILE, sheet_name=GOOGLE_SHEET_NAME)
        df_google = clean_cols(df_google_raw)
        
        col_data_google = find_col(df_google, ['data', 'date', 'day'])
        # ✅ CORREÇÃO FINAL: Adicionar mais sinônimos para investimento
        col_inv_google = find_col(df_google, ['investimento', 'cost', 'spend', 'investimento_google', 'valor'])
        # ✅ CORREÇÃO FINAL: Focar em 'Nome_Campanha' (Termo/Keyword pode não existir)
        col_termo_google = find_col(df_google, ['nome_campanha', 'campanha', 'campaign', 'keyword', 'search_term', 'termo'])
        
        if col_data_google and col_inv_google and col_termo_google:
            print(f"    ✅ Colunas Google encontradas: Data='{col_data_google}', Investimento='{col_inv_google}', Campanha/Termo='{col_termo_google}'")
            df_google['Data'] = pd.to_datetime(df_google[col_data_google], errors='coerce').dt.normalize()
            df_google['Investimento_Google'] = pd.to_numeric(df_google[col_inv_google], errors='coerce').fillna(0)
            
            df_google['Termo'] = df_google[col_termo_google].fillna(DEFAULT_NA_TEXT)
            # 💡 CORREÇÃO CRÍTICA: Limpeza da chave de merge do Google
            df_google['Termo_Merge_Key'] = df_google['Termo'].apply(clean_text)
            
            df_google_agg = df_google.groupby(['Data', 'Termo_Merge_Key'], dropna=False)['Investimento_Google'].sum().reset_index()
            print(f"    ✅ Google Ads carregado: {len(df_google_agg)} linhas agregadas")
        else:
            print(f"    ❌ ERRO FATAL: Uma ou mais colunas de investimento Google não foram encontradas no sheet '{GOOGLE_SHEET_NAME}'.")
            print("    -> Verifique se as colunas 'Data', 'Investimento' e 'Campanha' estão na base.")
            df_google_agg = pd.DataFrame(columns=['Data', 'Termo_Merge_Key', 'Investimento_Google'])
    else:
        print("    ⚠️  Arquivo Google Ads não encontrado")
        df_google_agg = pd.DataFrame(columns=['Data', 'Termo_Merge_Key', 'Investimento_Google'])
    
    # --- 3.5. Merge e Prorrateio de Investimento ---
    
    print("\n🔗 Realizando merge e prorrateio de investimento...")
    
    # Merge com Meta Ads
    df_merged = df_hub_filtrado.merge(
        df_meta_agg,
        left_on=['Data', 'Merge_Key_Meta'], # Usando a coluna limpa do HubSpot
        right_on=['Data', 'Campanha_Merge_Key'], # Usando a coluna limpa do Meta
        how='left'
    )
    df_merged['Investimento_Meta'] = df_merged['Investimento_Meta'].fillna(0)
    
    # Merge com Google Ads
    df_merged = df_merged.merge(
        df_google_agg,
        left_on=['Data', 'Merge_Key_Google'], # Usando a coluna limpa do HubSpot
        right_on=['Data', 'Termo_Merge_Key'], # Usando a coluna limpa do Google
        how='left'
    )
    df_merged['Investimento_Google'] = df_merged['Investimento_Google'].fillna(0)
    
    # Calcular investimento total por dia e origem
    df_merged['Investimento_Total_Dia'] = np.where(
        df_merged['Origem_Principal'] == 'Social Pago',
        df_merged['Investimento_Meta'],
        df_merged['Investimento_Google']
    )
    
    # Prorrateio: contar leads por (Data, Origem_Principal)
    leads_por_dia = df_merged.groupby(['Data', 'Origem_Principal']).size().reset_index(name='Count_Leads')
    df_merged = df_merged.drop(columns=['Count_Leads'], errors='ignore')
    df_merged = df_merged.merge(leads_por_dia, on=['Data', 'Origem_Principal'], how='left')
    
    # Calcular investimento prorrateado por lead
    df_merged['Midia_Paga'] = np.where(
        df_merged['Count_Leads'] > 0,
        df_merged['Investimento_Total_Dia'] / df_merged['Count_Leads'],
        0
    )
    
    print(f"    ✅ Investimento prorrateado calculado")
    print(f"    💰 Investimento total: R$ {df_merged['Midia_Paga'].sum():,.2f}")
    
    # --- 3.6. Gerar IDs e Preparar DataFrame Granular ---
    
    print("\n🔄 Gerando IDs únicos e preparando visão granular...")
    
    df_granular = generate_unique_id(df_merged)
    
    # Limpar colunas auxiliares
    cols_to_drop = [
        'Fonte_Original_do_Trafego_clean', 'Investimento_Meta', 'Investimento_Google',
        'Investimento_Total_Dia', 'Count_Leads', 'Campanha', 'Termo',
        'Status_Original', 'Status_Base',
        'Merge_Key_Meta', 'Merge_Key_Google', # Colunas de merge limpas do HubSpot
        'Campanha_Merge_Key', 'Termo_Merge_Key' # Colunas de merge limpas do Meta/Google
    ]
    df_granular = df_granular.drop(columns=[c for c in cols_to_drop if c in df_granular.columns])
    
    # Selecionar e reordenar colunas para a Visao_Granular_Final
    cols_granular = [
        'ID_Negocio_Completo', 'Lead_Key', 'Data', 'Data_Fechamento', 'Ciclo_Captacao', 'Ciclo_Captacao_Fechamento',
        'Unidade', 'Tipo', 'Total_Negocios', 'Midia_Paga', 'RVO', 'Matriculas', 
        'Status_Principal', 'Origem_Principal', 'Detalhamento_fonte_original_1', 
        'Detalhamento_fonte_original_2', 'Fonte_Original_do_Trafego', 'Nome_Conta_Final', 
        'Area_Gestao_RVO'
    ] + [c for c in df_granular.columns if c.startswith('Matriculas_') and c not in ['Matriculas']]
    
    df_granular = df_granular[cols_granular]
    
    print(f"    ✅ Visão granular final preparada com {len(df_granular)} linhas")
    
    # --- 3.7. Preparar DataFrame Agregado (Blend_Agregado_Dash) ---
    
    print("\n🔄 Preparando visão agregada para o Dashboard...")
    
    # Agrupar e somar
    agg_dict = {
        'Total_Negocios': 'sum',
        'Matriculas': 'sum',
        'Midia_Paga': 'sum',
        'RVO': 'sum'
    }
    
    # Adicionar agregação para cada coluna de matrícula por ciclo
    for col in [c for c in df_granular.columns if c.startswith('Matriculas_') and c not in ['Matriculas']]:
        agg_dict[col] = 'sum'
        
    df_agregado = df_granular.groupby([
        'Data', 'Origem_Principal', 'Detalhamento_fonte_original_1', 
        'Detalhamento_fonte_original_2', 'Status_Principal', 'Tipo', 'Unidade'
    ], dropna=False).agg(agg_dict).reset_index()
    
    # Renomear colunas
    df_agregado = df_agregado.rename(columns={
        'Origem_Principal': 'Canal',
        'Detalhamento_fonte_original_1': 'Campanha',
        'Detalhamento_fonte_original_2': 'Termo',
        'Status_Principal': 'Etapas_de_Negocios',
        'Tipo': 'Pipeline',
        'Unidade': 'Unidade_Desejada',
        'Total_Negocios': 'Volume_Total_Negocios',
        'Midia_Paga': 'Investimento',
        'RVO': 'RVO_Total'
    })
    
    # Selecionar e reordenar colunas
    cols_agregado = [
        'Data', 'Canal', 'Campanha', 'Termo', 'Etapas_de_Negocios', 'Pipeline', 
        'Unidade_Desejada', 'Volume_Total_Negocios', 'Matriculas', 'Investimento', 'RVO_Total'
    ] + [c for c in df_agregado.columns if c.startswith('Matriculas_')]
    
    df_agregado = df_agregado[cols_agregado]
    
    print(f"    ✅ Visão agregada preparada com {len(df_agregado)} linhas")
    
    # --- 3.8. Preparar DataFrame Agregado de Matrículas (Agregado_Matriculas_Fechamento) ---
    
    print("\n🔄 Preparando visão agregada de matrículas por data de fechamento...")
    
    # Filtrar apenas matrículas concluídas
    df_matriculas = df_granular[df_granular['Matriculas'] == 1].copy()
    
    if len(df_matriculas) == 0:
        print("    ⚠️  Nenhuma matrícula encontrada para gerar a aba Agregado_Matriculas_Fechamento")
        df_matriculas_fechamento = pd.DataFrame()
    else:
        # Colunas de Matrícula por Ciclo (baseado na Data de Fechamento)
        matriculas_ciclo_fechamento_cols = sorted(df_matriculas['Ciclo_Captacao_Fechamento'].unique())
        matriculas_ciclo_fechamento_cols = [c for c in matriculas_ciclo_fechamento_cols if c != DEFAULT_NA_TEXT]
        
        # Recalcular as colunas de matrícula por ciclo, usando Ciclo_Captacao_Fechamento
        for ciclo in matriculas_ciclo_fechamento_cols:
            col_name = f"Matriculas_{ciclo.replace('.', '_').replace(' ', '_')}"
            df_matriculas[col_name] = np.where(
                (df_matriculas['Matriculas'] == 1) & (df_matriculas['Ciclo_Captacao_Fechamento'] == ciclo),
                1, 0
            )
        
        # Adicionar as colunas de ciclo baseadas na Data de Criação (para manter a estrutura)
        for col in [c for c in df_granular.columns if c.startswith('Matriculas_') and c not in df_matriculas.columns]:
            df_matriculas[col] = 0
        
        # Agrupar por Data_Fechamento e Ciclo_Captacao_Fechamento
        agg_dict_mat = {
            'Total_Negocios': 'sum',
            'Matriculas': 'sum',
            'Midia_Paga': 'sum',
            'RVO': 'sum'
        }
        
        # Adicionar agregação para cada coluna de matrícula por ciclo
        for col in [c for c in df_matriculas.columns if c.startswith('Matriculas_')]:
            agg_dict_mat[col] = 'sum'
        
        df_matriculas_fechamento = df_matriculas.groupby([
            'Data_Fechamento', 'Ciclo_Captacao_Fechamento', 'Origem_Principal', 
            'Detalhamento_fonte_original_1', 'Detalhamento_fonte_original_2', 
            'Tipo', 'Unidade'
        ], dropna=False).agg(agg_dict_mat).reset_index()
        
        # Renomear colunas
        df_matriculas_fechamento = df_matriculas_fechamento.rename(columns={
            'Origem_Principal': 'Canal',
            'Detalhamento_fonte_original_1': 'Campanha',
            'Detalhamento_fonte_original_2': 'Termo',
            'Tipo': 'Pipeline',
            'Unidade': 'Unidade_Desejada',
            'Total_Negocios': 'Volume_Matriculas',
            'Midia_Paga': 'Investimento',
            'RVO': 'RVO_Total',
            'Ciclo_Captacao_Fechamento': 'Ciclo_Captacao'
        })
        
        # Selecionar e reordenar colunas
        cols_matriculas = [
            'Data_Fechamento', 'Ciclo_Captacao', 'Canal', 'Campanha', 'Termo', 
            'Pipeline', 'Unidade_Desejada', 'Volume_Matriculas', 'Matriculas', 
            'Investimento', 'RVO_Total'
        ] + [c for c in df_matriculas_fechamento.columns if c.startswith('Matriculas_')]
        
        df_matriculas_fechamento = df_matriculas_fechamento[cols_matriculas]
        
        # Ordenar por Data_Fechamento
        df_matriculas_fechamento = df_matriculas_fechamento.sort_values('Data_Fechamento')
        
        print(f"    ✅ Visão de matrículas preparada com {len(df_matriculas_fechamento)} linhas")
        print(f"    ✅ Total de matrículas na aba: {df_matriculas_fechamento['Matriculas'].sum()}")
    
    # --- 3.9. Salvar Arquivo Final ---
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUT_FILE = OUTPUT_DIR / f"{BLEND_BASE_NAME}_{timestamp}.xlsx"
    
    print(f"\n💾 Salvando blend final em: {OUT_FILE.resolve()}")
    
    try:
        with pd.ExcelWriter(OUT_FILE, engine='openpyxl') as writer:
            # Visao_Granular_Final
            df_granular.to_excel(writer, sheet_name='Visao_Granular_Final', index=False)
            print("    ✅ Aba 'Visao_Granular_Final' salva")
            
            # Blend_Agregado_Dash
            df_agregado.to_excel(writer, sheet_name='Blend_Agregado_Dash', index=False)
            print("    ✅ Aba 'Blend_Agregado_Dash' salva")
            
            # Agregado_Matriculas_Fechamento
            if len(df_matriculas_fechamento) > 0:
                df_matriculas_fechamento.to_excel(writer, sheet_name='Agregado_Matriculas_Fechamento', index=False)
                print("    ✅ Aba 'Agregado_Matriculas_Fechamento' salva")
            else:
                print("    ⚠️  Aba 'Agregado_Matriculas_Fechamento' não gerada (sem matrículas)")
        
        print(f"\n✅ Processo de blend concluído com sucesso!")
        print(f"    Arquivo gerado: {OUT_FILE.resolve()}")
        print(f"\n📊 RESUMO:")
        print(f"    - Total de negócios: {df_granular['Total_Negocios'].sum()}")
        print(f"    - Total de matrículas: {df_granular['Matriculas'].sum()}")
        print(f"    - Investimento total: R$ {df_granular['Midia_Paga'].sum():,.2f}")
        print(f"    - RVO total: R$ {df_granular['RVO'].sum():,.2f}")
        
    except ImportError:
        print("\n\n❌ ERRO: A BIBLIOTECA 'openpyxl' NÃO ESTÁ INSTALADA.")
        print("Para salvar em Excel, rode: pip install openpyxl")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ ERRO AO SALVAR O EXCEL: {e}")
        print("Verifique se o arquivo não está aberto em outro programa.")
        sys.exit(1)

if __name__ == "__main__":
    main()