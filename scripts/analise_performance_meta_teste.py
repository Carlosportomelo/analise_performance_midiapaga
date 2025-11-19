import pandas as pd
import sys
from pathlib import Path
import re
import numpy as np

# --- Constantes ---
FILE_PATH = Path("data/meta_dataset.csv")
SHEET_NAME = None # Use None para CSV. Mude para 'Meta_Completo' se for Excel.

# --- Caminhos de Saída ---
OUTPUT_DIR = Path("outputs")
OUT_EXCEL_FILE = OUTPUT_DIR / "meta_dataset_dashboard.xlsx"

# --- Função Utilitária para Números ---
def parse_number(x):
    if pd.isna(x):
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    s = s.replace(" ", "")
    if "." in s and "," in s:
        # Formato brasileiro (1.234,56) -> 1234.56
        s = s.replace(".", "").replace(",", ".")
    else:
        # Tenta tratar (1,234.56) ou (1234,56)
        s = s.replace(",", ".")
    try:
        return float(s)
    except:
        return 0.0

# --- 1. Carregar Dados ---
print(f"📊 Carregando dados de: {FILE_PATH}")
try:
    if FILE_PATH.suffix.lower() in ['.xlsx', '.xls']:
        df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME or 0)
    elif FILE_PATH.suffix.lower() == '.csv':
        # Tenta ler com engine python (mais flexível) e detectar separador
        try:
            df = pd.read_csv(FILE_PATH, engine='python', sep=None)
        except Exception as e_csv:
            print(f"Aviso: Falha ao ler CSV com engine='python' ({e_csv}). Tentando engine padrão.")
            df = pd.read_csv(FILE_PATH) # Tenta engine padrão
    else:
        raise ValueError(f"Formato de arquivo não suportado: {FILE_PATH.suffix}")
    
    if df.empty:
        raise ValueError("O DataFrame está vazio após o carregamento.")
    print(f"✅ {len(df)} linhas carregadas")

except FileNotFoundError:
    print(f"❌ ERRO: Arquivo não encontrado em: {FILE_PATH.resolve()}")
    print("   Verifique se o caminho e o nome do arquivo estão corretos.")
    sys.exit(1)
except Exception as e:
    print(f"❌ ERRO ao carregar o arquivo: {e}")
    sys.exit(1)

# =====================================================================
# --- 2. Normalizar Colunas ---
# =====================================================================

print("\n🔧 Processando dados...")
# remover espaços, normalizar caixa
df.columns = df.columns.map(lambda c: str(c).strip() if not pd.isna(c) else c)
# Remover aspas duplas (") que apareceram no seu log de erro
df.columns = df.columns.str.strip('"')
# Remover caracteres especiais (BOM, etc.)
df.columns = [re.sub(r'[^\x00-\x7F]+', '', col) for col in df.columns]
df.columns = df.columns.str.replace('\\r', '', regex=False).str.replace('\\n', '', regex=False)

print("\n📋 Colunas detectadas (normalizadas):")
print(list(df.columns))

# --- 3. Encontrar a Coluna de Data (Lógica flexível) ---

possiveis_nomes = ['Dia', 'dia', 'Data', 'data', 'Date', 'date', 'Data_Datetime', 'DataFormatada']
col_data = None
for nome in possiveis_nomes:
    if nome in df.columns:
        col_data = nome
        print(f"✔️ Coluna de data encontrada: '{col_data}'")
        break

if col_data is None:
    print("⚠️ Coluna de data não encontrada por nome. Tentando heurística...")
    for col in df.columns:
        if df[col].dtype == object:
            sample = df[col].dropna().astype(str).head(20).tolist()
            if not sample: 
                continue
            n_like = sum(1 for v in sample if ('/' in v or '-' in v or v.count('/')>=1 or v.count('-')>=1 or (v.isdigit() and len(v) >= 4)))
            if n_like >= max(3, len(sample)//3):
                col_data = col
                print(f"⚠️ Possível coluna de data detectada por heurística: '{col_data}'")
                break

if col_data is None:
    print("❌ Não foi possível localizar automaticamente uma coluna de data (esperada 'Dia' ou 'Data').")
    print("   Imprimindo amostra para inspeção manual:")
    print(df.head(10).to_string(index=False))
    raise KeyError("Coluna de data 'Dia' ou 'Data' não encontrada. Verifique cabeçalho do arquivo.")

# --- 3b. Encontrar a Coluna de Investimento ---
possiveis_nomes_invest = ['Valor usado (BRL)', 'Valor', 'Investimento', 'spent', 'gasto']
col_invest = None
for nome in possiveis_nomes_invest:
    if nome in df.columns:
        col_invest = nome
        print(f"✔️ Coluna de investimento encontrada: '{col_invest}'")
        break
        
if col_invest is None:
    print("❌ Não foi possível localizar automaticamente uma coluna de investimento.")
    raise KeyError("Coluna de investimento (ex: 'Valor usado (BRL)') não encontrada.")

# --- 4. Processar o Resto do Script ---
print(f"\n🔧 Usando coluna de data: '{col_data}' -> convertendo para datetime")

# Tentar converter a data, sendo flexível com o formato
try:
    df['Data_Datetime'] = pd.to_datetime(df[col_data], errors='coerce')
except Exception:
    print(f"Aviso: Falha na conversão de data. Tentando formato padrão.")
    df['Data_Datetime'] = pd.to_datetime(df[col_data], errors='coerce')

num_na_dates = df['Data_Datetime'].isna().sum()
if num_na_dates > 0:
    print(f"⚠️ Atenção: {num_na_dates} linhas não puderam ser convertidas para data e serão ignoradas.")
    df = df.dropna(subset=['Data_Datetime'])

if df.empty:
    print("❌ ERRO: Nenhuma linha restou após a limpeza das datas. Verifique o formato da data no arquivo.")
    sys.exit(1)

print(f"\n🔧 Usando coluna de investimento: '{col_invest}' -> convertendo para número")
df[col_invest] = df[col_invest].apply(parse_number)

print("\n🔧 Processando dados... (Ex: Ano, Mês, etc.)")
df['Ano'] = df['Data_Datetime'].dt.year
df['Mes'] = df['Data_Datetime'].dt.month
df['Mes_Ano'] = df['Data_Datetime'].dt.to_period('M')

# =====================================================================
# --- FILTRO: EXCLUIR "BILINGUAL" ---
# =====================================================================
print("\n🔧 Aplicando filtro: Excluindo registros com 'bilingual'...")
linhas_antes = len(df)

# Criar máscara para identificar linhas com "bilingual" em qualquer coluna de texto
mask_bilingual = pd.Series([False] * len(df), index=df.index)

for col in df.columns:
    if df[col].dtype == 'object':  # Apenas colunas de texto
        try:
            mask_bilingual |= df[col].astype(str).str.contains(
                'bilingual', 
                case=False, 
                na=False, 
                regex=False
            )
        except Exception as e:
            print(f"  ⚠️ Aviso: Erro ao processar coluna '{col}': {e}")
            continue

# Aplicar filtro (manter apenas linhas SEM "bilingual")
df = df[~mask_bilingual].copy()

linhas_removidas = linhas_antes - len(df)
print(f"  ✅ Filtro aplicado: {linhas_removidas} linhas removidas (contendo 'bilingual')")
print(f"  ✅ Linhas restantes: {len(df)}")

if df.empty:
    print("❌ ERRO: Nenhuma linha restou após a exclusão de 'bilingual'. Verifique os dados.")
    sys.exit(1)

print("\n✅ Processamento básico concluído com sucesso!")

# =====================================================================
# --- 5. GERAR RELATÓRIOS ---
# =====================================================================
print("\nGerando relatórios...")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Relatório 1: Meta_YoY (Agregado por Dia) ---
print(f"  Processando dados para a aba 'Meta_YoY'...")
df_daily_agg = df.groupby('Data_Datetime').agg(
    Investimento=(col_invest, 'sum')
).reset_index()

# Renomear colunas para bater com o anexo
df_daily_agg = df_daily_agg.rename(columns={'Data_Datetime': 'Data'})

# Filtrar dias sem investimento para não poluir o arquivo e ordenar
df_daily_agg = df_daily_agg[df_daily_agg['Investimento'] > 0].sort_values(by='Data')


# --- Relatório 2: Abas por Ano (2023, 2024, 2025) ---
print("  Processando dados para as abas por ano...")

df_2023 = df[df['Ano'] == 2023]
df_2024 = df[df['Ano'] == 2024]
df_2025 = df[df['Ano'] == 2025]

# Salvar tudo em um único arquivo Excel com abas
print(f"\n💾 Salvando arquivo Excel único em: {OUT_EXCEL_FILE}")
try:
    with pd.ExcelWriter(OUT_EXCEL_FILE, engine='openpyxl') as writer:
        # Aba 1: Meta_YoY
        df_daily_agg.to_excel(writer, sheet_name='Meta_YoY', index=False, float_format='%.2f')
        print("  ✅ Aba 'Meta_YoY' salva.")
        
        # Aba 2: Meta_Completo (O dataframe 'df' original processado)
        df.to_excel(writer, sheet_name='Meta_Completo', index=False)
        print("  ✅ Aba 'Meta_Completo' salva.")

        # Abas por Ano
        if not df_2023.empty:
            df_2023.to_excel(writer, sheet_name='Meta_2023', index=False)
            print("  ✅ Aba 'Meta_2023' salva.")
        if not df_2024.empty:
            df_2024.to_excel(writer, sheet_name='Meta_2024', index=False)
            print("  ✅ Aba 'Meta_2024' salva.")
        if not df_2025.empty:
            df_2025.to_excel(writer, sheet_name='Meta_2025', index=False)
            print("  ✅ Aba 'Meta_2025' salva.")
    
    print(f"\n✅ Arquivo Excel '{OUT_EXCEL_FILE.name}' gerado com sucesso na pasta '{OUTPUT_DIR}'!")

except ImportError:
    print("\n\n❌ ERRO: A BIBLIOTECA 'openpyxl' NÃO ESTÁ INSTALADA.")
    print("Para salvar em Excel, por favor, rode o comando no seu terminal:")
    print("pip install openpyxl")
    sys.exit(1)
except Exception as e:
    print(f"\n\n❌ ERRO AO SALVAR O EXCEL: {e}")
    print("Verifique se o arquivo não está aberto em outro programa.")
    sys.exit(1)


# =====================================================================
# --- 6. CONFIRMAÇÃO DE DADOS ---
# =====================================================================
print("\n--- Confirmação de Investimento (2025) ---")

try:
    # Assegurar que 'Data' é datetime (já deve ser, mas para garantir)
    df_daily_agg['Data'] = pd.to_datetime(df_daily_agg['Data'])
    
    # Calcular Setembro 2025
    invest_set_2025 = df_daily_agg[
        (df_daily_agg['Data'].dt.year == 2025) &
        (df_daily_agg['Data'].dt.month == 9)
    ]['Investimento'].sum()
    
    # Calcular Outubro 2025
    invest_out_2025 = df_daily_agg[
        (df_daily_agg['Data'].dt.year == 2025) &
        (df_daily_agg['Data'].dt.month == 10)
    ]['Investimento'].sum()
    
    # Formatar como moeda brasileira
    print(f"  ✅ Investimento Total em Setembro/2025: R$ {invest_set_2025:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
    print(f"  ✅ Investimento Total em Outubro/2025:   R$ {invest_out_2025:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

except Exception as e_conf:
    print(f"  ⚠️ Não foi possível calcular a confirmação de investimento: {e_conf}")


print("\n--- Amostra do Relatório YoY (Aba 'Meta_YoY') ---")
print(df_daily_agg.head())  