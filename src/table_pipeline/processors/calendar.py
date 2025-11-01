import pandas as pd
from typing import Dict, Any, Optional, List

# Importa a "caixa de ferramentas" da nossa arquitetura
from ..extractor import get_raw_tables_from_page

# --- Funções "Privadas" (Sua lógica de 'enhance_table' refatorada) ---

def _extract_legend(raw_df: pd.DataFrame, num_legend_rows: int = 1) -> (pd.DataFrame, str):
    """
    Separa a(s) linha(s) de legenda do DataFrame principal.
    (Baseado na sua lógica original)
    """
    if raw_df.empty or len(raw_df) <= num_legend_rows:
        return raw_df, "" 

    df_tabela = raw_df.iloc[:-num_legend_rows]
    df_legenda = raw_df.iloc[-num_legend_rows:]
    
    legend_text = ' '.join(df_legenda.iloc[:, 0].dropna().astype(str).tolist()).replace('\n', ' ')
    return df_tabela, legend_text

def _clean_calendar_data(table_df: pd.DataFrame) -> (pd.DataFrame, str, Optional[str]):
    """
    Limpa o DataFrame do calendário, encontra título e cabeçalho.
    (Baseado na sua lógica original)
    
    Retorna: (DataFrame_limpo, titulo_da_tabela, status_erro)
    """
    if table_df.empty:
        return pd.DataFrame(), "", "Tabela vazia."

    table_title = table_df.iloc[0, 0].strip() if not table_df.empty else ""
    
    # Sua lógica original para encontrar o cabeçalho (dias da semana curtos)
    header_row_index = -1
    for i, row in table_df.iterrows():
        non_empty_cells = [cell for cell in row if str(cell).strip()]
        if len(non_empty_cells) > 3 and all(len(str(c).strip()) < 5 for c in non_empty_cells):
            header_row_index = i
            break
            
    if header_row_index == -1:
        return pd.DataFrame(), table_title, "Não foi possível identificar o cabeçalho (dias da semana)."

    # --- Correção para colunas duplicadas (sua lógica) ---
    header_list = table_df.iloc[header_row_index].astype(str).tolist()
    counts = {}
    unique_header = []
    for col_name in header_list:
        col_name_clean = str(col_name).strip()
        counts[col_name_clean] = counts.get(col_name_clean, 0) + 1
        if counts[col_name_clean] > 1:
            unique_header.append(f"{col_name_clean}_{counts[col_name_clean]}")
        else:
            unique_header.append(col_name_clean)
    # --- Fim da correção ---

    cleaned_df = table_df.copy()
    cleaned_df.columns = unique_header
    cleaned_df = cleaned_df.iloc[header_row_index + 1:].reset_index(drop=True)
    cleaned_df.dropna(axis=0, how='all', inplace=True)
    cleaned_df.dropna(axis=1, how='all', inplace=True)
    cleaned_df = cleaned_df.astype(str).map(lambda x: ' '.join(str(x).split()))
    
    return cleaned_df, table_title, None # None = Sem erro

def _summarize_calendar_table(cleaned_df: pd.DataFrame, table_title: str, legend_text: str) -> str:
    """
    Gera um resumo semântico específico para uma tabela de CALENDÁRIO.
    (Baseado na sua lógica original)
    """
    summary = f"A tabela '{table_title}' descreve um calendário. A legenda informa: '{legend_text}'. Detalhes do calendário: "
    rows_summaries = []
    
    if cleaned_df.empty:
        return f"A tabela '{table_title}' (legenda: '{legend_text}') está vazia."

    for index, row in cleaned_df.iterrows():
        week_summary_parts = []
        for day_name, day_number in row.items():
            day_name_clean = day_name.split('_')[0] # Remove sufixo _2
            day_number_str = str(day_number).strip()
            if day_name_clean and day_number_str and day_number_str.lower() not in ['nan', 'none', '']:
                week_summary_parts.append(f"dia {day_name_clean} é {day_number_str}")
                
        if week_summary_parts:
            rows_summaries.append(f"na semana {index + 1}, " + ", ".join(week_summary_parts))
            
    summary += "; ".join(rows_summaries) + "."
    return summary

def _process_calendar_df(raw_df: pd.DataFrame) -> Dict[str, Any]:
    """
    Orquestrador "privado" que executa a lógica de 'enhance_table'
    """
    if raw_df.empty:
        return {}

    # 1. SEPARA A LEGENDA
    table_df, legend_text = _extract_legend(raw_df, num_legend_rows=1)
    
    # 2. LIMPEZA INTELIGENTE
    cleaned_df, table_title, error_status = _clean_calendar_data(table_df)

    # 3. GERAÇÃO DO RESUMO SEMÂNTICO
    if error_status:
        summary = error_status
    else:
        summary = _summarize_calendar_table(cleaned_df, table_title, legend_text)
    
    return {
        "title": table_title,
        "cleaned_table": cleaned_df.to_dict(orient='records') if not cleaned_df.empty else [],
        "legend": legend_text,
        "summary": summary
    }

# --- Função "Pública" (Ponto de Entrada para este módulo) ---

def process_calendar_page(pdf_path: str, page_num: int) -> Optional[Dict[str, Any]]:
    """
    Função principal: Orquestra a extração de tabelas de CALENDÁRIO.
    Esta é a função que o 'table_runner.py' irá chamar.
    """
    print(f"--- [calendar.py] Processando Página {page_num} ---")
    
    # 1. Extrai tabelas da página
    raw_tables = get_raw_tables_from_page(pdf_path, page_num)
    
    if not raw_tables:
        print(f"  [calendar.py] Nenhuma tabela encontrada por Camelot na página {page_num}.")
        return None

    if len(raw_tables) > 1:
        print(f"  Alerta [calendar.py]: Múltiplas tabelas ({len(raw_tables)}) encontradas. Processando a primeira.")
    
    # 2. Processa a primeira tabela encontrada usando sua lógica
    processed_data = _process_calendar_df(raw_tables[0])

    # 3. Adiciona metadados da página ao resultado
    processed_data["pagina"] = page_num
    processed_data["identified_type"] = "calendar" # Para fácil identificação
    
    print(f"  [calendar.py] Tabela da página {page_num} processada.")
    return processed_data