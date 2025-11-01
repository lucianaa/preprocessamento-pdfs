# table_pipeline/processors/ppc.py
import pandas as pd
from typing import Dict, Any, Optional, List
import re

# IMPORT CORRIGIDO: Puxa da nossa "caixa de ferramentas"
from ..extractor import get_raw_tables_from_page

# --- Funções Auxiliares de Limpeza (COM AS CORREÇÕES) ---

def _clean_string(text: Any) -> Optional[str]:
    """Limpa uma string, retornando None SÓ se a entrada for None/NaN."""
    if text is None or pd.isna(text):
        return None
        
    cleaned = str(text).replace('\n', ' ').strip()
    cleaned = ' '.join(cleaned.split()) # Remove espaços duplicados
    
    # CORREÇÃO: Se a string estiver vazia (""), retorne "", não None.
    return cleaned 

def _get_raw_table_text(raw_df: pd.DataFrame) -> str:
    """Retorna o texto bruto de um DataFrame, garantindo que seja sempre uma string."""
    if raw_df is None or raw_df.empty:
        return ""
        
    try:
        # Garante que a saída seja uma string antes de 'lower()'
        text_output = str(raw_df.to_string(header=False, index=False))
        return text_output.lower()
    except Exception as e:
        print(f"      [ppc.py] ALERTA: Falha em _get_raw_table_text: {e}")
        return "" # Retorna "" em caso de qualquer erro


# --- Parsers Específicos para cada TIPO de Tabela (CÓDIGO RESTAURADO) ---

def _parse_matriz_curricular(raw_df: pd.DataFrame) -> Optional[Dict[str, Any]]:

    df = raw_df.copy()
    
    # 1. Encontrar o Título do Período (ex: "1° PERÍODO")
    periodo_title = None
    for i, row in df.iterrows():
        cell_value = _clean_string(row.iloc[0])
        # (Adicionado 'cell_value is not None' por segurança após a correção)
        if cell_value is not None and "PERÍODO" in cell_value.upper():
            periodo_title = cell_value
            break
            
    # 2. Encontrar a Linha de Cabeçalho ("DISCIPLINA")
    header_row_index = -1
    for i, row in df.iterrows():
        cell_value_0 = _clean_string(row.iloc[0])
        if cell_value_0 is not None and "DISCIPLINA" in cell_value_0.upper():
            header_row_index = i
            break
            
    if header_row_index == -1:
        print("      [ppc.py] Alerta: _parse_matriz_curricular não encontrou 'DISCIPLINA'.")
        return None

    # 3. Definir o Início dos Dados
    data_start_index = header_row_index + 2
    
    if data_start_index >= len(df):
         print(f"      [ppc.py] Alerta: Encontrou cabeçalho mas não encontrou linhas de dados para '{periodo_title}'.")
         return None
    
    # 4. Processar o DataFrame
    data_df = df.iloc[data_start_index:].copy()
    
    expected_columns = [
        'DISCIPLINA', 'CH_Semanal_Teorica', 'CH_Semanal_Pratica',
        'CH_Semanal_Total', 'CH_Semestral_Hora_Aula', 'CH_Semestral_Horas',
        'Pre_Requisitos'
    ]
    
    num_cols = len(data_df.columns)
    if num_cols > len(expected_columns):
        data_df = data_df.iloc[:, :len(expected_columns)]
        num_cols = len(expected_columns)
        
    data_df.columns = expected_columns[:num_cols]
    
    for i in range(num_cols, len(expected_columns)):
        data_df[expected_columns[i]] = None
        
    # 5. Limpar os Dados
    data_df = data_df[data_df['DISCIPLINA'].astype(str).str.contains('TOTAL', case=False, na=False) == False]
    
    for col in data_df.columns:
        data_df[col] = data_df[col].apply(_clean_string)
        
    data_df.dropna(how='all', inplace=True)

    if data_df.empty:
        return None

    table_dict_list = data_df.where(pd.notna(data_df), None).to_dict(orient='records')
    
    return {
        "periodo": periodo_title,
        "disciplinas": table_dict_list
    }

def _parse_optativas(raw_df: pd.DataFrame) -> Optional[Dict[str, Any]]:

    if raw_df is None or raw_df.empty:
        return None

    df = raw_df.copy()

    # 1. Tentar Encontrar a Linha de Cabeçalho ("DISCIPLINA")
    header_row_index = -1
    for i, row in df.iterrows():
        cell_value = _clean_string(row.iloc[0])
        if cell_value is not None and "DISCIPLINA" in cell_value.upper():
            header_row_index = i
            break
            
    # 2. Definir o Início dos Dados
    data_start_index = 0
    if header_row_index != -1:
        data_start_index = header_row_index + 2
    else:
        print(f"      [ppc.py] Info: _parse_optativas não encontrou 'DISCIPLINA'. Tratando como página de continuação.")
            
    if data_start_index >= len(df):
         print(f"      [ppc.py] Alerta: _parse_optativas não encontrou linhas de dados após o índice {data_start_index}.")
         return None
    
    # 3. Processar o DataFrame
    data_df = df.iloc[data_start_index:].copy()
    
    expected_columns = [
        'DISCIPLINA', 'CH_Semanal_Teorica', 'CH_Semanal_Pratica',
        'CH_Semanal_Total', 'CH_Semestral_Hora_Aula', 'CH_Semestral_Horas',
        'Pre_Requisitos'
    ]
    
    num_cols = len(data_df.columns)
    if num_cols > len(expected_columns):
        data_df = data_df.iloc[:, :len(expected_columns)]
        num_cols = len(expected_columns)
        
    data_df.columns = expected_columns[:num_cols]
    
    for i in range(num_cols, len(expected_columns)):
        data_df[expected_columns[i]] = None
        
    # 4. Limpar os Dados
    data_df['DISCIPLINA'] = data_df['DISCIPLINA'].ffill()

    for col in data_df.columns:
        data_df[col] = data_df[col].apply(_clean_string)
        
    data_df.dropna(how='all', inplace=True)
    data_df.dropna(subset=['CH_Semanal_Total'], inplace=True)

    if data_df.empty:
        return None

    def aggregate_text(rows):
        return ' '.join(rows.dropna().unique())

    grouped = data_df.groupby('DISCIPLINA')
    
    aggregated_df = pd.DataFrame({
        'CH_Semanal_Teorica': grouped['CH_Semanal_Teorica'].first(),
        'CH_Semanal_Pratica': grouped['CH_Semanal_Pratica'].first(),
        'CH_Semanal_Total': grouped['CH_Semanal_Total'].first(),
        'CH_Semestral_Hora_Aula': grouped['CH_Semestral_Hora_Aula'].first(),
        'CH_Semestral_Horas': grouped['CH_Semestral_Horas'].first(),
        'Pre_Requisitos': grouped['Pre_Requisitos'].apply(aggregate_text)
    }).reset_index()

    table_dict_list = aggregated_df.where(pd.notna(aggregated_df), None).to_dict(orient='records')
    
    return {
        "disciplinas_optativas": table_dict_list
    }

def _parse_docentes(raw_df: pd.DataFrame) -> Optional[Dict[str, Any]]:

    df = raw_df.copy()
    
    header_row_index = -1
    for i, row in df.iterrows():
        row_str = ' '.join(row.dropna().astype(str)).lower()
        if "nome do professor" in row_str and "regime de trabalho" in row_str:
            header_row_index = i
            break
            
    if header_row_index != -1:
        df = df.iloc[header_row_index + 1:].reset_index(drop=True)

    expected_columns = ['Item', 'Nome do Professor', 'Formacao', 'Regime de Trabalho']
    
    num_cols = len(df.columns)
    if num_cols < 4:
        if num_cols == 3:
             print("      [ppc.py] Alerta: Tabela de docente com 3 colunas. Assumindo [Nome, Formacao, Regime].")
             df.columns = expected_columns[1:] # Usa Nome, Formacao, Regime
             df['Item'] = pd.NA # Adiciona coluna de Item vazia
        else:
             print(f"      [ppc.py] Alerta: Tabela de docente com colunas inesperadas ({num_cols}). Pulando.")
             return None
    else:
        df = df.iloc[:, :4]
        df.columns = expected_columns
    
    for col in df.columns:
        df[col] = df[col].apply(_clean_string)
        
    df.dropna(how='all', inplace=True)

    # Preenche 'Item' e 'Nome' para baixo para associar as linhas de formação
    df['Item'] = df['Item'].ffill()
    df['Nome do Professor'] = df['Nome do Professor'].ffill()
    
    df.dropna(subset=['Formacao'], inplace=True)
    
    if df.empty:
        return None

    def aggregate_formation(rows):
        full_formation = ' '.join(rows.dropna())
        return full_formation

    grouped = df.groupby('Nome do Professor')
    
    aggregated_df = pd.DataFrame({
        'Item': grouped['Item'].first(), # Pega o primeiro item
        'Formacao': grouped['Formacao'].apply(aggregate_formation),
        'Regime de Trabalho': grouped['Regime de Trabalho'].last()
    }).reset_index() 

    # Reordena colunas
    final_cols = ['Item', 'Nome do Professor', 'Formacao', 'Regime de Trabalho']
    aggregated_df = aggregated_df[[col for col in final_cols if col in aggregated_df.columns]]
    
    aggregated_df = aggregated_df.where(pd.notna(aggregated_df), None)

    return {
        "docentes": aggregated_df.to_dict(orient='records')
    }

def _parse_ementario(raw_df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Função especializada para extrair dados das tabelas de Ementário (págs 33-85).
    """
    if raw_df is None or raw_df.empty or len(raw_df.columns) < 2:
        return None
    
    ementa_dict = {}
    current_key = None
    
    # Processa a primeira linha (pode ter 3 ou 4 colunas: Disciplina, Carga, Aulas)
    first_row = raw_df.iloc[0]
    try:
        ementa_dict["disciplina"] = _clean_string(first_row.iloc[1])
        ementa_dict["carga_horaria"] = _clean_string(first_row.iloc[2])
        ementa_dict["aulas_semanais"] = _clean_string(first_row.iloc[3])
    except IndexError:
        pass # Ignora se não houver 3a/4a coluna
    
    # Processa o restante das linhas (chave-valor)
    for i in range(1, len(raw_df)):
        row = raw_df.iloc[i]
        key_cell = _clean_string(row.iloc[0])
        value_cell = _clean_string(row.iloc[1])

        # (Adicionado 'key_cell is not None' por segurança)
        if key_cell is not None and key_cell: # Se a célula da chave tem texto
            current_key = key_cell.strip().rstrip(':').lower().replace(' ', '_')
            if current_key and (value_cell is not None and value_cell):
                ementa_dict[current_key] = value_cell
            elif current_key:
                ementa_dict[current_key] = "" # Chave existe, mas valor está em outra linha
        
        # Se a chave está vazia mas o valor não, concatena com a chave anterior
        elif current_key and (value_cell is not None and value_cell):
            if current_key in ementa_dict:
                 ementa_dict[current_key] += " " + value_cell
            else:
                 ementa_dict[current_key] = value_cell # Segurança

    # Limpa os valores concatenados
    for key, value in ementa_dict.items():
        if isinstance(value, str):
            ementa_dict[key] = ' '.join(value.split())

    # Retorna o dicionário apenas se ele for válido (tiver ementa)
    return ementa_dict if "ementa" in ementa_dict else None


# --- Função PÚBLICA (com DEBUG) ---

def parse_ppc_page(pdf_path: str, page_num: int) -> Dict[str, Any]:
    """
    Função principal do parser de PPC. (Versão com DEBUG ADICIONADO)
    Extrai TODAS as tabelas e roteia CADA UMA para o parser correto.
    """
    
    # 1. Extrai TODAS as tabelas brutas da página
    raw_tables = get_raw_tables_from_page(pdf_path, page_num)
    
    parsed_data_list = [] 
    raw_table_list = []   
    table_types_found = [] 

    if not raw_tables:
        summary = "Nenhuma tabela encontrada nesta página pelo Camelot."
    else:
        summary = f"{len(raw_tables)} tabelas encontradas. Processando..."
        
        # --- DEBUG: Itera sobre as tabelas encontradas ---
        for i, raw_df in enumerate(raw_tables):
            if raw_df.empty:
                 continue
            
            print(f"      [ppc.py] Processando Tabela {i+1}/{len(raw_tables)} na página {page_num}...")
                 
            raw_table_list.append(raw_df.where(pd.notna(raw_df), None).to_dict(orient='records'))
            
            # --- CORREÇÃO DE BUG ESTÁ AQUI ---
            # (Chamamos a função _get_raw_table_text corrigida)
            table_text = _get_raw_table_text(raw_df) 
            
            parsed_data = None
            table_type = "ppc_tabela_desconhecida" # Padrão

            # --- Início do Roteamento Interno (com DEBUG) ---
            if ("disciplina:" in table_text and 
                "ementa:" in table_text and 
                "bibliografia básica:" in table_text):
                
                print(f"      [ppc.py] -> Tabela {i+1} identificada como: 'ppc_ementario'")
                table_type = "ppc_ementario"
                parsed_data = _parse_ementario(raw_df) 

            elif ("disciplina" in table_text and 
                  "ch semanal" in table_text and 
                  ("pré – requisitos" in table_text or "pré- requisitos" in table_text)):
                
                print(f"      [ppc.py] -> Tabela {i+1} identificada como: 'ppc_matriz_curricular' (ou optativa)")
                
                if "disciplinas optativas" not in table_text:
                    table_type = "ppc_matriz_curricular"
                    parsed_data = _parse_matriz_curricular(raw_df)
                else:
                    table_type = "ppc_optativas"
                    parsed_data = _parse_optativas(raw_df)
            
            elif (page_num >= 25 and page_num <= 28 and ("tópicos especiais" in table_text or "algoritmos geométricos" in table_text)):
                print(f"      [ppc.py] -> Tabela {i+1} identificada como: 'ppc_optativas' (pela regra da página)")
                table_type = "ppc_optativas"
                parsed_data = _parse_optativas(raw_df) 

            elif ("nome do professor" in table_text or 
                  ("formação" in table_text and "regime de trabalho" in table_text) or
                  (page_num in [97, 98] and "mestrado em" in table_text)):
                
                print(f"      [ppc.py] -> Tabela {i+1} identificada como: 'ppc_docentes'")
                table_type = "ppc_docentes"
                parsed_data = _parse_docentes(raw_df)
            
            elif ("componentes curriculares" in table_text and "matriz 2015" in table_text):
                 print(f"      [ppc.py] -> Tabela {i+1} identificada como: 'ppc_equivalencia' (Parser TODO)")
                 table_type = "ppc_equivalencia"
                 parsed_data = None # TODO
            # --- Fim do Roteamento Interno ---

            table_types_found.append(table_type)
            
            # --- DEBUG: Verificação do Resultado do Parser ---
            if parsed_data:
                print(f"      [ppc.py] -> SUCESSO: Parser '{table_type}' retornou dados.")
                parsed_data_list.append(parsed_data)
            else:
                if table_type == "ppc_tabela_desconhecida":
                    print(f"      [ppc.py] -> ALERTA: Tabela {i+1} não correspondeu a nenhum tipo de PPC conhecido (ementa, matriz, etc.).")
                else:
                    print(f"      [ppc.py] -> ALERTA: Detectou tipo '{table_type}' mas o parser (ex: _parse...()) falhou e retornou 'None'.")
            
            # --- Fim do loop de tabelas ---

        if table_types_found:
             summary = f"Processadas {len(raw_tables)} tabelas. Tipos detectados: {', '.join(table_types_found)}"

    return {
        "page_type": "ppc", 
        "parsed_data_list": parsed_data_list, 
        "raw_table_list": raw_table_list, 
        "summary": summary
    }