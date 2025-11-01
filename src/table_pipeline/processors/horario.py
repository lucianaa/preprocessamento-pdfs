import fitz  # PyMuPDF
import re
import pandas as pd
from typing import Dict, Optional, List, Any

# --- IMPORTAÇÃO CHAVE (MODIFICADA) ---
# Em vez de ter a função aqui, importamos da nossa "caixa de ferramentas"
# O '..' significa 'subir um nível' (de 'processors' para 'table_pipeline')
# e então acessar o 'extractor.py'
from ..extractor import get_raw_tables_from_page

# --- Funções "Privadas" de Processamento (Helpers) ---
# (Todo o seu código original, inalterado)

def _get_default_room(salas_info_text: Optional[str]) -> Optional[str]:
    """Tenta extrair a primeira sala principal mencionada em salas_info."""
    if not salas_info_text:
        return None
    
    match = re.search(r"(P\d\s*[-–—]?\s*Sala\s*\d+|Sala\s*\d+)", salas_info_text, re.IGNORECASE)
    if match:
        return ' '.join(match.group(1).split())
    else:
        parts = salas_info_text.split(',')
        first_part = parts[0].strip()
        if len(first_part) > 2 and "indicado" not in first_part.lower():
            return first_part
        else:
            return None

def _extract_horario_metadata(page: fitz.Page) -> Dict[str, Optional[str]]:
    """Extrai metadados (Semestre, Curso/Turma, Sala) do texto de uma página de horário."""
    metadata = {"semestre": None, "turma": None, "salas_info": None}
    text = page.get_text("text")
    lines = text.split('\n')

    semestre_pattern = re.compile(r"^\s*\d+/\d{4}\s*$")
    turma_pattern = re.compile(r"CIÊNCIA DA COMPUTAÇÃO\s*[-–—]\s*(.+)", re.IGNORECASE)
    salas_pattern = re.compile(r"SALAS?:\s*(.+)", re.IGNORECASE)

    lines_to_check = lines[:15]

    for line in lines_to_check:
        line = line.strip()
        if not metadata["semestre"] and semestre_pattern.match(line):
            metadata["semestre"] = line
        elif not metadata["turma"] and turma_pattern.search(line):
            metadata["turma"] = turma_pattern.search(line).group(1).strip()
        elif not metadata["salas_info"] and salas_pattern.search(line):
            metadata["salas_info"] = salas_pattern.search(line).group(1).strip()
        
        if metadata["semestre"] and metadata["turma"] and metadata["salas_info"]:
            break
    return metadata

def _parse_cell_content(cell_text: str) -> Dict[str, Optional[str]]:
    """Extrai Disciplina, Professor e Sala de uma célula da tabela de horário."""
    aula = {"disciplina": None, "professor": None, "sala": None}
    if pd.isna(cell_text) or not str(cell_text).strip():
        return aula

    text = ' '.join(str(cell_text).strip().split())
    professor = None
    sala = None
    disciplina = text

    # 1. Extrai Professor
    prof_match = re.search(r"\(([^)]+)\)", disciplina)
    if prof_match:
        professor = prof_match.group(1).strip()
        disciplina = (disciplina[:prof_match.start()] + disciplina[prof_match.end():]).strip()

    # 2. Extrai Sala
    sala_patterns_ordered = [
        r"(?i)(P\d\s*[-–—]?\s*Sala\s*\d+)",
        r"(?i)(LabCC\s*[-–—]?\s*P\d)",
        r"(?i)(LabRedes\s*[-–—]?\s*P\d)",
        r"(?i)(Sala\s*\d+)",
        r"(?i)(LabCC)",
        r"(?i)(LabRedes)"
    ]
    
    for pattern in sala_patterns_ordered:
        matches = list(re.finditer(pattern, disciplina))
        if matches:
            last_match = matches[-1]
            if last_match.end() >= len(disciplina) - 1:
                sala = ' '.join(last_match.group(1).split())
                disciplina = disciplina[:last_match.start()].strip()
                break

    # 3. Limpeza final
    disciplina = disciplina.strip(' -')
    aula["disciplina"] = disciplina if disciplina else None
    aula["professor"] = professor
    aula["sala"] = sala
    return aula

def _process_horario_df(raw_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """Limpa, estrutura e parseia um DataFrame bruto de horário."""
    if raw_df.empty:
        return None

    df = raw_df.copy()
    df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)

    # --- Identificação do Cabeçalho
    header_row_index = -1
    dias_semana_keywords = ["Segunda", "Terça", "Quarta", "Quinta", "Sexta"]
    for i, row in df.iterrows():
        row_str = ' '.join(row.dropna().astype(str))
        if any(dia in row_str for dia in dias_semana_keywords):
            header_row_index = i
            break

    if header_row_index == -1:
        print("Alerta [horario.py]: Não foi possível encontrar a linha de cabeçalho. Tentando a primeira linha.")
        if df.iloc[0].dropna().count() >= 3:
            header_row_index = 0
        else:
            print("Erro [horario.py]: Primeira linha também não parece ser cabeçalho. Abortando.")
            return None

    # --- Processamento do DataFrame
    df.columns = df.iloc[header_row_index]
    df = df.iloc[header_row_index + 1:].reset_index(drop=True)

    if df.columns[0] is pd.NA or str(df.columns[0]).strip() == "":
        df.rename(columns={df.columns[0]: "HorarioInfo"}, inplace=True)
    df.dropna(axis=1, how='all', inplace=True)

    day_columns = [col for col in df.columns if isinstance(col, str) and any(kw in col for kw in dias_semana_keywords)]
    
    df[day_columns] = df[day_columns].ffill()
    df.dropna(subset=day_columns, how='all', inplace=True)

    # --- Parseamento
    for day_col in day_columns:
        if day_col in df.columns:
            df[day_col] = df[day_col].apply(_parse_cell_content)

    # --- Limpeza Final
    def check_row_fully_parsed_empty(row):
        for day in day_columns:
            if day in row.index and isinstance(row[day], dict) and row[day].get("disciplina"):
                return False
        return True
    df = df[~df.apply(check_row_fully_parsed_empty, axis=1)]

    return df.reset_index(drop=True)


# --- Função "Pública" (Ponto de Entrada para este módulo) ---

def extract_schedule_from_page(pdf_path: str, page_num: int) -> Optional[Dict[str, Any]]:
    """
    Função principal: Orquestra a extração de metadados e tabelas de UMA página.
    Esta é a função que o 'table_runner.py' irá chamar.
    """
    print(f"--- [horario.py] Processando Página {page_num} ---")
    doc = None
    metadata = {}
    try:
        doc = fitz.open(pdf_path)
        if page_num < 1 or page_num > len(doc):
            print(f"Erro [horario.py]: Número da página {page_num} inválido.")
            return None
        page = doc.load_page(page_num - 1)
        metadata = _extract_horario_metadata(page)
        print(f"  [horario.py] Metadados encontrados: {metadata.get('turma')}")
    except Exception as e:
        print(f"Erro [horario.py] ao extrair metadados da página {page_num}: {e}")
        if doc: doc.close()
        return None
    finally:
        if doc: doc.close()

    # --- MODIFICAÇÃO ---
    # Chama a função do 'extractor.py' em vez de tê-la aqui
    raw_tables = get_raw_tables_from_page(pdf_path, page_num)

    if not raw_tables:
        print(f"  [horario.py] Nenhuma tabela encontrada por Camelot na página {page_num}.")
        return None

    if len(raw_tables) > 1:
        print(f"  Alerta [horario.py]: Múltiplas tabelas ({len(raw_tables)}) encontradas. Processando a primeira.")

    processed_df = _process_horario_df(raw_tables[0])

    if processed_df is not None and not processed_df.empty:
        # --- Limpeza dos nomes das colunas
        cleaned_columns = []
        for i, col in enumerate(processed_df.columns):
            col_str = str(col).strip() if pd.notna(col) else ""
            if not col_str:
                cleaned_columns.append(f"Coluna_Vazia_{i}")
            else:
                cleaned_columns.append(col_str)
        processed_df.columns = cleaned_columns

        horario_dict = processed_df.to_dict(orient='records')

        # --- Lógica para preencher salas vazias
        default_room = _get_default_room(metadata.get("salas_info"))
        if default_room:
            print(f"  [horario.py] Sala padrão detectada: '{default_room}'")
            for row_dict in horario_dict:
                for key, value in row_dict.items():
                    if isinstance(value, dict) and value.get("disciplina") and value.get("sala") is None:
                        value["sala"] = default_room
        
        print(f"  [horario.py] Tabela da página {page_num} processada com sucesso.")
        return {
            "pagina": page_num,
            **metadata,
            "horario": horario_dict
        }
    else:
        print(f"  [horario.py] Falha ao processar a tabela da página {page_num} após extração.")
        return None