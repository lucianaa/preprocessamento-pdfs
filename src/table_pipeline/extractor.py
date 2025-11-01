import camelot
import pandas as pd
from typing import List

def get_raw_tables_from_page(pdf_path: str, page_num: int) -> List[pd.DataFrame]:
    """
    Extrai todas as tabelas brutas de uma ÚNICA página usando Camelot (lattice).
    (Esta é a função do seu horario_parser.py e ppc_parser.py)
    """
    tables_found = []
    try:
        tables = camelot.read_pdf(
            pdf_path,
            pages=str(page_num),
            flavor='lattice',
            line_scale=40 # (Bom parâmetro que você usou)
        )
        if tables:
            # Filtra tabelas vazias
            tables_found = [tbl.df for tbl in tables if not tbl.df.empty]
    except Exception as e:
        print(f"Alerta [Extractor]: Erro no Camelot ao processar pág {page_num}: {e}")
    return tables_found