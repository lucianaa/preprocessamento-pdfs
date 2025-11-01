import camelot
import pandas as pd

def extract_tables(pdf_path: str) -> list[list[list[str]]]:
    """
    Extrai tabelas de um arquivo PDF e as retorna em um formato estruturado.
    Tenta extrair usando os dois 'flavors' do Camelot (lattice e stream) para maximizar a precisão.

    Args:
        pdf_path (str): O caminho para o arquivo PDF.

    Returns:
        list[list[list[str]]]: Uma lista de tabelas, onde cada tabela é uma lista de linhas,
                                e cada linha é uma lista de strings (células).
    """
    all_tables_data = []

    try:
        tables_lattice = camelot.read_pdf(pdf_path, pages='all', flavor='lattice', suppress_stdout=True)
        for table in tables_lattice:
            df = table.df
            all_tables_data.append(df.values.tolist())
    except Exception as e:
        print(f"Aviso: Erro ao extrair tabelas com flavor='lattice': {e}")

    try:
        tables_stream = camelot.read_pdf(pdf_path, pages='all', flavor='stream', suppress_stdout=True)
        for table in tables_stream:
            df = table.df

            if df.values.tolist() not in all_tables_data:
                all_tables_data.append(df.values.tolist())
    except Exception as e:
        print(f"Aviso: Erro ao extrair tabelas com flavor='stream': {e}")

    cleaned_tables = []
    for table in all_tables_data:
        if len(table) > 1 and any(cell.strip() for row in table for cell in row):
            cleaned_tables.append(table)

    return cleaned_tables