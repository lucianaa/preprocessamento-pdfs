import os
import sys
import json
from pathlib import Path

sys.path.append(str(Path(__file__).parent / 'src'))

from extract_raw import extract_raw
from normalize_text import normalize_text
from detect_structure import detect_structure
from extract_tables import extract_tables
from deduplicate import deduplicate
from enrich_metadata import enrich_metadata

def main():
    print("\n--- Pipeline de Processamento de Documentos --- ")

    # --- Configuração de Diretórios ---
    # Ajuste para o caminho correto, já que o script está em project/src/
    input_dir = Path('data/input')
    output_dir = Path('data/output')
    
    # Define o caminho para o arquivo de dicionários usando a variável 'input_dir' já criada.
    dictionaries_path = input_dir / 'dicionarios.json'

    # Cria diretórios se eles não existirem
    input_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    # --- Seleção de Arquivo ---
    pdf_files = list(input_dir.glob('*.pdf'))

    if not pdf_files:
        print(f"\nERRO: Nenhum arquivo PDF encontrado no diretório '{input_dir}'.")
        print("Por favor, coloque seus PDFs lá para que o programa possa encontrá-los.")
        return

    print("\nArquivos PDF disponíveis:")
    for i, pdf_file in enumerate(pdf_files):
        print(f"  {i+1}. {pdf_file.name}")

    while True:
        try:
            choice_str = input(f"\nSelecione o número do arquivo PDF para processar (1-{len(pdf_files)}): ")
            if not choice_str: continue
            choice = int(choice_str)
            if 1 <= choice <= len(pdf_files):
                input_pdf_path = pdf_files[choice - 1]
                break
            else:
                print("Escolha inválida. Por favor, digite um número da lista.")
        except ValueError:
            print("Entrada inválida. Por favor, digite um número.")

    print(f"\nProcessando: {input_pdf_path.name}\n")
    
    # CORREÇÃO: A variável 'base_name' é definida aqui, logo após a escolha do arquivo.
    base_name = input_pdf_path.stem

    # --- Carregamento dos Dicionários (Equipe 2) ---
    try:
        with open(dictionaries_path, 'r', encoding='utf-8') as f:
            dictionaries = json.load(f)
        acronyms = dictionaries.get("acronyms", {})
        standardization_map = dictionaries.get("standardization_map", {})
        print("-> Dicionários de normalização carregados com sucesso.")
    except FileNotFoundError:
        print(f"-> AVISO: Arquivo '{dictionaries_path}' não encontrado. A normalização será limitada.")
        acronyms = {}
        standardization_map = {}

    # --- Execução do Pipeline ---
    print("\n1. Extraindo blocos de texto com metadados (página, bbox)...")
    text_blocks = extract_raw(str(input_pdf_path))

    # Concatena o texto para a normalização (o normalized_text não é mais usado na detecção de estrutura)
    raw_text = " ".join(block["text"] for block in text_blocks)

    print("2. Normalizando texto...")
    normalized_text = normalize_text(raw_text, acronyms=acronyms, standardization_map=standardization_map)
    print(f"   Prévia: '{normalized_text[:100]}...'")

    print("3. Detectando estrutura...")
    # A função detect_structure agora recebe os blocos de texto com metadados
    structured_content = detect_structure(str(input_pdf_path), text_blocks)

    print("4. Extraindo tabelas...")
    tables_data = extract_tables(str(input_pdf_path))

    print("5. Deduplicando conteúdo...")
    deduplicated_content = deduplicate(structured_content) 

    print("6. Enriquecendo com metadados...")
    custom_metadata = {
        "nome_doc": base_name.replace('_', ' ').replace('-', ' '),
        "versao": "2023.1",
        "data_publicacao": "2023-01-01" 
    }
    final_document = enrich_metadata(deduplicated_content, str(input_pdf_path), custom_metadata)

    if tables_data:
        final_document["tables"] = tables_data

    # --- Salvando o Resultado ---
    output_filename = f"{base_name}_output.jsonl"
    output_path = output_dir / output_filename
    print(f"\nProcessamento concluído. Salvando resultados em '{output_path}'...")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(final_document, f, ensure_ascii=False, indent=4)

    print("\nPipeline finalizado com sucesso!")

if __name__ == "__main__":
    main()
