# table_pipeline/table_runner.py
import fitz
from typing import Dict, Any, List

# Nossos módulos
from .identifier import identify_page_type
from .processors.horario import extract_schedule_from_page
from .processors.ppc import parse_ppc_page
from .processors.calendar import process_calendar_page

def run_extraction_pipeline(pdf_path: str) -> Dict[str, List[Any]]:
    """
    Ponto de entrada ÚNICO para o módulo de tabelas.
    """
    results = {
        "horarios": [],
        "ppc_data": [],
        "calendarios": [], 
        "history_logs": [],
        "generic_tables": [] 
    }
    
    print(f"Iniciando [Table Pipeline] para: {pdf_path}")
    
    try:
        doc = fitz.open(pdf_path)
    except Exception as e:
        print(f"Erro [Table Pipeline]: Não foi possível abrir o PDF {pdf_path}: {e}")
        return results

    for page_index in range(len(doc)):
        page_num = page_index + 1
        page = doc.load_page(page_index)
        
        page_type = identify_page_type(page)

        # (MUDANÇA 1: 'print' único, ANTES de pular)
        print(f"  [Table Pipeline] Página {page_num}/{len(doc)} -> Tipo: {page_type}")
        
        if page_type == "unknown":
            continue
        
        # (O 'print' duplicado foi removido daqui)

        try:
            if page_type == "horario":
                horario_data = extract_schedule_from_page(pdf_path, page_num)
                if horario_data:
                    results["horarios"].append(horario_data)
            
            # --- MUDANÇA 2: Debug detalhado do PPC ---
            elif page_type == "ppc":
                print(f"    -> Chamando processador 'ppc.py'...")
                ppc_data = parse_ppc_page(pdf_path, page_num)
                
                # Vamos verificar o que o 'ppc.py' retornou
                if ppc_data and ppc_data.get("parsed_data_list"):
                    # SUCESSO!
                    num_tabelas = len(ppc_data['parsed_data_list'])
                    print(f"    -> SUCESSO: 'ppc.py' retornou {num_tabelas} tabela(s). Adicionando aos resultados.")
                    results["ppc_data"].append(ppc_data)
                else:
                    # FALHA! (O processador rodou mas não achou tabelas)
                    print(f"    -> ALERTA: 'ppc.py' foi chamado, mas não retornou nenhuma tabela ('parsed_data_list' está vazia).")
                    
                    # Vamos imprimir o sumário do ppc.py para saber o porquê
                    if ppc_data and "summary" in ppc_data:
                         print(f"    -> Sumário do 'ppc.py': {ppc_data['summary']}")
            # --- FIM DA MUDANÇA 2 ---
            
            elif page_type == "calendar":
                cal_data = process_calendar_page(pdf_path, page_num)
                if cal_data:
                    results["calendarios"].append(cal_data)
            
            elif page_type == "history_log":
                # (Ainda para implementar)
                pass 
                
        except Exception as e:
            print(f"      ERRO [Table Pipeline] ao processar Página {page_num} (Tipo: {page_type}): {e}")

    doc.close()
    print(f"Concluído [Table Pipeline]. Resultados: { {k: len(v) for k, v in results.items() if v} }")
    return results