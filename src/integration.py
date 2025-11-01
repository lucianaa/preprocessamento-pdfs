# Salve este arquivo como: integration.py
# (Ele deve ficar no mesmo nível que o 'main_pipeline.py')

import json
from typing import Dict, Any, List

def _format_table_for_final_json(table_data: Dict[str, Any], table_type: str) -> Dict[str, Any]:
    """
    Função helper para "normalizar" os diferentes formatos de saída
    (horario, ppc, calendar) para um formato de tabela consistente
    para o JSON final.
    """
    
    # Estrutura base para a tabela no JSON final
    formatted_table = {
        "tipo": "tabela",
        "tipo_tabela_identificado": table_type,
        "titulo": "Tabela",
        "cabecalho": [],
        "linhas": [],
        "resumo_semantico": None,
        "legenda": None,
        "pagina_origem": table_data.get("pagina", table_data.get("page_num"))
    }

    # --- Lógica de Normalização por Tipo ---
    
    if table_type == "calendar":
        linhas = table_data.get("cleaned_table", [])
        formatted_table["titulo"] = table_data.get("title", "Calendário Acadêmico")
        formatted_table["linhas"] = linhas
        formatted_table["resumo_semantico"] = table_data.get("summary")
        formatted_table["legenda"] = table_data.get("legend")
        if linhas:
            formatted_table["cabecalho"] = list(linhas[0].keys())

    elif table_type == "horario":
        linhas = table_data.get("horario", [])
        formatted_table["titulo"] = f"Horário: {table_data.get('turma', 'N/A')}"
        formatted_table["linhas"] = linhas
        formatted_table["resumo_semantico"] = f"Horário para o semestre {table_data.get('semestre')}. Turma: {table_data.get('turma')}. Salas: {table_data.get('salas_info')}"
        formatted_table["legenda"] = table_data.get("salas_info")
        if linhas:
            # O cabeçalho aqui são os dias da semana
            formatted_table["cabecalho"] = list(linhas[0].keys())

    elif table_type == "ppc":
        # 'table_data' aqui é um SUB-DICIONÁRIO de 'parsed_data_list'
        # (ex: {"periodo": "1° PERÍODO", "disciplinas": [...]})
        if "disciplinas" in table_data:
            linhas = table_data.get("disciplinas", [])
            titulo = f"Matriz Curricular: {table_data.get('periodo', 'Disciplinas')}"
            formatted_table["titulo"] = titulo
            formatted_table["linhas"] = linhas
            formatted_table["resumo_semantico"] = f"Lista de disciplinas para {table_data.get('periodo')}"
            if linhas:
                formatted_table["cabecalho"] = list(linhas[0].keys())
        
        elif "docentes" in table_data:
            linhas = table_data.get("docentes", [])
            formatted_table["titulo"] = "Corpo Docente"
            formatted_table["linhas"] = linhas
            formatted_table["resumo_semantico"] = "Lista de docentes, formação e regime de trabalho."
            if linhas:
                formatted_table["cabecalho"] = list(linhas[0].keys())
        
        # (Adicionar 'elif' para 'optativas', 'ementario', etc. se necessário)
        
        else:
            # Fallback para outros tipos de PPC
            formatted_table["titulo"] = "Dados do PPC"
            formatted_table["linhas"] = [table_data] # Apenas dumpar o dict

    # (Adicionar 'elif' para 'history_log' quando for implementado)
    
    return formatted_table


def integrate_table_data(main_json_data: Dict[str, Any], 
                         table_pipeline_results: Dict[str, List[Any]]) -> Dict[str, Any]:
    """
    Função principal de integração.
    
    Itera sobre a 'estrutura' do JSON principal e substitui os 
    elementos de parágrafo "fantasma" pelas tabelas reais.
    
    Esta é a sua lógica de "mock manual".
    """
    
    print("--- [Integração] Iniciando integração de tabelas no JSON principal. ---")
    
    # Cria uma cópia "consumível" das listas de tabelas extraídas
    # Usar .pop(0) garante que não inserimos a mesma tabela duas vezes
    horarios_list = table_pipeline_results.get("horarios", []).copy()
    calendarios_list = table_pipeline_results.get("calendarios", []).copy()
    ppc_pages_list = table_pipeline_results.get("ppc_data", []).copy()
    # history_logs_list = table_pipeline_results.get("history_logs", []).copy()

    # Esta será a nova lista de "estrutura"
    final_estrutura = []
    
    # Itera sobre todos os elementos do JSON principal (parágrafos, capítulos...)
    for elemento in main_json_data["estrutura"]:
        
        # Só nos importamos em substituir parágrafos
        if elemento.get("tipo") != "paragrafo":
            final_estrutura.append(elemento)
            continue
            
        # --- LÓGICA DE SUBSTITUIÇÃO (O "MOCK" MANUAL) ---
        
        texto_paragrafo = elemento.get("texto", "").lower()
        elemento_substituido = False

        try:
            # REGRA 1: Detectar "Histórico de Alterações" (Exemplo do 1º JSON)
            if "histórico de alterações" in texto_paragrafo and "resolução" in texto_paragrafo:
                # if history_logs_list:
                #     table_data = history_logs_list.pop(0)
                #     json_tabela = _format_table_for_final_json(table_data, "history_log")
                #     final_estrutura.append(json_tabela)
                #     print(f"  [Integração] SUBSTITUIÇÃO: Parágrafo 'Histórico' por Tabela.")
                #     elemento_substituido = True
                pass # (Descomentar quando o processador 'history_log' existir)

            # REGRA 2: Detectar "Calendário Acadêmico"
            elif "calendário acadêmico" in texto_paragrafo and calendarios_list:
                table_data = calendarios_list.pop(0)
                json_tabela = _format_table_for_final_json(table_data, "calendar")
                final_estrutura.append(json_tabela)
                print(f"  [Integração] SUBSTITUIÇÃO: Parágrafo 'Calendário' por Tabela (Pág. {json_tabela['pagina_origem']}).")
                elemento_substituido = True

            # REGRA 3: Detectar "Horário"
            elif "ciência da computação" in texto_paragrafo and "segunda" in texto_paragrafo and horarios_list:
                table_data = horarios_list.pop(0)
                json_tabela = _format_table_for_final_json(table_data, "horario")
                final_estrutura.append(json_tabela)
                print(f"  [Integração] SUBSTITUIÇÃO: Parágrafo 'Horário' por Tabela (Pág. {json_tabela['pagina_origem']}).")
                elemento_substituido = True

            # REGRA 4: Detectar "PPC" (Matriz, Ementa, etc.)
            elif ("matriz curricular" in texto_paragrafo or "ementa:" in texto_paragrafo or "projeto pedagógico" in texto_paragrafo) and ppc_pages_list:
                # Esta regra é especial: 1 parágrafo pode ser substituído por VÁRIAS tabelas
                
                ppc_page_data = ppc_pages_list.pop(0) # Pega a *página* inteira de PPC
                
                num_tabelas_ppc = len(ppc_page_data.get("parsed_data_list", []))
                print(f"  [Integração] SUBSTITUIÇÃO: Parágrafo 'PPC' por {num_tabelas_ppc} tabelas (Pág. {ppc_page_data.get('page_num', 'N/A')}).")
                
                # Adiciona cada tabela daquela página
                for sub_tabela_data in ppc_page_data.get("parsed_data_list", []):
                    # 'ppc' é o tipo genérico, o formatador sabe lidar
                    json_tabela = _format_table_for_final_json(sub_tabela_data, "ppc")
                    final_estrutura.append(json_tabela)
                
                elemento_substituido = True

        except Exception as e:
            print(f"  ERRO [Integração] ao tentar substituir elemento. Mantendo original. Erro: {e}")
            elemento_substituido = False # Garante que o original seja mantido

        # Se nenhuma regra bateu ou se deu erro, mantém o elemento original
        if not elemento_substituido:
            final_estrutura.append(elemento)

    # --- Fim do Loop ---

    # Verifica se sobraram tabelas que não foram integradas
    if horarios_list or calendarios_list or ppc_pages_list:
        print("  ALERTA [Integração]: Sobraram tabelas extraídas que não foram integradas.")
        # (Opcional: você pode decidir "dumpar" elas no final do JSON)
        # for table in calendarios_list:
        #    final_estrutura.append(_format_table_for_final_json(table, "calendar"))
        
    print("--- [Integração] Concluída. Retornando JSON final. ---")
    
    # Substitui a estrutura antiga pela nova, integrada
    main_json_data["estrutura"] = final_estrutura
    return main_json_data