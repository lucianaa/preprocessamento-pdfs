# table_pipeline/identifier.py
import fitz # PyMuPDF
from typing import Literal

PageType = Literal["horario", "ppc", "calendar", "history_log", "unknown"]

def identify_page_type(page: fitz.Page) -> PageType:
    """Analisa o TEXTO de uma página fitz e retorna seu tipo."""
    
    text = page.get_text("text").lower() # Converte tudo para minúsculo

    # --- Regras de Identificação (Baseadas nos seus arquivos) ---

    # 1. Regra para PPC (do ppc_parser.py)
    # Palavras-chave fortes: "projeto pedagógico", "matriz curricular", "ementa:", "bibliografia básica:"
    if (
        ("projeto pedagógico" in text) or
        ("matriz curricular" in text) or
        ("corpo docente" in text and "regime de trabalho" in text) or
        ("ementa:" in text and "bibliografia básica:" in text) or
        ("disciplinas optativas" in text)
       ):
        return "ppc"

    # 2. Regra para Horário (do horario_parser.py)
    # Palavras-chave fortes: "ciência da computação", dias da semana
    if "ciência da computação" in text and "segunda" in text and "terça" in text and "quarta" in text:
        return "horario"

    # 3. Regra para Calendário (da nossa 1ª conversa)
    if "calendário acadêmico" in text:
        return "calendar"
        
    # 4. Regra para Histórico (do seu 1º JSON)
    if "histórico de alterações do estatuto" in text and "resolução do conselho" in text:
        return "history_log"

    # Se nenhuma regra bater
    return "unknown"