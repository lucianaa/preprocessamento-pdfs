import re
import json
import pdfplumber
from pathlib import Path

def detect_structure(pdf_path: str, text_blocks: list[dict], metadata: dict = None) -> dict:
    """
    Detecta a estrutura de um documento PDF e retorna um JSON padrão.
    Parágrafos que não são artigos recebem título como null.
    """
    metadata = metadata or {}
    doc_id = metadata.get("doc_id", "")
    nome_doc = metadata.get("nome_doc", Path(pdf_path).stem if isinstance(pdf_path, (str, Path)) else str(pdf_path))
    versao = metadata.get("versao", "1.0")
    data_publicacao = metadata.get("data_publicacao", "")
    pagina_inicial = metadata.get("pagina_inicial", 1)
    pagina_final = metadata.get("pagina_final", len(text_blocks))

    structure = {
        "doc_id": doc_id,
        "nome_doc": nome_doc,
        "versao": versao,
        "data_publicacao": data_publicacao,
        "pagina_inicial": pagina_inicial,
        "pagina_final": pagina_final,
        "estrutura": []
    }

    current_article = None

    for block in text_blocks:
        line = block["text"].strip()
        page_num = block["page"]

        if not line:
            continue

        # Detecta artigos
        match_artigo = re.match(r"^(art\.?\s*\d+º?)(.*)", line, re.IGNORECASE)
        if match_artigo:
            artigo_titulo = match_artigo.group(1).capitalize()
            artigo_texto = match_artigo.group(2).strip()
            new_article = {"tipo": "artigo", "titulo": artigo_titulo, "paragrafos": []}
            if artigo_texto:
                new_article["paragrafos"].append({"numero": None, "texto": artigo_texto, "pagina": page_num})
            structure["estrutura"].append(new_article)
            current_article = new_article
            continue

        # Detecta parágrafos numerados ou incisos
        match_paragrafo = re.match(r"^(§\s*\d+º|parágrafo único|\b[ivxlcdm]+\s*[-–—])\s*(.*)", line, re.IGNORECASE)
        if match_paragrafo:
            paragraph_number = match_paragrafo.group(1).strip()
            paragraph_text = match_paragrafo.group(2).strip()
            if current_article:
                current_article["paragrafos"].append({"numero": paragraph_number, "texto": paragraph_text, "pagina": page_num})
            else:
                structure["estrutura"].append({
                    "tipo": "paragrafo",
                    "titulo": None,
                    "numero": paragraph_number,
                    "texto": paragraph_text,
                    "pagina": page_num
                })
            continue

        # Qualquer outro texto que não seja artigo ou parágrafo numerado
        if current_article:
            current_article["paragrafos"].append({"numero": None, "texto": line, "pagina": page_num})
        else:
            structure["estrutura"].append({
                "tipo": "paragrafo",
                "titulo": None,
                "numero": None,
                "texto": line,
                "pagina": page_num
            })

    return structure
