import re
import pdfplumber
from collections import Counter, defaultdict

def _reconstruct_lines_from_words(page, x_tol=3, y_tol=3):
    """
    Fallback: reconstrói linhas agrupando words por proximidade vertical (y0).
    Retorna lista de linhas já ordenadas left-to-right, top-to-bottom.
    """
    words = page.extract_words()
    if not words:
        return []

    # Agrupa por "linha" usando aproximação de y0
    lines_map = []  # lista de (y_mid, [words])
    for w in words:
        # cada word tem 'top' e 'bottom' (ou 'doctop'), usar 'top' se disponível
        y = float(w.get("top") or w.get("doctop") or 0)
        x = float(w.get("x0") or 0)
        text = w.get("text", "")

        placed = False
        for idx, (y_ref, items) in enumerate(lines_map):
            if abs(y_ref - y) <= y_tol:
                items.append((x, text))
                placed = True
                break
        if not placed:
            lines_map.append((y, [(x, text)]))

    # Ordena linhas top -> bottom (menor y -> topo) e palavras left->right
    lines_map.sort(key=lambda p: p[0])
    lines = []
    for _, items in lines_map:
        items.sort(key=lambda it: it[0])
        line = " ".join(t for _, t in items).strip()
        if line:
            lines.append(line)
    return lines

def extract_raw(pdf_path: str, header_height_ratio: float = 0.15, footer_height_ratio: float = 0.12) -> list[dict]:
    """
    Extrai texto bruto de um PDF, removendo cabeçalhos e rodapés e segmentando em blocos (parágrafos).
    Possui fallback robusto caso page.extract_text retorne None.
    """
    all_text_blocks = []
    header_candidates = []
    footer_candidates = []

    try:
        with pdfplumber.open(pdf_path) as pdf:
            n_pages = len(pdf.pages)
            # 1ª passada: coletar possíveis cabeçalhos/rodapés (até 10 páginas)
            for page in pdf.pages[:min(n_pages, 10)]:
                text = page.extract_text(x_tolerance=3, y_tolerance=3) or ""
                if not text.strip():
                    # fallback: tenta reconstruir a partir de words
                    lines_fb = _reconstruct_lines_from_words(page)
                    if not lines_fb:
                        continue
                    lines = lines_fb
                else:
                    lines = [ln.strip() for ln in text.split("\n") if ln and ln.strip()]

                if len(lines) < 1:
                    continue
                header_candidates.append(lines[0])
                footer_candidates.append(lines[-1])

            # Detecta os mais comuns (apenas se aparecerem >2 vezes)
            common_header = None
            common_footer = None
            if header_candidates:
                header_count = Counter(header_candidates).most_common(1)
                if header_count and header_count[0][1] > 2:
                    common_header = header_count[0][0]
            if footer_candidates:
                footer_count = Counter(footer_candidates).most_common(1)
                if footer_count and footer_count[0][1] > 2:
                    common_footer = footer_count[0][0]

            # 2ª passada: extração por página com crop + fallback e segmentação
            for page_num, page in enumerate(pdf.pages, 1):
                page_height = page.height
                page_width = page.width

                content_bbox = (
                    0,
                    page_height * header_height_ratio,
                    page_width,
                    page_height * (1 - footer_height_ratio)
                )
                content_page = page.crop(bbox=content_bbox)

                page_text = content_page.extract_text(x_tolerance=3, y_tolerance=3)

                # Se extract_text retornou None ou vazio, tenta reconstruir por words
                if not page_text or not page_text.strip():
                    lines = _reconstruct_lines_from_words(content_page)
                else:
                    # split seguro - garantimos page_text ser string
                    lines = [ln.strip() for ln in page_text.split("\n") if ln and ln.strip()]

                # Remove cabeçalho/rodapé detectados (comparação por prefixo)
                if common_header and lines and lines[0].startswith(common_header[:15]):
                    lines = lines[1:]
                if common_footer and lines and lines[-1].startswith(common_footer[:15]):
                    lines = lines[:-1]

                if not lines:
                    continue

                # Junta linhas em um único texto para aplicar heurística semântica depois
                page_text_clean = " ".join(lines)

                # Heurística de parágrafos (divide por sentence boundaries + conectores comuns)
                paragraph_candidates = re.split(
                    r'\.\n|(?=\b(Diante|Além disso|Assim|Portanto|Os números|Com base|Em seguida|Dessa forma|Por fim|Ciente|Dando continuidade)\b)',
                    page_text_clean
                    )

                # Filtra e adiciona blocos robustos
                for para in paragraph_candidates:
                    if not para:
                        continue
                    para = para.strip()
                    # elimina strings muito curtas (p. ex. letras soltas) — ajuste conforme necessidade
                    if len(para) < 30:
                        # se for título curto em maiúsculas, ainda pode ser útil
                        if para.isupper() and len(para) > 5:
                            pass
                        else:
                            continue
                    all_text_blocks.append({
                        "text": para,
                        "page": page_num
                    })

    except Exception as e:
        print(f"❌ Erro ao processar PDF '{pdf_path}': {e}")
        return []

    return all_text_blocks
