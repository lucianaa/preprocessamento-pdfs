import re
import hashlib
import json
from typing import List, Dict, Any, Optional
from pathlib import Path
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import unicodedata

class CrossDocumentDeduplication:
    def __init__(self):
        self.exact_similarity_threshold = 1.0    # 100% igual
        self.semantic_similarity_threshold = 0.85 # 85% similar
        self.min_text_length = 50
        
        # Cache global para deduplicação entre execuções
        self.global_content_hashes = set()
        self.processed_documents_cache = {}
        self.cache_file = Path('data/cache/deduplication_cache.json')
        self._load_cache()

    def _load_cache(self):
        """Carrega cache de documentos processados anteriormente"""
        try:
            if self.cache_file.exists():
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    cache_data = json.load(f)
                    self.global_content_hashes = set(cache_data.get('global_hashes', []))
                    self.processed_documents_cache = cache_data.get('processed_docs', {})
                print(f"Cache carregado: {len(self.global_content_hashes)} hashes globais")
        except Exception as e:
            print(f"Erro ao carregar cache: {e}")

    def _save_cache(self):
        """Salva cache para uso futuro"""
        try:
            self.cache_file.parent.mkdir(parents=True, exist_ok=True)
            cache_data = {
                'global_hashes': list(self.global_content_hashes),
                'processed_docs': self.processed_documents_cache
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Erro ao salvar cache: {e}")

    def preprocess_text_for_deduplication(self, text: str) -> str:
        """Normalização para deduplicação cruzada"""
        if not text:
            return ""
            
        # Converter para minúsculas
        text = text.lower()
        
        # Normalizar caracteres (remover acentos)
        text = unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('ASCII')
        
        # Remover pontuação e caracteres especiais
        text = re.sub(r'[^\w\s]', ' ', text)
        
        # Remover espaços extras
        text = re.sub(r'\s+', ' ', text).strip()
        
        return text

    def extract_text_from_structure(self, content: Dict) -> List[Dict]:
        """Extrai todo o texto da estrutura para comparação cruzada"""
        texts = []
        
        if not content or 'estrutura' not in content:
            return texts
            
        for item in content['estrutura']:
            # Texto do artigo
            artigo_text = f"{item.get('artigo', '')} "
            
            # Texto dos parágrafos
            for paragraph in item.get('paragrafos', []):
                full_text = artigo_text + paragraph.get('texto', '')
                if len(full_text.strip()) >= self.min_text_length:
                    texts.append({
                        'full_text': full_text.strip(),
                        'artigo': item.get('artigo'),
                        'capitulo': item.get('capitulo'),
                        'secao': item.get('secao'),
                        'paragraph_numero': paragraph.get('numero'),
                        'paragraph_text': paragraph.get('texto', ''),
                        'normalized_text': self.preprocess_text_for_deduplication(full_text)
                    })
        
        return texts

    def find_cross_document_exact_duplicates(self, current_doc: Dict, doc_name: str) -> List[Dict]:
        """Encontra duplicatas exatas comparando com documentos anteriores"""
        exact_duplicates = []
        current_texts = self.extract_text_from_structure(current_doc)
        
        for text_info in current_texts:
            content_hash = hashlib.md5(text_info['normalized_text'].encode()).hexdigest()
            
            # Verificar contra cache global
            if content_hash in self.global_content_hashes:
                exact_duplicates.append({
                    'type': 'exact_cross_document',
                    'current_document': doc_name,
                    'artigo': text_info['artigo'],
                    'paragraph_numero': text_info['paragraph_numero'],
                    'text_preview': text_info['paragraph_text'][:100] + '...',
                    'hash': content_hash
                })
            else:
                # Adicionar ao cache global
                self.global_content_hashes.add(content_hash)
        
        return exact_duplicates

    def find_cross_document_semantic_similarities(self, current_doc: Dict, doc_name: str) -> List[Dict]:
        """Encontra similaridades semânticas com documentos anteriores"""
        semantic_similarities = []
        
        # Coletar textos de documentos anteriores do cache
        previous_texts = []
        previous_info = []
        
        for prev_doc_name, prev_texts in self.processed_documents_cache.items():
            if prev_doc_name != doc_name:  # Não comparar com ele mesmo
                previous_texts.extend(prev_texts)
                previous_info.extend([{
                    'document': prev_doc_name,
                    'text': text_data['full_text']
                } for text_data in prev_texts])
        
        current_texts = self.extract_text_from_structure(current_doc)
        current_texts_list = [text['normalized_text'] for text in current_texts]
        
        if not previous_texts or not current_texts_list:
            return semantic_similarities
        
        # Combinar todos os textos para TF-IDF
        all_texts = previous_texts + current_texts_list
        
        try:
            vectorizer = TfidfVectorizer(min_df=1, max_df=0.9, stop_words=None)
            tfidf_matrix = vectorizer.fit_transform(all_texts)
            cosine_sim = cosine_similarity(tfidf_matrix)
            
            # Comparar textos atuais com textos anteriores
            num_previous = len(previous_texts)
            num_current = len(current_texts_list)
            
            for i in range(num_current):
                current_idx = num_previous + i
                for j in range(num_previous):
                    similarity = cosine_sim[current_idx][j]
                    
                    if similarity >= self.semantic_similarity_threshold:
                        semantic_similarities.append({
                            'type': 'semantic_cross_document',
                            'current_document': doc_name,
                            'previous_document': previous_info[j]['document'],
                            'current_artigo': current_texts[i]['artigo'],
                            'similarity': similarity,
                            'current_preview': current_texts[i]['paragraph_text'][:100] + '...',
                            'previous_preview': previous_info[j]['text'][:100] + '...'
                        })
                        
        except Exception as e:
            print(f"Erro na detecção de similaridades semânticas cruzadas: {e}")
        
        return semantic_similarities

    def remove_cross_document_duplicates(self, content: Dict, doc_name: str) -> Dict:
        """Remove duplicatas cruzadas e atualiza cache"""
        if not content or 'estrutura' not in content:
            return content
            
        exact_duplicates = self.find_cross_document_exact_duplicates(content, doc_name)
        
        if not exact_duplicates:
            # Ainda assim atualizar o cache
            self._update_document_cache(content, doc_name)
            self._save_cache()
            return content
            
        print(f"Removendo {len(exact_duplicates)} duplicatas cruzadas...")
        
        # Criar novo conteúdo sem duplicatas
        new_estrutura = []
        seen_hashes = set()
        
        for item in content['estrutura']:
            new_item = item.copy()
            new_item['paragrafos'] = []
            
            for paragraph in item.get('paragrafos', []):
                full_text = f"{item.get('artigo', '')} {paragraph.get('texto', '')}"
                normalized_text = self.preprocess_text_for_deduplication(full_text)
                content_hash = hashlib.md5(normalized_text.encode()).hexdigest()
                
                # Só manter se não for duplicata global
                if content_hash not in self.global_content_hashes:
                    self.global_content_hashes.add(content_hash)
                    new_item['paragrafos'].append(paragraph)
                # else: duplicata é silenciosamente removida
            
            # Só manter itens com parágrafos
            if new_item['paragrafos']:
                new_estrutura.append(new_item)
        
        content['estrutura'] = new_estrutura
        
        # Atualizar cache
        self._update_document_cache(content, doc_name)
        self._save_cache()
        
        return content

    def _update_document_cache(self, content: Dict, doc_name: str):
        """Atualiza cache com textos do documento atual"""
        current_texts = self.extract_text_from_structure(content)
        self.processed_documents_cache[doc_name] = current_texts

    def get_deduplication_report(self, doc_name: str) -> Dict:
        """Gera relatório de deduplicação para o documento atual"""
        # Esta função pode ser usada para obter estatísticas
        return {
            'document': doc_name,
            'global_hashes_count': len(self.global_content_hashes),
            'processed_documents_count': len(self.processed_documents_cache),
            'cache_file': str(self.cache_file)
        }

# Função principal que se encaixa no seu pipeline
def deduplicate(structured_content: Dict) -> Dict:
    """
    Função de deduplicação cruzada que se encaixa no pipeline existente
    SEM alterar a assinatura da função!
    """
    print("   Aplicando deduplicação cruzada...")
    
    # Obter nome do documento do conteúdo (se disponível)
    doc_name = structured_content.get('nome_doc', 'documento_atual')
    doc_id = structured_content.get('doc_id', 'unknown')
    
    deduplicator = CrossDocumentDeduplication()
    
    # Aplicar deduplicação cruzada
    clean_content = deduplicator.remove_cross_document_duplicates(structured_content, doc_name)
    
    # Encontrar duplicatas e similaridades (apenas para logging)
    exact_duplicates = deduplicator.find_cross_document_exact_duplicates(structured_content, doc_name)
    semantic_similarities = deduplicator.find_cross_document_semantic_similarities(structured_content, doc_name)
    
    # Log das estatísticas
    if exact_duplicates:
        print(f"{len(exact_duplicates)} duplicatas exatas cruzadas detectadas")
    
    if semantic_similarities:
        print(f"{len(semantic_similarities)} similaridades semânticas cruzadas detectadas")
    
    
    
    print(f"Deduplicação cruzada concluída")
    
    return clean_content


def clear_deduplication_cache():
    """Limpa o cache de deduplicação"""
    cache_file = Path('data/cache/deduplication_cache.json')
    if cache_file.exists():
        cache_file.unlink()
        print("Cache de deduplicação limpo")
    else:
        print("ℹNenhum cache encontrado para limpar")
