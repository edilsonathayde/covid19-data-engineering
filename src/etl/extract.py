# scr/etl/extract.py

import requests
import os

def download_file_from_url(url, file_name, dbfs_path):
    """
    Baixa um arquivo CSV de uma URL e o salva no caminho especificado do DBFS.
    Limpa a pasta de destino antes de salvar o novo arquivo.

    Parâmetros:
    - url: URL de onde o arquivo CSV será baixado
    - file_name: Nome final do arquivo para salvar
    - dbfs_path: Caminho do DBFS onde o arquivo será salvo

    Retorna:
    - O caminho completo do arquivo salvo

    Lança:
    - Exception: Se houver erro ao baixar ou salvar o arquivo
    """
    
    # Realiza o download
    try:
        response = requests.get(url)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao baixar o arquivo: {e}")

    # Caminho completo no DBFS
    full_path = f'/dbfs{dbfs_path}'
    if not os.path.exists(full_path):
        os.makedirs(full_path)

    # Salva o arquivo no DBFS
    file_path = os.path.join(full_path, file_name)
    with open(file_path, 'wb') as file:
        file.write(response.content)

    return file_path
