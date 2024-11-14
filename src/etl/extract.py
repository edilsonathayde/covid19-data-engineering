# scr/etl/extract.py

import requests
import os

def download_file_from_url(url, file_name, dbfs_path):
    """
    Baixa um arquivo da URL e salva no DBFS.
    :param url: URL para o download do arquivo
    :param file_name: Nome do arquivo a ser salvo
    :param dbfs_path: Caminho do DBFS para salvar o arquivo
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