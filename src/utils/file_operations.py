%python
# scr/utils/file_operations.py

import os
import requests

def download_file_from_url(url, file_name, dbfs_path):
    """
    Baixa um arquivo CSV de uma URL e o salva no caminho especificado do DBFS.
    
    :param url: URL de onde o arquivo CSV será baixado
    :param file_name: Nome final do arquivo para salvar
    :param dbfs_path: Caminho do DBFS onde o arquivo será salvo
    :return: O caminho completo do arquivo salvo
    """
    # Tenta realizar o download do arquivo
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lança uma exceção se a resposta não for 200 OK
    except requests.exceptions.RequestException as e:
        raise Exception(f"Erro ao baixar o arquivo: {e}")

    # Cria o diretório no DBFS caso ele não exista
    full_path = f'/dbfs{dbfs_path}'  # Caminho completo para o DBFS
    if not os.path.exists(full_path):
        os.makedirs(full_path)  # Cria o diretório caso não exista
    
    # Constrói o caminho final do arquivo
    file_path = os.path.join(full_path, file_name)
    
    # Salva o conteúdo do arquivo no caminho especificado
    try:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return file_path
    except Exception as e:
        raise Exception(f"Erro ao salvar o arquivo: {e}")