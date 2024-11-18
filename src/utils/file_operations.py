# scr/utils/file_operations.py

import requests
import os
import shutil
from requests.exceptions import RequestException

def download_file_from_url(url, file_name, dbfs_path):
    """
    Baixa um arquivo CSV de uma URL e o salva no caminho especificado do DBFS.
    Limpa a pasta de destino antes de salvar o novo arquivo.
    
    :param url: URL de onde o arquivo CSV será baixado
    :param file_name: Nome final do arquivo para salvar
    :param dbfs_path: Caminho do DBFS onde o arquivo será salvo
    :return: O caminho completo do arquivo salvo
    """
    # Constrói o caminho completo para o diretório
    full_path = f'{dbfs_path}'

    # Limpa a pasta de destino se ela já existir
    if os.path.exists(full_path):
        # Remove todos os arquivos e diretórios dentro da pasta
        shutil.rmtree(full_path)

    # Cria o diretório no DBFS novamente, agora vazio
    os.makedirs(full_path, exist_ok=True)

    # Tenta realizar o download do arquivo
    try:
        response = requests.get(url)
        response.raise_for_status()  # Lança uma exceção se a resposta não for 200 OK
    except RequestException as e:
        raise Exception(f"Erro ao baixar o arquivo: {e}")

    # Constrói o caminho final do arquivo
    file_path = os.path.join(full_path, file_name)
    
    # Salva o conteúdo do arquivo no caminho especificado
    try:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return file_path
    except Exception as e:
        raise Exception(f"Erro ao salvar o arquivo: {e}")
