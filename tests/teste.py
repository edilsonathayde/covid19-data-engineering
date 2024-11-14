import os
import requests

def baixa_arquivos_url(url, file_name, dbfs_path):
    """
    Baixa o arquivo CSV de uma URL e salva no DBFS, em um caminho especificado.
    
    :param url: URL de onde o arquivo CSV será baixado
    :param file_name: Nome final do arquivo para salvar
    :param dbfs_path: Caminho no DBFS onde o arquivo será salvo
    """
    
    # Realiza o download do arquivo a partir da URL
    response = requests.get(url)
    response.raise_for_status()  # Verifica se houve algum erro no download
    
    # Construção do caminho completo para o DBFS
    full_path = f'/dbfs{dbfs_path}'
    
    # Certifica-se de que o diretório existe
    if not os.path.exists(full_path):
        os.makedirs(full_path)  # Cria o diretório, caso não exista
    
    # Salva o conteúdo no arquivo especificado
    file_path = f'{full_path}/{file_name}'
    with open(file_path, 'wb') as file:
        file.write(response.content)
    
    return f"Arquivo {file_name} baixado e salvo com sucesso em {file_path}!"

# Exemplo de como utilizar
url = "https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv"
file_name = "covid19_aggregated.csv"
dbfs_path = "/mnt/data_engineering_covid.ntconsult/raw"

# Baixando o arquivo
print(baixa_arquivos_url(url, file_name, dbfs_path))
