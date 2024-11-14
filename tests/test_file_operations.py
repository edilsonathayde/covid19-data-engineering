import os
from src.utils.file_operations import save_raw_data

def test_save_raw_data():
    content = b"Sample data for testing"
    file_path = "./data_engineering_covid.ntconsult/raw/test_data.csv"
    save_raw_data(content, file_path)
    assert os.path.exists(file_path)
    os.remove(file_path)  # Limpar após o teste
