import os
import requests

def download_file_from_url(url, file_name, dbfs_path):
    """
    Downloads a CSV file from a URL and saves it in DBFS at a specified path.
    
    :param url: URL from which the CSV file will be downloaded
    :param file_name: Final name of the file to save
    :param dbfs_path: DBFS path where the file will be saved
    :return: Success or error message
    """
    
    # Attempt to download the file
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raises an exception if the response is not 200 OK
    except requests.exceptions.RequestException as e:
        return f"Error downloading the file: {e}"

    # Create the directory in DBFS if it does not exist
    full_path = f'/dbfs{dbfs_path}'  # Full path for DBFS
    if not os.path.exists(full_path):
        os.makedirs(full_path)  # Creates the directory if it does not exist
    
    # Construct the final file path
    file_path = os.path.join(full_path, file_name)
    
    # Save the file content to the specified path
    try:
        with open(file_path, 'wb') as file:
            file.write(response.content)
        return f"File {file_name} successfully downloaded and saved to {file_path}!"
    except Exception as e:
        return f"Error saving the file: {e}"

# Example usage
url = "https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv"
file_name = "covid19.csv"
dbfs_path = "/mnt/ntconsult/raw"  # DBFS path where the file will be saved

# Downloading the file
print(download_file_from_url(url, file_name, dbfs_path))
