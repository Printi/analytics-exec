import io
import os
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
from src.utils.func import print_log
from dotenv import load_dotenv
from pathlib import Path

load_dotenv()
CREDENTIALS_FILE_PATH = os.getenv('CREDENTIALS_GOOGLE')
AUTHORIZED_USER_FILE_PATH = os.getenv('AU_GOOGLE')
DATA_PATH=os.getenv('DATA_PATH')

# Verifica se os caminhos foram carregados corretamente
if not CREDENTIALS_FILE_PATH:
    raise ValueError("A variável de ambiente 'CREDENTIALS_GOOGLE' não está definida.")
if not AUTHORIZED_USER_FILE_PATH:
    raise ValueError("A variável de ambiente 'AU_GOOGLE' não está definida.")

# Se você modificar os escopos, o arquivo authorized_user.json (ou token.json) precisará ser excluído
# para que um novo seja gerado com as permissões corretas.
# Para apenas ler arquivos, 'https://www.googleapis.com/auth/drive.readonly' é suficiente.
SCOPES = ["https://www.googleapis.com/auth/drive"]

def get_drive_service():
    """
    Autentica o usuário e retorna o objeto de serviço da API do Google Drive.
    Usa 'AUTHORIZED_USER_FILE_PATH' para credenciais existentes e 'CREDENTIALS_FILE_PATH' para a primeira autorização.
    """
    creds = None
    # Tenta carregar as credenciais existentes do arquivo autorizado.
    if os.path.exists(AUTHORIZED_USER_FILE_PATH):
        creds = Credentials.from_authorized_user_file(AUTHORIZED_USER_FILE_PATH, SCOPES)
    
    # Se as credenciais não forem válidas ou não existirem, inicie o fluxo de autorização.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            # Se as credenciais expiraram mas há um token de refresh, tente renová-las.
            creds.refresh(Request())
        else:
            # Se não houver credenciais ou token de refresh, inicie o fluxo de instalação.
            # O arquivo 'credentials.json' (caminho via CREDENTIALS_FILE_PATH) é necessário para esta etapa.
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE_PATH, SCOPES)
            creds = flow.run_local_server(port=0)
        
        # Salva as credenciais (tokens) atualizadas no arquivo authorized_user.json para uso futuro.
        with open(AUTHORIZED_USER_FILE_PATH, 'w') as token:
            token.write(creds.to_json())
    
    try:
        # Constrói e retorna o serviço da API do Drive.
        service = build('drive', 'v3', credentials=creds)
        return service
    except HttpError as error:
        print(f'Ocorreu um erro ao conectar ao Google Drive: {error}')
        return None

def download_csv_file(service, file_id, file_name):
    """Baixa um arquivo CSV específico do Google Drive."""
    try:
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print(f"Baixando {int(status.progress() * 100)}%...")
        
        with open(Path(DATA_PATH+file_name), 'wb') as f:
            f.write(fh.getvalue())
        print_log(f'Arquivo "{file_name}" baixado com sucesso!', level='terminal_process')

    except HttpError as error:
        print_log(f'Ocorreu um erro ao baixar o arquivo: {error}', level='terminal_process')

def list_csv_files_in_folder(service, folder_id, is_shared_drive=False, shared_drive_id=None):
    """
    Lista os arquivos CSV em uma pasta específica no Google Drive.
    is_shared_drive: Booleano. Defina como True se a busca for dentro de uma Shared Drive.
    shared_drive_id: O ID da Shared Drive se is_shared_drive for True.
    """
    try:
        query = f"mimeType='text/csv' and '{folder_id}' in parents"
        
        # Parâmetros padrão para pesquisa
        list_params = {
            'q': query,
            'pageSize': 10,
            'fields': "nextPageToken, files(id, name)"
        }

        # Adiciona parâmetros para Shared Drives, se aplicável
        if is_shared_drive:
            if not shared_drive_id:
                raise ValueError("Se is_shared_drive é True, shared_drive_id deve ser fornecido.")
            list_params['corpora'] = 'drive'
            list_params['driveId'] = shared_drive_id # Este é o ID da Shared Drive, não da pasta!
            list_params['includeItemsFromAllDrives'] = True
            list_params['supportsAllDrives'] = True
        else:
            # Para "Meu Drive" ou pastas compartilhadas dentro do "Meu Drive" de alguém
            list_params['corpora'] = 'user'


        results = service.files().list(**list_params).execute()
        
        items = results.get('files', [])
        
        if not items:
            print(f'Nenhum arquivo CSV encontrado na pasta com ID: {folder_id}.')
            return []
        else:
            print(f'Arquivos CSV encontrados na pasta (ID: {folder_id}):')
            for item in items:
                print(f'- {item["name"]} (ID: {item["id"]})')
            return items
    except HttpError as error:
        print(f'Ocorreu um erro ao listar arquivos na pasta: {error}')
        return []

def connect_list_csv_and_download():
    drive_service = get_drive_service()
    
    if drive_service:
        # --- NOVO FLUXO DE PERGUNTAS AO USUÁRIO ---
        drive_type = 'compartilhada'
        
        if drive_type == 'compartilhada':
            shared_drive_id = os.getenv("ID_SHARED_DRIVE_GROWTH")
            folder_to_search = os.getenv("ID_FOLDER_DATA_SCIENCE_HUB_DATA")
            
            if shared_drive_id and folder_to_search:
                csv_files = list_csv_files_in_folder(
                    drive_service, 
                    folder_to_search, 
                    is_shared_drive=True, 
                    shared_drive_id=shared_drive_id
                )
            else:
                print_log("IDs da Unidade Compartilhada ou da pasta não foram fornecidos.", level='terminal_process')
                csv_files = [] # Garante que csv_files esteja definida
        
        elif drive_type == 'meu_drive':
            folder_to_search = input("Digite o ID da pasta no 'Meu Drive': ")
            if folder_to_search:
                csv_files = list_csv_files_in_folder(drive_service, folder_to_search, is_shared_drive=False)
            else:
                print_log("ID da pasta não foi fornecido.", level='terminal_process')
                csv_files = []
        else:
            print_log("Tipo de drive inválido. Por favor, digite 'meu_drive' ou 'compartilhada'.", level='terminal_process')
            csv_files = []

        # ... (Resto do bloco if csv_files: para baixar o arquivo) ...
        if csv_files:
            for i, file in enumerate(csv_files):
                download_csv_file(drive_service, file['id'], file['name'])
        elif folder_to_search and csv_files == []: # Garante que a mensagem seja mostrada apenas se um ID de pasta foi fornecido
            print_log("<DRIVE> Nenhum arquivo CSV encontrado na pasta especificada.", level='terminal_process')

if __name__ == '__main__':
    connect_list_csv_and_download()
           