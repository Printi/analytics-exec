from sqlalchemy import create_engine
import os
import dotenv

class DatabaseConnector:

    def __init__(self, database):
        self.database = database
        self.host, self.username, self.password = self._load_credentials()
        
    def _load_credentials(self):
        try:
            host = os.environ['HOST_RELACIONAMENTO']
            username = os.environ['USER_RELACIONAMENTO']
            password = os.environ['PASSWD_RELACIONAMENTO']  
        except KeyError:
            dotenv.load_dotenv('.env')
            host = os.getenv('HOST_RELACIONAMENTO')
            username = os.getenv('USER_RELACIONAMENTO')
            password = os.getenv('PASSWD_RELACIONAMENTO')
        return host, username, password
    
    
    def create_engine(self):
        limite_timeout = 1800
        connection_url = f'mysql+pymysql://{self.username}:{self.password}@{self.host}:3306/{self.database}?connect_timeout={limite_timeout}&read_timeout={limite_timeout}&write_timeout={limite_timeout}'
        engine = create_engine(
            connection_url, 
            pool_size=10,         # Número máximo de conexões ativas no pool
            max_overflow=20,      # Número de conexões extras em espera
            pool_timeout=60,      # Tempo de espera para obter uma conexão
            pool_recycle=1800     # Reciclar conexões após 30 minutos
        )
        return engine

