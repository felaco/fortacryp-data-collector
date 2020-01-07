from config import BaseConfig
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import sqlalchemy

engine_str_mapper = {
    'postgres': 'postgresql+psycopg2://%user:%pass@%url/%dbname',
    'mysql': 'mysql+mysqlconnector://%user:%pass@%url/%dbname',
    'sqlserver': 'mssql+pyodbc://%user:%pass@%url/%dbname',
    'sqlite': 'sqlite:///%url'
}

url = engine_str_mapper[BaseConfig.DBConnection.db_provider]
url = url.replace('%user', BaseConfig.DBConnection.username or '')
url = url.replace('%pass', BaseConfig.DBConnection.password or '')
url = url.replace('%url', BaseConfig.DBConnection.location or '')
url = url.replace('%dbname', BaseConfig.DBConnection.db_name or '')

engine = sqlalchemy.create_engine(url)
engine.connect()
Session = sessionmaker(bind=engine)
Base = declarative_base()
