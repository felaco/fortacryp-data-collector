import sqlalchemy
from sqlalchemy.engine import Connection
from sqlalchemy.ext.declarative import declarative_base, DeclarativeMeta
from sqlalchemy.orm import sessionmaker

from config import BaseConfig

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
connection: Connection = engine.connect()
session = sessionmaker(bind=engine)
Base: DeclarativeMeta = declarative_base()


# didnt manage to make this work :'(
# @contextmanager
# def session_scope():
#     """Provide a transactional scope around a series of operations."""
#     session_maker = session()
#     try:
#         yield session_maker
#         session_maker.commit()
#     except Exception as e:
#         session_maker.rollback()
#         raise e
#     finally:
#         session_maker.close()
#         i = 0
