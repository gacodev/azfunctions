import os
import logging
import requests
from sqlalchemy import create_engine, Column, Integer, String, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert
import azure.functions as func

DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://test:TestFunction123@localhost:5432/users')
engine = create_engine(DATABASE_URL)

Base = declarative_base()

class User(Base):
   __tablename__ = 'users'
   id = Column(Integer, primary_key=True)
   name = Column(String, unique=True)
   email = Column(String)
   __table_args__ = (UniqueConstraint('name', name='uq_name'),)

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)

def upsert_user(session, user):
   stmt = insert(User).values(user).on_conflict_do_update(
       index_elements=['name'],
       set_=user
   )
   session.execute(stmt)
   session.commit()

is_azure_function = os.environ.get('AZURE_FUNCTIONS_ENVIRONMENT') is not None

if is_azure_function:
   def main(mytimer: func.TimerRequest) -> None:
       logging.info('Python timer trigger function executed at %s', mytimer)

       response = requests.get('https://jsonplaceholder.typicode.com/users')
       users = response.json()

       session = Session()
       try:
           for user in users:
               user_data = {
                   'name': user['name'],
                   'email': user['email']
               }
               upsert_user(session, user_data)
           logging.info('Upsert completed successfully.')
       except Exception as e:
           logging.error('Error during upsert: %s', e)
       finally:
           session.close()

else:
   if __name__ == "__main__":
       logging.basicConfig(level=logging.INFO)

       response = requests.get('https://jsonplaceholder.typicode.com/users')
       users = response.json()

       session = Session()
       try:
           for user in users:
               user_data = {
                   'name': user['name'],
                   'email': user['email']
               }
               upsert_user(session, user_data)
           logging.info('Upsert completed successfully.')
       except Exception as e:
           logging.error('Error during upsert: %s', e)
       finally:
           session.close()