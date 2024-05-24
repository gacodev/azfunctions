import os
import logging
import requests
from sqlalchemy import create_engine, Column, Integer, String, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError
import azure.functions as func
from time import sleep

# Database setup
DATABASE_URL = os.getenv('DATABASE_URL', 'postgresql://test:TestFunction123@localhost:5432/users')
engine = create_engine(DATABASE_URL)
Base = declarative_base()
Session = sessionmaker(bind=engine)

class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True)
    email = Column(String)
    __table_args__ = (UniqueConstraint('name', name='uq_name'),)

Base.metadata.create_all(engine)

# Database operations
def create_db_session():
    return Session()

def upsert_user(session, user):
    user['name'] = user['name'].strip()
    user['email'] = user['email'].strip()

    stmt = insert(User).values(user).on_conflict_do_update(
        index_elements=['name'],
        set_=user
    )
    session.execute(stmt)
    session.commit()

# Data fetching
def fetch_users(url, retries=3, delay=5):
    for attempt in range(retries):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as e:
            logging.error(f'Error fetching users: {e}')
            if attempt < retries - 1:
                logging.info(f'Retrying in {delay} seconds...')
                sleep(delay)
            else:
                raise

# Main logic
def process_users(users):
    try:
        with create_db_session() as session:
            for user in users:
                user_data = {
                    'name': user['name'],
                    'email': user['email']
                }
                upsert_user(session, user_data)
        logging.info('Upsert completed successfully.')
    except SQLAlchemyError as e:
        logging.error(f'Error during upsert: {e}')

is_azure_function = os.environ.get('AZURE_FUNCTIONS_ENVIRONMENT') is not None

if is_azure_function:
    def main(mytimer: func.TimerRequest) -> None:
        logging.info('Python timer trigger function executed at %s', mytimer)
        users = fetch_users('https://jsonplaceholder.typicode.com/users')
        process_users(users)
else:
    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        users = fetch_users('https://jsonplaceholder.typicode.com/users')
        process_users(users)
