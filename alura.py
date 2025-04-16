import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

def connect_banco_dados() -> psycopg2.connect:
    connection = psycopg2.connect(
        user=os.getenv("USER"),
        password=os.getenv("PASSWORD"),
        database= os.getenv("DATABASE"), 
        host=os.getenv("HOST"), 
        port=os.getenv("PORT"), 
    )

    return connection    


def execute_one_connection():
    connection = None
    try:

        connection = connect_banco_dados()
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION();")
        record = cursor.fetchone()

        print(f"Connected: {record}")

    except psycopg2.Error as error:
        print(f"Error: {error.args}")
        print("conexao \033[31mNAO\033[m realizada com o banco de dados")

    except Exception as e:
        print(f"Error: {e.args}")

    finally:
        if connection:
            cursor.close()
            connection.close()
            print("Conexão com o PostgreSQL fechada")


if __name__ == "__main__":
    execute_one_connection()
