import psycopg2
from dotenv import load_dotenv
import os
from typing import List, Tuple, Iterable

load_dotenv()


class Connection:

    @staticmethod
    def _connect_banco_dados() -> psycopg2.connect:
        connection = psycopg2.connect(
            user=os.getenv("USER"),
            password=os.getenv("PASSWORD"),
            database= os.getenv("DATABASE"), 
            host=os.getenv("HOST"), 
            port=os.getenv("PORT"), 
        )
        print("Conexão \033[32mrealizada\033[m com o banco de dados")
        return connection    

    @staticmethod
    def _close_connection(connection: psycopg2.connect) -> None:
        if connection:
            connection.close()
            print("Conexão com o PostgreSQL fechada")

    @staticmethod
    def execute_sql(sql: str, data: Iterable[Iterable] = None) -> None:
        connection = None
        try:
            connection = Connection._connect_banco_dados()
            cursor = connection.cursor()
            if data:
                for item in data:
                    cursor.execute(sql, item)
                    print(sql, *cursor.fetchone(), sep='\n')

            else:
                cursor.execute(sql)
                print(sql, *cursor.fetchall(), sep='\n')
            connection.commit()
        except psycopg2.Error as error:
            print(f"Error: {error.args}")
            print(error.diag.context)
            print(error.diag.constraint_name)
            print(error.diag.message_detail)
            print(error.diag.internal_query)
            print(error.diag.message_hint)
            print("conexao \033[31mNAO\033[m realizada com o banco de dados")
        except Exception as e:
            print(f"Error: {e.args}")
        finally:
            cursor.close()
            Connection._close_connection(connection)

    @staticmethod
    def insert_sql(table_name: str, fields: Iterable[str] | Tuple[str], data: Iterable[Iterable]) -> None:
        
        if (not isinstance(data, (List, Tuple, set))):
            raise TypeError(f"{data} is Not Iterable")
        if  not isinstance(fields, (List, Tuple, set)):
            raise TypeError(f"{fields} is Not Iterable")

        sql = f'INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({(len(fields)-1)*"%s, "+"%s"}) returning {fields[0]};'
        Connection.execute_sql(sql, data=data)
        

if __name__ == "__main__":

    # alunos = [
    #     ('Dayse', 'Brown', '1975-07-30'),
    #     ('Marcelo', 'Baptista', '1971-04-19'),
    #     ('Joao', 'Rodrigues', '1998-08-13')
    # ]
    # Connection.insert_sql('aluno', ('primeiro_nome', 'ultimo_nome', 'data_nascimento'), alunos)

    Connection.execute_sql("SELECT * FROM aluno")
    Connection.execute_sql("SELECT * FROM curso")
    Connection.execute_sql("SELECT * FROM aluno_curso")
    Connection.execute_sql("SELECT * FROM categoria")
