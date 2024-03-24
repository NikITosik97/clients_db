import psycopg2


class ClientsData:
    def connection_db(self):
        with psycopg2.connect(database='clientsdata',
                              user='postgres',
                              password='postgres') as conn_:
            return conn_

    def drop_all_db(self, conn_):
        with conn_.cursor() as cur:
            cur.execute("""
                DROP TABLE phone_numbers;
                DROP TABLE clients;
            """)
        conn_.commit()

    def create_table(self, conn_):
        with conn_.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clients(
                    client_id SERIAL PRIMARY KEY,
                    first_name text NOT NULL,
                    last_name text NOT NULL,
                    email text NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS phone_numbers(
                    client_id int REFERENCES clients(client_id) NOT NULL,
                    phone_number varchar(60)
                )
            """)
            conn_.commit()

    def add_client(self, conn_, first_name: str, last_name: str, email: str):
        with conn_.cursor() as cur:
            cur.execute("""
                SELECT first_name, last_name, email
                FROM clients
                WHERE first_name = %s AND last_name = %s AND email = %s
            """, (first_name, last_name, email))
            client = cur.fetchall()
        if len(client) < 1:
            with conn_.cursor() as cur:
                cur.execute("""
                    INSERT INTO clients(
                        first_name,
                        last_name,
                        email
                    )
                    VALUES(%s, %s, %s)
                """, (first_name, last_name, email))

            conn_.commit()

    def add_phone_number_for_client(self, conn_, client_id: int, number_phone=None):
        with conn_.cursor() as cur:
            cur.execute("""
                SELECT phone_number
                FROM phone_numbers
            """)
            numbers: list[()] = cur.fetchall()

            if len(numbers) < 1:
                cur.execute("""
                    INSERT INTO phone_numbers(
                        client_id,
                        phone_number
                    )
                    VALUES(%s, %s)
                """, (client_id, number_phone))
            else:
                set_numbers = set()
                for number in numbers:
                    set_numbers.add(*number)

                if number_phone not in set_numbers:
                    cur.execute("""
                        INSERT INTO phone_numbers(
                            client_id,
                            phone_number
                        )
                        VALUES(%s, %s)
                    """, (client_id, number_phone))

        conn_.commit()

    def alter_data_for_client(self, conn_, client_id, **kwargs):
        for key, val in kwargs.items():
            match key:
                case 'first_name':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            UPDATE clients
                            SET first_name = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        conn_.commit()

                case 'last_name':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            UPDATE clients
                            SET last_name = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        conn_.commit()

                case 'email':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            UPDATE clients
                            SET email = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        conn_.commit()

                case 'phone_number':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            UPDATE phone_numbers
                            SET phone_number = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        conn_.commit()

    def delete_phone_client(self, conn_, *args):
        if len(args) >= 1:
            for number in args:
                with conn_.cursor() as cur:
                    cur.execute("""
                        DELETE FROM phone_numbers
                        WHERE phone_number = %s
                    """, (number,)
                                )
                    conn_.commit()

    def delete_client(self, conn_, *client_id):

        with conn_.cursor() as cur:
            cur.execute("""
                SELECT phone_number
                FROM phone_numbers
            """)
            numbers = cur.fetchall()
            if len(numbers) < 1:
                for id_ in client_id:
                    with conn_.cursor() as cur:
                        cur.execute("""
                            DELETE FROM clients
                            WHERE client_id = %s
                        """, (id_,)
                                    )
                        conn_.commit()
            else:
                self.delete_phone_client(numbers)
                with conn_.cursor() as cur:
                    cur.execute("""
                        DELETE FROM clients
                        WHERE client_id = %s
                    """, client_id
                                )
                conn_.commit()

    def show_client_data(self, *args):

        client_id = None
        first_name = None
        last_name = None
        email = None
        phone_number = []

        client_data = dict()

        for data in args:
            if data[0] == client_id:
                for i in data[4:]:
                    phone_number.append(i)
            else:

                for ind, val in enumerate(data):
                    if ind == 0:
                        client_id = val
                    elif ind == 1:
                        first_name = val
                    elif ind == 2:
                        last_name = val
                    elif ind == 3:
                        email = val
                    else:
                        phone_number = [val]

            client_data.update({client_id: {'first_name': first_name,
                                            'last_name': last_name,
                                            'email': email,
                                            'phone_number': phone_number
                                            }
                                })
        return client_data

    def find_client_by_data(self, conn_, **kwargs):
        for key, val in kwargs.items():
            match key:
                case 'client_id':
                    with conn_.cursor() as cur:
                        cur.execute("""
                                SELECT client_id, first_name, last_name, email, phone_number
                                FROM clients
                                LEFT JOIN phone_numbers USING(client_id)
                                WHERE clients.client_id = %s
                            """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        conn_.commit()

                case 'first_name':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE first_name = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        conn_.commit()

                case 'last_name':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE last_name = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        conn_.commit()

                case 'email':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE email = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        conn_.commit()

                case 'phone_number':
                    with conn_.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE phone_number = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        conn_.commit()


if __name__ == '__main__':
    clients_data = ClientsData()
    conn = clients_data.connection_db()
    clients_data.drop_all_db(conn)
    clients_data.create_table(conn)
