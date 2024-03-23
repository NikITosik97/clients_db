import psycopg2


class ClientsData:
    conn = psycopg2.connect(database='clientsdata',
                            user='postgres',
                            password='postgres')

    def drop_all_db(self):
        with self.conn.cursor() as cur:
            cur.execute("""
                DROP TABLE phone_numbers;
                DROP TABLE clients;
            """)
        self.conn.commit()

    def create_table(self):
        with self.conn.cursor() as cur:
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
            self.conn.commit()

    def add_client(self, first_name: str, last_name: str, email: str):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT first_name, last_name, email
                FROM clients
                WHERE first_name = %s AND last_name = %s AND email = %s
            """, (first_name, last_name, email))
            client = cur.fetchall()
        if len(client) < 1:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO clients(
                        first_name,
                        last_name,
                        email
                    )
                    VALUES(%s, %s, %s)
                """, (first_name, last_name, email))

            self.conn.commit()

    def add_phone_number_for_client(self, client_id: int, number_phone=None):
        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT phone_number
                FROM phone_numbers
            """)
            numbers: list[()] = cur.fetchall()

            if len(numbers) > 0 and numbers[0][0] is None:
                cur.execute("""
                    UPDATE phone_numbers
                    SET phone_number = %s
                    WHERE client_id = %s
                """, (number_phone, client_id))
            else:
                cur.execute("""
                    INSERT INTO phone_numbers(
                        client_id,
                        phone_number
                    )
                    VALUES(%s, %s)
                """, (client_id, number_phone))
        self.conn.commit()

    def alter_data_for_client(self, client_id, **kwargs):
        for key, val in kwargs.items():
            match key:
                case 'first_name':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE clients
                            SET first_name = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        self.conn.commit()

                case 'last_name':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE clients
                            SET last_name = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        self.conn.commit()

                case 'email':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE clients
                            SET email = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        self.conn.commit()

                case 'phone_number':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            UPDATE phone_numbers
                            SET phone_number = %s
                            WHERE client_id = %s    
                        """, (val, client_id)
                                    )
                        self.conn.commit()

    def delete_phone_client(self, *args):
        if len(args) >= 1:
            for number in args:
                with self.conn.cursor() as cur:
                    cur.execute("""
                        DELETE FROM phone_numbers
                        WHERE phone_number = %s
                    """, (number,)
                                )
                    self.conn.commit()

    def delete_client(self, *client_id):

        with self.conn.cursor() as cur:
            cur.execute("""
                SELECT phone_number
                FROM phone_numbers
            """)
            numbers = cur.fetchall()
            if len(numbers) < 1:
                for id_ in client_id:
                    with self.conn.cursor() as cur1:
                        cur1.execute("""
                            DELETE FROM clients
                            WHERE client_id = %s
                        """, (id_,)
                                     )
                        self.conn.commit()
            else:
                self.delete_phone_client(numbers)
                with self.conn.cursor() as cur2:
                    cur2.execute("""
                        DELETE FROM clients
                        WHERE client_id = %s
                    """, client_id
                                 )
                self.conn.commit()

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

    def find_client_by_data(self, **kwargs):
        for key, val in kwargs.items():
            match key:
                case 'client_id':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                                SELECT client_id, first_name, last_name, email, phone_number
                                FROM clients
                                LEFT JOIN phone_numbers USING(client_id)
                                WHERE clients.client_id = %s
                            """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        self.conn.commit()

                case 'first_name':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE first_name = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        self.conn.commit()

                case 'last_name':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE last_name = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        self.conn.commit()

                case 'email':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE email = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        self.conn.commit()

                case 'phone_number':
                    with self.conn.cursor() as cur:
                        cur.execute("""
                            SELECT client_id, first_name, last_name, email, phone_number
                            FROM clients
                            LEFT JOIN phone_numbers USING(client_id)
                            WHERE phone_number = %s
                        """, (val,)
                                    )
                        print(self.show_client_data(*cur.fetchall()))
                        self.conn.commit()


clients_data = ClientsData()
clients_data.drop_all_db()
clients_data.create_table()
