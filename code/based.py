import psycopg2

class based:
    def __init__(self):
        self.info = "host='34.48.163.96' port='5432' user='tpv' password='PSpsalm14!!!' dbname='tpv-dev' sslmode = 'require'"
        
        #self.info = "host='34.48.99.7' port='5432' user='tpv' password=';AQ}*DHKfO6gVDH4' dbname='tpvdb' sslmode = 'require'"
        self.connection = None
        self.cursor = None
    def connect(self):
        try:
            self.connection = psycopg2.connect(self.info)
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(f'Error connecting: {e}')

    def close(self):
        if self.connection is not None:
            self.cursor.close()
            self.connection.close()
        else:
            print('Connection already closed')

    def get_version(self):
        query = "SELECT version();"
        self.cursor.execute(query)
        return(self.cursor.fetchone())

    def get_zips(self):
        self.cursor.execute("SELECT DISTINCT zipcode FROM offenders")
        return [z[0] for z in self.cursor.fetchall()]

    def get_image_links(self, state):
        query = "SELECT image_link FROM offenders WHERE state = %s;"
        self.cursor.execute(query, (state,))
        return [i[0] for i in self.cursor.fetchall()]

