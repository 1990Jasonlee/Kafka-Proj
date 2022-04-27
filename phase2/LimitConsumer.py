from kafka import KafkaConsumer, TopicPartition
from json import loads
# from sqlalchemy import create_engine, Column, Integer, String
# from sqlalchemy.ext.declarative import declarative_base
# import os
import psycopg2

#docs.sqlalchemy.org/en/13/orm/extensions/declarative/basic_use.html
# engine = create_engine(os.getenv('sqlite:///test.db'))
# Base = declarative_base(bind=engine)

# class Transaction(Base):
#     __tablename__ = 'transaction'
#     # Here we define columns for the table person
#     # Notice that each column is also a normal Python instance attribute.
#     id = Column(Integer, primary_key=True)
#     custid = Column(Integer)
#     type = Column(String(250), nullable=False)
#     date = Column(Integer)
#     amt = Column(Integer)

class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
            bootstrap_servers=['localhost:9092'],
            # auto_offset_reset='earliest',
            value_deserializer=lambda m: loads(m.decode('ascii')))
        # These are two python dictionaries
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current balance of each customer
        # account is kept.
        self.custBalances = {}
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        #Go back to the readme.
        self.limit = -5000
        self.conn = psycopg2.connect()
        self.cur = self.conn.cursor()

        create_table = '''CREATE TABLE IF NOT EXISTS Transaction(
                            custid int NOT NULL,
                            type text NOT NULL,
                            date int NOT NULL,
                            amt int NOT NULL)'''
        self.cur.execute(create_table)
        self.conn.commit()

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            for key, value in self.custBalances.items():
                if value < self.limit:
                    print('Over cash limit', key, value)

            self.cur = self.conn.cursor()
            messages = (message['custid'], message['type'], message['date'], message['amt'])
            insert_table = 'INSERT INTO Transaction (custid, type, date, amt) VALUES (%s, %s, %s, %s)'
            self.cur.execute(insert_table, messages)
            self.conn.commit()


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
