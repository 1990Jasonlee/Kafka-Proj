from kafka import KafkaConsumer
from json import loads
import sqlalchemy

consumer = KafkaConsumer(
    'bank-customer-events',
     bootstrap_servers=['localhost:9092'],
     value_deserializer=lambda m: loads(m.decode('ascii')))

for message in consumer:
    print(message)
    message = message.value
    print('{} found'.format(message))

class Transaction(Base):
    __tablename__ = 'transaction'
    # Here we define columns for the table person
    # Notice that each column is also a normal Python instance attribute.
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250), nullable=False)
    date = Column(Integer)
    amt = Column(Integer)
