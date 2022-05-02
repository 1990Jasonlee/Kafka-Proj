from kafka import KafkaConsumer, TopicPartition
from json import loads
from statistics import mean, stdev
import psycopg2


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
        self.deposits = []
        self.withdraw = []

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
            print(self.custBalances)
            print(f'The mean of your deposits is: ' + str(mean(self.deposits)))
            print(f'The mean of your withdrawals is: ' + str(mean(self.deposits)))
            print(f'The std deviation of your deposits is: ' + str(stdev(self.deposits)))
            if len(self.withdraw) == 0:
                print("You haven't made a withdrawal.")
            else:
                print(f'The std deviation of your withdrawals is: ' + str(stdev(self.deposits)))


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
