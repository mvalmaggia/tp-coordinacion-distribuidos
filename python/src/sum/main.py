import os
import logging
import signal
import threading

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
SUM_CONTROL_EXCHANGE = "SUM_CONTROL_EXCHANGE"
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]

EOF_BROADCAST = "EOF_BROADCAST"

class SumFilter:
    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        
        self.client_amounts = {}

        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [EOF_BROADCAST]
        )

        self.data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX,
            [f"{AGGREGATION_PREFIX}_{i}" for i in range(AGGREGATION_AMOUNT)]
        )

        self.lock = threading.Lock()
        self.eof_handled_by_client = {}

    def _process_data(self, client_id, fruit, amount):
        logging.info(f"Process data for client {client_id}")
        fruit = fruit.lower()

        if client_id not in self.client_amounts:
            self.client_amounts[client_id] = {}

        client_dict = self.client_amounts[client_id]
        if fruit in client_dict.keys():
            client_dict[fruit] = client_dict[fruit] + fruit_item.FruitItem(fruit, int(amount))
        else:
            client_dict[fruit] = fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, client_id):
        logging.info(f"Routing data messages")

        if client_id in self.client_amounts:
            client_fruits = self.client_amounts[client_id]
            for item in client_fruits.values():
                first_letter = item.fruit[0]
                letter_number = ord(first_letter) - ord('a')

                target_idx = letter_number % AGGREGATION_AMOUNT
                target_routing_key = f"{AGGREGATION_PREFIX}_{target_idx}"
                # logging.info(f"Sending data message for client {client_id}, fruit {item.fruit}, amount {item.amount} to exchange {target_routing_key}")
                self.data_output_exchange.send(
                    message_protocol.internal.serialize(
                        [client_id, item.fruit, item.amount]
                    ),
                    routing_key=target_routing_key
                )
            
            del self.client_amounts[client_id]

        logging.info(f"Broadcasting EOF message for client {client_id}")
        for i in range(AGGREGATION_AMOUNT):
            self.data_output_exchange.send(
                message_protocol.internal.serialize([client_id]),
                routing_key=f"{AGGREGATION_PREFIX}_{i}"
            )


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        with self.lock:
            if len(fields) == 3:
                client_id, fruit, amount = fields
                
                self._process_data(client_id, fruit, amount)

            elif len(fields) == 1:
                client_id = fields[0]
                self.control_exchange.send(message_protocol.internal.serialize([client_id]))
        ack()

    def start(self):
        eof_control_thread = threading.Thread(target=self._listen_for_eof)
        eof_control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)

        eof_control_thread.join()

    def _listen_for_eof(self):
        control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [EOF_BROADCAST]
        )

        control_exchange.start_consuming(self._process_eof_message)
        
    def _process_eof_message(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        client_id = fields[0]

        with self.lock:
            if not self.eof_handled_by_client.get(client_id, False):
                self.eof_handled_by_client[client_id] = True
                logging.info(f"Received EOF message for client {client_id}.")

                self._process_eof(client_id)

        ack()


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        sum_filter.input_queue.stop_consuming()
        sum_filter.input_queue.close()
        sum_filter.control_exchange.close()
        sum_filter.data_output_exchange.close()

    signal.signal(signal.SIGTERM, handle_sigterm)
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
