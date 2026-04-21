import os
import logging
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
        self.data_output_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            data_output_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
                MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
            )
            self.data_output_exchanges.append(data_output_exchange)
        self.amount_by_fruit = {}

        self.control_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, SUM_CONTROL_EXCHANGE, [EOF_BROADCAST]
        )

        self.lock = threading.Lock()

    def _process_data(self, fruit, amount):
        logging.info(f"Process data")
        self.amount_by_fruit[fruit] = self.amount_by_fruit.get(
            fruit, fruit_item.FruitItem(fruit, 0)
        ) + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self):
        logging.info(f"Broadcasting data messages")
        for final_fruit_item in self.amount_by_fruit.values():
            
            first_letter = final_fruit_item.fruit[0]
            
            letter_number = ord(first_letter) - 97
            
            # Uso modulo para distribuir las frutas entre los filtros de agregación
            target_idx = letter_number % AGGREGATION_AMOUNT
            
            target_exchange = self.data_output_exchanges[target_idx]
            target_exchange.send(
                message_protocol.internal.serialize(
                    [final_fruit_item.fruit, final_fruit_item.amount]
                )
            )
            
        logging.info(f"Broadcasting EOF message")
        for data_output_exchange in self.data_output_exchanges:
            data_output_exchange.send(message_protocol.internal.serialize([]))


    def process_data_messsage(self, message, ack, nack):
        fields = message_protocol.internal.deserialize(message)
        with self.lock:
            if len(fields) == 2:
                self._process_data(*fields)
            else:
                self.control_exchange.send(message_protocol.internal.serialize([]))
        ack()

    def start(self):
        eof_control_thread = threading.Thread(target=self._listen_for_eof)
        eof_control_thread.start()

        self.input_queue.start_consuming(self.process_data_messsage)

        eof_control_thread.join()

    def _listen_for_eof(self):
        self.control_exchange.start_consuming(self._process_eof_message)

    def _process_eof_message(self, message, ack, nack):
        logging.info("Received EOF message.")

        self._process_eof()

        self.input_queue.stop_consuming()
        self.control_exchange.stop_consuming()

        ack()

def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()
    sum_filter.start()
    return 0


if __name__ == "__main__":
    main()
