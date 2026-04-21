import os
import logging

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )

        self.all_fruits = []
        self.eofs_received = 0

    def process_messsage(self, message, ack, nack):
        logging.info("Received top")
        fruit_top = message_protocol.internal.deserialize(message)
        for fruit, amount in fruit_top:
            self.partial_sums[fruit] = self.partial_sums.get(fruit, 0) + amount
        ack()

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            self._process_data(*fields)
        else:
            self.eofs_received += 1
            if self.eofs_received == SUM_AMOUNT:
                self._process_eof()
        ack()

    def _process_data(self, fruit_top):
        logging.info("Process data")
        for fruit, amount in fruit_top:
            self.all_fruits.append(fruit_item.FruitItem(fruit, amount))
   
    def _process_eof(self):
        logging.info("Sending data messages")
        # Ordeno las frutas por cantidad, de mayor a menor
        self.all_fruits.sort(key=lambda item: item.amount, reverse=True)
        final_top_items = self.all_fruits[:TOP_SIZE]
        
        final_top = [(item.fruit, item.amount) for item in final_top_items]
        self.output_queue.send(
            message_protocol.internal.serialize(final_top)
        )
        self.output_queue.send(
            message_protocol.internal.serialize(
                []
             )
        )

        self.input_queue.stop_consuming()
    
    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
