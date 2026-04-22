import os
import logging
import signal

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

        self.fruits_by_client = {}
        self.eofs_received_by_client = {}

    def process_messsage(self, message, ack, nack):
        logging.info("Process message")
        fields = message_protocol.internal.deserialize(message)
        logging.info(f"Deserialized message: {fields}")

        if len(fields) == 2:
            self._process_data(*fields)
        else:
            client_id = fields[0]
            
            self.eofs_received_by_client[client_id] = self.eofs_received_by_client.get(client_id, 0) + 1
            if self.eofs_received_by_client[client_id] == AGGREGATION_AMOUNT:
                logging.info(f"Received all EOFs for client {client_id}. Processing final top fruits.")
                self._process_eof(client_id)
        ack()

    def _process_data(self, client_id, fruit_top):
        logging.info("Process data: {fruit_top}")
        if client_id not in self.fruits_by_client:
            self.fruits_by_client[client_id] = []

        for fruit, amount in fruit_top:
            self.fruits_by_client[client_id].append(fruit_item.FruitItem(fruit, amount))

    def _process_eof(self, client_id):
        logging.info(f"Processing EOF for client {client_id}")

        logging.info(f"All fruits received for client {client_id}: {[ (item.fruit, item.amount) for item in self.fruits_by_client[client_id] ]}")
        # Ordeno las frutas por cantidad, de mayor a menor
        self.fruits_by_client[client_id].sort(reverse=True)
        # self.all_fruits.sort(reverse=True)
        final_top_items = self.fruits_by_client[client_id][:TOP_SIZE]
        
        final_top = [(item.fruit, item.amount) for item in final_top_items]
        logging.info(f"Final top fruits: {final_top} for client {client_id}")

        self.output_queue.send(
            message_protocol.internal.serialize([client_id, final_top])
        )

        del self.fruits_by_client[client_id]
        del self.eofs_received_by_client[client_id]
    
    def start(self):
        self.input_queue.start_consuming(self.process_messsage)

def main():
    logging.basicConfig(level=logging.INFO)
    join_filter = JoinFilter()

    def handle_sigterm(signum, frame):
        logging.info("Received SIGTERM signal")
        join_filter.input_queue.stop_consuming()
        join_filter.input_queue.close()
        join_filter.output_queue.close()

    signal.signal(signal.SIGTERM, handle_sigterm)
    join_filter.start()

    return 0


if __name__ == "__main__":
    main()
