from common import message_protocol

class MessageHandler:
    client_counter = 0

    def __init__(self):
        self.client_id = MessageHandler.client_counter
        MessageHandler.client_counter += 1
    
    def serialize_data_message(self, message):
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.client_id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.client_id])

    def deserialize_result_message(self, message):
        fields = message_protocol.internal.deserialize(message)
        if len(fields) == 2:
            client_id, fruit_top = fields
            if client_id == self.client_id:
                return fruit_top
        return None
