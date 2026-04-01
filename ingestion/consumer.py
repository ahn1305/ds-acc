import json
from channels.generic.websocket import AsyncWebsocketConsumer

class BatchConsumer(AsyncWebsocketConsumer):

    async def connect(self):
        self.batch_id = self.scope["url_route"]["kwargs"]["batch_id"]
        self.group_name = f"batch_{self.batch_id}"

        await self.channel_layer.group_add(
            self.group_name,
            self.channel_name
        )

        await self.accept()

    async def disconnect(self, close_code):
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )

    async def send_update(self, event):
        await self.send(text_data=json.dumps(event["data"]))

# ingestion/consumer.py

async def connect(self):
    print("WS CONNECT ATTEMPT")  # 👈 MUST PRINT

    self.batch_id = self.scope["url_route"]["kwargs"]["batch_id"]
    self.group_name = f"batch_{self.batch_id}"

    await self.channel_layer.group_add(
        self.group_name,
        self.channel_name
    )

    await self.accept()

    print("WS CONNECTED")  # 👈 MUST PRINT

