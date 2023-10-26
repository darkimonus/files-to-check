import asyncio
import json

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from fastapi.responses import JSONResponse

from app.core.config import settings
from app.core.ws.websocket_connection import ws_manager
from app.errors.app_errors import InvalidEmailOrPhone
from app.services import AuthService
from app.utils.utils import isValid


class KafkaService:
    @staticmethod
    async def send_confirmation_code_producer(
        email_or_phone,
        producer: AIOKafkaProducer = Provide["kafka_producer"],
        confirmation_topic: str = Provide["confirmation_topic"],
    ):
        """Send the message to the Kafka topic"""
        if not isValid(email_or_phone):
            raise InvalidEmailOrPhone()
        if isinstance(email_or_phone, str):
            value_json = json.dumps(email_or_phone.strip('"')).encode("utf-8")
        else:
            value_json = json.dumps(
                email_or_phone.dict()["email_or_phone"].strip('"')
            ).encode("utf-8")
        await producer.send_and_wait(topic=confirmation_topic, value=value_json)

    @staticmethod
    async def send_confirmation_code_consumer(
        consumer: AIOKafkaProducer = Provide["consumer"],
        confirmation_topic: str = Provide["confirmation_topic"],
    ):
        """Consume the messages from the Kafka topic and sends verification code"""
        consumer.subscribe(topics=(confirmation_topic, ))
        async for msg in consumer:
            email_or_phone = json.loads(msg.value)
            await asyncio.get_event_loop().run_in_executor(
                None, AuthService().send_verification_code, email_or_phone
            )

    @staticmethod
    async def send_chat_message_producer(
        message,
        producer: AIOKafkaProducer = Provide["kafka_producer"],
        chat_rooms_topic: str = Provide["chat_rooms_topic"],
    ):
        value_json = json.dumps(message).encode("utf-8")
        await producer.send_and_wait(topic=chat_rooms_topic, value=value_json)

    @staticmethod
    async def send_chat_message_consumer(
        consumer: AIOKafkaProducer = Provide["consumer"],
        confirmation_topic: str = Provide["confirmation_topic"],
    ):
        consumer.subscribe(topics=(confirmation_topic, ))
        async for msg in consumer:
            message = json.loads(msg.value)
            room_id = message["room_id"]
            await ws_manager.broadcast(room_id, message)
