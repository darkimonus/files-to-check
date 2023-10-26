import logging

from fastapi import WebSocket
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.ws.websocket_connection import ws_manager
from app.errors.app_errors import NotFoundError
from app.models import User
from app.services.chatroom import ChatService
from app.services.message import MessageService

logger = logging.getLogger(__name__)


class ChatRoom:
    def __init__(
        self,
        websocket: WebSocket,
        db: AsyncSession,
        current_user: User,
        other_user_nickname: str,
    ):
        self.websocket = websocket
        self.db = db
        self.current_user = current_user
        self.other_user_nickname = other_user_nickname
        self.room = None

    async def on_connect(self):
        if self.current_user.nickname == self.other_user_nickname:
            raise RuntimeError("The user can't connect to the room")

        self.room = await ChatService(self.db).get_or_create_room(
            self.current_user, self.other_user_nickname
        )
        try:
            messages = await MessageService(self.db).get_all_messages_in_room(
                self.room.id
            )
            chat_messages = [
                {
                    "message_id": message.id,
                    "message_context": message.context,
                    "author_id": message.author_id,
                }
                for message in messages
            ]
        except NotFoundError:
            chat_messages = []

        response = {
            "type": "USER_JOIN",
            "room_id": self.room.id,
            "user": {
                "user_id": self.current_user.id,
                "user_nickname": self.current_user.nickname,
            },
            "chat_messages": chat_messages,
        }
        await ws_manager.connection_state(
            self.db, self.websocket, self.room.id, response
        )

    async def on_receive(self):
        msg = await self.websocket.receive_json()
        chat_service = ChatService(self.db)
        message_service = MessageService(self.db)

        if msg["action"] == "CREATE":
            new_message = await chat_service.upload_message_to_room(
                self.current_user, self.room, msg
            )
            response = {
                "type": "CREATE_MESSAGE",
                "room_id": self.room.id,
                "user": {
                    "user_id": self.current_user.id,
                    "user_nickname": self.current_user.nickname,
                },
                "message": {
                    "message_id": new_message.id,
                    "message_context": new_message.context,
                },
            }
            await ws_manager.connection_state(
                self.db, self.websocket, self.room.id, response
            )
        elif msg["action"] == "DELETE":
            await message_service.delete_message(msg["content"])
            response = {
                "type": "DELETE_MESSAGE",
                "message_id": msg["content"],
            }
            await ws_manager.connection_state(
                self.db, self.websocket, self.room.id, response
            )
        elif msg["action"] == "UPDATE":
            message_db = await message_service.update_message(
                msg["message_id"], msg["content"]
            )
            response = {
                "type": "UPDATE_MESSAGE",
                "message_id": message_db.id,
                "message_context": message_db.context,
            }
            await ws_manager.connection_state(
                self.db, self.websocket, self.room.id, response
            )

    async def on_disconnect(self):
        response = {
            "type": "USER_LEAVE",
            "room_id": self.room.id,
            "user_id": self.current_user.id,
            "user_nickname": self.current_user.nickname,
        }
        await ws_manager.connection_state(
            self.db, self.websocket, self.room.id, response
        )
