import logging
from typing import Union

from sqlalchemy import select, update
from sqlalchemy.orm import joinedload, selectinload, subqueryload

from app.errors.app_errors import NotFoundError
from app.models import ChatMessage, ChatRoom, User
from app.schemas import MessageCreate
from app.services import UserService
from app.services.base import BaseService
from app.services.message import MessageService

logger = logging.getLogger(__name__)


class ChatService(BaseService):
    async def get_or_create_room(self, current_user: User, other_user_nickname: str):
        user_db = await UserService(self.db).get_user_by_field(
            user_field="nickname", data=other_user_nickname
        )
        if room_db := await self.get_room_by_nickname(
            current_user, other_user_nickname
        ):
            return room_db

        room = ChatRoom(
            room_name=f"{current_user.nickname}|{other_user_nickname}",
            users=[user_db, current_user],
        )
        self.db.add(room)
        await self.db.commit()
        return await self.get_room_by_id(room.id, current_user, filter_result=False)

    async def insert_room(self, current_user: User, nickname: str) -> ChatRoom:
        user_service = UserService(self.db)
        user_db_1 = await user_service.get_user_by_nickname(nickname)
        if not user_db_1:
            raise NotFoundError("User")

        room_db = await self.get_room_by_nickname(current_user, nickname)
        if room_db:
            await self._extract_nickname(room_db, current_user)
            return room_db

        room = ChatRoom(
            room_name=f"{current_user.nickname}|{nickname}",
            users=[user_db_1, current_user],
        )
        self.db.add(room)
        await self.db.commit()

        room_db = await self.get_room_by_id(room.id, current_user, filter_result=False)
        return room_db

    async def get_user_rooms(self, current_user: User) -> User:
        query = await self.db.execute(
            select(ChatRoom)
            .options(subqueryload(ChatRoom.users))
            .options(subqueryload(ChatRoom.room_messages))
            .filter(ChatRoom.users.any(User.id == current_user.id))
        )
        result = query.scalars().all()
        for room in result:
            await self._extract_nickname(room, current_user)
        return result

    async def get_room_by_id(
        self, room_id: int, current_user: User = None, filter_result: bool = True
    ) -> ChatRoom:
        stmt = (
            select(ChatRoom)
            .options(selectinload(ChatRoom.users))
            .options(selectinload(ChatRoom.room_messages))
        )
        if current_user:
            query = await self.db.execute(
                stmt.where(
                    ChatRoom.users.any(User.id == current_user.id),
                    ChatRoom.id == room_id,
                )
            )
        else:
            query = await self.db.execute(stmt.where(ChatRoom.id == room_id))

        room_db = query.scalars().first()
        if all([current_user, filter_result, room_db]):
            await self._extract_nickname(room_db, current_user)

        if not room_db:
            raise NotFoundError("Chat")
        return room_db

    async def get_room_by_nickname(self, current_user: User, nickname: str) -> ChatRoom:
        query = await self.db.execute(
            select(ChatRoom)
            .options(joinedload(ChatRoom.users))
            .options(joinedload(ChatRoom.room_messages))
            .filter(ChatRoom.users.any(User.nickname.in_([nickname])))
            .filter(ChatRoom.users.any(User.nickname.in_([current_user.nickname])))
        )
        return query.scalars().first()

    async def set_room_activity(self, room_id: int, activity_bool: bool):
        room_db = await self.get_room_by_id(room_id)
        if room_db is not None:
            try:
                result = await self.db.execute(
                    update(ChatRoom)
                    .where(ChatRoom.id == room_id)
                    .values(active=activity_bool)
                    .returning(ChatRoom.active)
                )
                await self.db.commit()
                logger.info(f"Updated room activity {result}")
            except Exception as e:
                logger.error(f"ERROR SETTING ACTIVITY: {e}")
            return await self.get_room_by_id(room_id)
        return None

    async def upload_message_to_room(
        self, current_user: User, room_db: ChatRoom, message: dict
    ) -> Union[ChatMessage, bool]:
        try:
            return await MessageService(self.db).create_message(
                MessageCreate(
                    context=message["content"],
                    room_id=room_db.id,
                    author_id=current_user.id,
                )
            )
        except Exception as e:
            logger.error(f"Error adding message to DB: {type(e)} {e}")
            return False

    async def remove_user_from_chat(self, current_user: User, room_id: int):

        room_db = await self.get_room_by_id(room_id, current_user, filter_result=False)

        rooms_user_list = [user.id for user in room_db.users]
        if current_user.id in rooms_user_list:
            logger.info(f"Removing chat with # {room_id} from your chats.")
            room_db.users.remove(current_user)
            self.db.add(room_db)

            await self.db.commit()

            if not room_db.users:
                await self.db.delete(room_db)
                await self.db.commit()

        else:
            logger.info(f"You don't have chat # {room_id}")

    @staticmethod
    async def _extract_nickname(room_db: ChatRoom, current_user: User):
        friend_name = room_db.room_name.split("|")
        friend_name.remove(f"{current_user.nickname}")
        room_db.room_name = "".join(friend_name)
