import asyncio
import sql_service
import service
from decouple import config
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import UserBlocked, UserDeactivated

ID = config('ID')
HASH = config('HASH')

app = Client("me", ID, HASH)


@app.on_message(filters.text & filters.private)
async def process_message(client, message: Message):
    await app.send_message(chat_id=message.from_user.id,text = 'tep')
    async with await sql_service.create_session() as session:
        await sql_service.chek_client_id(session, client_id=message.chat.id)
        if message.chat.id == client.me.id:
            if "прекрасно" in message.text.lower() or "ожидать" in message.text.lower():
                await sql_service.cancel(client, message)
            else:
                await sql_service.cancel_msg2(session, message.chat.id)
        await session.commit()


@app.on_message()
async def send_message1(client, message: Message):
    await client.send_message(message.chat.id, "Это сообщение 1")

@app.on_message(filters.command("send_message2"))
async def send_message2(client, message: Message):
    async with await sql_service.create_session() as session:
        await sql_service.create_client_and_msg2(session, message.chat.id)
        await client.send_message(message.chat.id, "Это сообщение 2")
        await session.commit()


@app.on_message(filters.command("send_message3"))
async def send_message3(client, message: Message):
    async with await sql_service.create_session() as session:
        await sql_service.create_client_and_msg3(session, message.chat.id)
        await client.send_message(message.chat.id, "Это сообщение 3")
        await session.commit()


@app.on_message()
async def error_handler(client, error):
    if isinstance(error, UserBlocked):
        print("Бот был заблокирован пользователем.")
    elif isinstance(error, UserDeactivated):
        print("Пользователь деактивировал свой аккаунт.")
    else:
        print(f"Произошла ошибка: {error}")


async def main():
    await app.start()
    await asyncio.Future()
    await service.run()


if __name__ == "__main__":
    asyncio.run(main())
