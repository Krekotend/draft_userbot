import asyncio
import sql_service
import service
import logging
from decouple import config
from pyrogram import Client, filters
from pyrogram.types import Message
from pyrogram.errors import UserBlocked, UserDeactivated

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


ID = config('ID')
HASH = config('HASH')

app = Client("me", ID, HASH)

async def send_message1(id):
    await app.send_message(id, "Это сообщение 1")

async def send_message2(id):
    await app.send_message(id, "Это сообщение 2")

async def send_message3(id):
    await app.send_message(id, "Это сообщение 3")

@app.on_message()
async def process_message(client, message: Message):
    async with await sql_service.create_session() as session:
        await sql_service.check_client_id(session, client_id=message.from_user.id)
    if message.from_user.id == client.me.id:
        if "прекрасно" in message.text.lower() or "ожидать" in message.text.lower():
            await sql_service.cancel(message.from_user.id, message)
        else:
            await sql_service.cancel_msg2(session, message.chat.id)
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
    while True:
        await asyncio.gather(
            service.run(),
            app.start()
            )

if __name__ == "__main__":
    app.run(main())

#     Выдает ошибку
#     raise ConnectionError("Client has not been started yet")
# ConnectionError: Client has not been started yet

