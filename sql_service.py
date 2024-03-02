import asyncio
import logging
from datetime import datetime, timedelta
from sqlalchemy import text, Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

Base = declarative_base()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


class Client(Base):
    __tablename__ = 'clients'
    id = Column(Integer, primary_key=True)
    status = Column(Integer, ForeignKey('status.id'))
    create_at = Column(DateTime)
    status_update = Column(DateTime)


class Status(Base):
    __tablename__ = 'status'
    id = Column(Integer, primary_key=True)
    status = Column(String)


class Msg1(Base):
    __tablename__ = 'msg1'
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey('clients.id'))
    data = Column(DateTime)


class Msg2(Base):
    __tablename__ = 'msg2'
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey('clients.id'))
    data = Column(DateTime)


class Msg3(Base):
    __tablename__ = 'msg3'
    id = Column(Integer, primary_key=True)
    client_id = Column(Integer, ForeignKey('clients.id'))
    data = Column(DateTime)


engine = create_async_engine('postgresql+asyncpg://postgres:230890@localhost:5600/test_userbot')


async def create_session():
    async_session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)
    async with async_session() as session:
        return session


async def create_client_and_msg1(session, client_id):
    logging.info(f"New client for client_id: {client_id}")
    status_alive = await session.execute(text("SELECT id FROM status WHERE status = 'alive'"))
    status_id = status_alive.scalar()

    client = Client(id=client_id, status=status_id, create_at=datetime.now(), status_update=datetime.now())
    msg1 = Msg1(client_id=client_id, data=datetime.now())

    session.add_all([client, msg1])
    await session.commit()
    logging.info(f"The recording successfully")


async def check_and_move_msg(session, source_table, destination_table, minutes: int):
    current_time = datetime.now()
    minutes_ago = current_time - timedelta(minutes=minutes)

    query = text(f"""
           SELECT {source_table}.client_id
           FROM {source_table} 
           JOIN clients ON {source_table}.client_id = clients.id
           JOIN status ON clients.status = status.id  
           WHERE {source_table}.data <= :minutes_ago AND status.id = :status_id
       """)

    status_alive = await session.execute(text("SELECT id FROM status WHERE status = 'alive'"))
    status_id = status_alive.scalar()

    results = await session.execute(query, {'minutes_ago': minutes_ago, 'status_id': status_id})
    msg_ids = [msg_id for msg_id, in results.fetchall()]

    client_ids_moved = []

    for msg_id in msg_ids:
        await session.execute(
            text(f"INSERT INTO {destination_table} (client_id, data) VALUES (:client_id, :current_time)"),
            {'client_id': msg_id, 'current_time': current_time}
        )
        await session.execute(text(f"DELETE FROM {source_table} WHERE client_id = :msg_id"), {'msg_id': msg_id})

        client_ids_moved.append(msg_id)

    await session.commit()

    return client_ids_moved


async def check_client_id(session, client_id):
    client = await session.execute(text("SELECT id FROM clients WHERE id = :client_id"), {'client_id': client_id})
    if not client.scalar():
        await create_client_and_msg1(session, client_id)


async def send_msg1():
    session = await create_session()
    try:
        id_list = await check_and_move_msg(session, 'msg1', 'msg2', minutes=6)
        return id_list
    finally:
        await session.close()


async def send_msg2():
    session = await create_session()
    try:
        id_list = await check_and_move_msg(session, 'msg2', 'msg3', minutes=39) 
        return id_list
    finally:
        await session.close()


async def send_msg3():
    session = await create_session()
    try:
        current_time = datetime.now()
        minutes_ago = current_time - timedelta(minutes=1560)

        query = text(f"""
                SELECT msg3.client_id
                FROM msg3
                JOIN clients ON msg3.client_id = clients.id
                JOIN status ON clients.status = status.id
                WHERE msg3.data <= :minutes_ago AND status.status = 'alive'
            """)
        results = await session.execute(query, {'minutes_ago': minutes_ago})

        client_ids_moved = []

        for client_id, in results.fetchall():
            await session.execute(text(
                f""" UPDATE clients 
                    SET status = (SELECT id FROM status WHERE status = 'finished') 
                    WHERE id = :client_id
                    """),
                {'client_id': client_id})

            await session.execute(text(f"DELETE FROM msg3 WHERE client_id = :client_id", {'client_id': client_id}))

            client_ids_moved.append(client_id)

        await session.commit()

        return client_ids_moved
    finally:
        await session.close()


async def cancel_msg2(session, client_id):
    try:
        client = await session.execute(
            text("SELECT status_id FROM clients WHERE id = :client_id", {'client_id': client_id}))
        status_id = client.scalar()

        if status_id:
            msg2_exists = await session.execute(text("SELECT id FROM msg2 WHERE client_id = :client_id",
                                                     {'client_id': client_id}))
            if msg2_exists.scalar():
                await session.execute(text("INSERT INTO msg3 (client_id, data) VALUES (:client_id, :current_time)",
                                           {'client_id': client_id, 'current_time': datetime.now()}))
                await session.execute(text("DELETE FROM msg2 WHERE client_id = :client_id", {'client_id': client_id}))
                await session.commit()
    finally:
        await session.close()


async def cancel(session, client_id):
    try:
        client = await session.execute(
            text("SELECT status_id FROM clients WHERE id = :client_id", {'client_id': client_id}))
        status_id = client.scalar()

        if status_id:
            msg2_exists = await session.execute(text("SELECT id FROM msg2 WHERE client_id = :client_id",
                                                     {'client_id': client_id}))
            if msg2_exists.scalar():
                await session.execute(text(
                    f""" UPDATE clients 
                SET status_id = (SELECT id FROM status WHERE status = 'finished') 
                WHERE id = :client_id
                """,
                    {'client_id': client_id}
                ))
                await session.execute(text("DELETE FROM msg2 WHERE client_id = :client_id", {'client_id': client_id}))
                await session.commit()
    finally:
        await session.close()


async def run():
    session = await create_session()
    try:
        await asyncio.gather(
            send_msg1(),
            send_msg2(),
            send_msg3()
        )
    finally:
        await session.close()
