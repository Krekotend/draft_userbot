import asyncio
import sql_service
import main


async def process_msg1():
    while True:
        id_list = await sql_service.send_msg1()
        if id_list:
            print(f"Processing msg1...")
            for id in id_list:
                await main.send_message1(id=id)
        await asyncio.sleep(1)

async def process_msg2():
    while True:
        id_list = await sql_service.send_msg2()
        if id_list:
            print(f"Processing msg2...")
            for id in id_list:
                await main.send_message2(id=id)
        await asyncio.sleep(1)

async def process_msg3():
    while True:
        id_list = await sql_service.send_msg3()
        if id_list:
            print(f"Processing msg3...")
            for id in id_list:
                await main.send_message3(id=id)
        await asyncio.sleep(1)


async def run():
    await asyncio.gather(
        process_msg1(),
        process_msg2(),
        process_msg3()
    )

if __name__ == "__main__":
    asyncio.run(run())







