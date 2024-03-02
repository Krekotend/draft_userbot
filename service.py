import asyncio
import sql_service
import logging
import main

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

async def process_msg1():
    logging.info(f"Processing msg1 statred")
    while True:
        id_list = await sql_service.send_msg1()
        if id_list:
            logging.info(f"Message msg1 sent")
            for id in id_list:
                await main.send_message1(id)


async def process_msg2():
    logging.info(f"Processing msg2 statred")
    while True:
        id_list = await sql_service.send_msg2()
        if id_list:
            logging.info(f"Message msg2 sent")
            for id in id_list:
                await main.send_message2(id)
        await asyncio.sleep(1)

async def process_msg3():
    logging.info(f"Processing msg3 statred")
    while True:
        id_list = await sql_service.send_msg3()
        if id_list:
            logging.info(f"Message msg3 sent")
            for id in id_list:
                await main.send_message3(id)
        await asyncio.sleep(1)


async def run():
    logging.info("Starting the event loop")
    while True:
        await asyncio.gather(
            sql_service.run(),
            process_msg1(),
            process_msg2(),
            process_msg3()
        )

if __name__ == "__main__":
    asyncio.run(run())
