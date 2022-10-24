from datetime import (
    datetime,
)
import logging

import asyncio
import site
import zmq
import zmq.asyncio
from data_builder.messaging.adapters.async_default import get_default_adapter

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


async def fuck():
    print("start")
    while 1:
        ad = get_default_adapter()
        socket = ad._get_or_make_socket(zmq.ROUTER)
        _id, sb=await socket.recv_multipart()
        print(sb)
        await socket.send_multipart([_id, b"la me"])


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    loop.create_task(fuck())
    try:
        loop.run_forever()
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
