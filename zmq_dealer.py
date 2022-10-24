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
    ad = get_default_adapter()
    socket = ad._get_or_make_socket(zmq.DEALER)
    await socket.send_multipart([b"la YOU"])
    sb = await socket.recv_multipart()
    print(sb)



if __name__ == "__main__":
    asyncio.run(fuck())
