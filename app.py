import asyncio, aio_pika, json
from async_timeout import timeout
from asyncio import TimeoutError
from db.engine import SessionLocal, engine
from db import crud, models

async def process_message(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        try:
            async with timeout(1.5):
                body: dict = json.loads(message.body)

                username: str = body['username']
                amount: int = body['amount']
                price: int = body['price']

                print(f" [x] Received {body}")

                # Create Payment.
                async with SessionLocal() as db:
                    try:
                        user = await crud.create_user(db=db, username=username, init_credits=500)
                    except Exception as e:
                        print(e)

                    if (user.credits - amount * price < 0):
                        # Roll Back from Payment (INSUFFICIENT_FUND)
                        await process_rb_status(message=message, connection=connection, status="INSUFFICIENT_FUND")
                        print("Roll Back")
                        return
                    else:
                        is_success = await crud.process_payment(db=db, username=user.username, price=price, amount=amount)

                    if (is_success):
                        routing_key = "from.payment"

                        channel = await connection.channel()

                        await channel.default_exchange.publish(
                            aio_pika.Message(body=message.body),
                            routing_key=routing_key,
                        )
                        await db.commit()
                    else:
                        # Roll Back from Payment
                        await process_rb_status(message=message, connection=connection)
                        print("Roll Back")
                        return
        except TimeoutError:
            # Roll Back from Timed Out
            await process_rb_status(message=message, connection=connection, status="TIMEOUT")
            print("Timed Out Rolling Back....")
        except Exception as e:
            await process_rb_status(message=message, connection=connection)
            print(f"Error: {e}, Rolling Back...")

async def process_rb(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
) -> None:
    async with message.process():
        body: dict = json.loads(message.body)

        username: str = body["username"]
        price: int = body["price"]
        amount: int = body["amount"]
        price_taken: int = price * amount

        print(f" [x] Rolling Back {body}")

        async with SessionLocal() as db:
            is_done = await crud.change_money(db, username=username, price_taken=price_taken)
            if is_done:
                channel = await connection.channel()

                await channel.default_exchange.publish(
                    aio_pika.Message(body=message.body),
                    routing_key="rb.order",
                )

                await db.commit()
            else:
                print("GG[1]")



async def process_rb_status(
    message: aio_pika.abc.AbstractIncomingMessage,
    connection: aio_pika.Connection,  # Add connection parameter
    status: str | None = None,
) -> None:
    body: dict = json.loads(message.body)

    print(f" [x] Rolling Back {body}")

    if status is not None:
        body["status"] = status

    # Could be from Delivery dying or Insufficient money
    channel = await connection.channel()

    await channel.default_exchange.publish(
        aio_pika.Message(body=bytes(json.dumps(body), 'utf-8')),
        routing_key="rb.order",
    )


async def main() -> None:
    connection = await aio_pika.connect_robust(
        "amqp://rabbit-mq",
    )

    # Init the tables in db
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.drop_all) # Reset every time
        await conn.run_sync(models.Base.metadata.create_all)


    queue_name = "to.payment"

    # Creating channel
    channel = await connection.channel()

    # Maximum message count which will be processing at the same time.
    await channel.set_qos(prefetch_count=10)

    # Declaring queue
    queue = await channel.declare_queue(queue_name, arguments={
                                                    'x-message-ttl' : 1000,
                                                    'x-dead-letter-exchange' : 'dlx',
                                                    'x-dead-letter-routing-key' : 'dl'
                                                    })
    queue_rb = await channel.declare_queue("rb.payment");

    print(' [*] Waiting for messages. To exit press CTRL+C')

    await queue.consume(lambda message: process_message(message, connection))
    await queue_rb.consume(lambda message: process_rb(message, connection))

    try:
        # Wait until terminate
        await asyncio.Future()
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())