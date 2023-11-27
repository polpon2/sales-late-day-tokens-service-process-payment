from typing import List
import pika, sys, os, json
from dotenv import load_dotenv

import os
from requests import Session

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine

from db import crud, models
from db.engine import SessionLocal, engine



load_dotenv()
models.Base.metadata.create_all(bind=engine)


def callback(ch, method, properties, body):
    body: dict = json.loads(body)

    username: str = body['username']
    token_name: str = body['token_name']
    amount: int = body['amount']

    print(f" [x] Received {body}")

    # Create payment.
    db: Session = SessionLocal()
    token = crud.get_token_by_name(db=db, token_name=token_name)
    user = crud.get_user_by_username(db=db, username=username)
    if (user):
        crud.create_user(db=db, username=username)
    if (token):
        is_success = crud.process_payment(db=db, username=username, price=token.price * amount)
        if (is_success):
            print(f"create payment")
        else:
            print("Roll back")
    else:
        print("Roll back")
    

    ch.queue_declare(queue='from.payment')

    ch.basic_publish(exchange='',
                        routing_key='from.payment',
                        body=json.dumps(body))

    print(f" [x] Sent {json.dumps(body)}")

    ch.basic_ack(delivery_tag=method.delivery_tag)

    return


def main():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbit-mq', port=5672))
    except:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))

    channel = connection.channel()

    channel.queue_declare(queue='to.payment', arguments={
                          'x-message-ttl' : 1000,
                          'x-dead-letter-exchange' : 'dlx',
                          'x-dead-letter-routing-key' : 'dl'
                          })

    channel.basic_consume(queue='to.payment', on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)