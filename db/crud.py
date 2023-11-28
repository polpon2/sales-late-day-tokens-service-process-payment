from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound

from . import models


def create_user(db: Session, username: str):
    user = get_user_by_username(db=db, username=username)
    if (user):
        return user
    else:
        db_user = models.User(username=username, credits=100)
        db.add(db_user)
        db.commit()
        db.refresh(db_user)
        return db_user

def get_user_by_username(db: Session, username: str):
    return db.query(models.User).filter(models.User.username == username).first()


def get_user(db: Session, user_id: int):
    return db.query(models.User).filter(models.User.id == user_id).first()

def get_token_by_name(db: Session, token_name: str):
    return db.query(models.Token).filter(models.Token.token_name == token_name).first()

def process_payment(db: Session, username: str, price: int):
    user = db.query(models.User).filter(models.User.username == username).first()
    print(f'user: {user.username}')
    if (user):
        print(f"user credit: {user.credits}")
        print(f"price: {price}")
        if (user.credits - price > 0):
            print(f"remaind credits: {user.credits - price}")
            db.query(models.User).filter(models.User.username == username).update({'credits': user.credits - price})
            db.commit()
            return True # payment success
        return False
    return False


def create_token(db: Session, token_name: str, price: str):
    token = get_token_by_name(db=db, token_name=token_name)
    if (token is None):
        db_token = models.Token(token_name=token_name, price=price)
        db.add(db_token)
        db.commit()
        db.refresh(db_token)
        return db_token


def init_token(db: Session):
    for i in range(10):
        create_token(db=db, token_name="a" + i, price= i + 1)
