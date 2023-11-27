from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound

from . import models, schemas
from datetime import datetime

def create_user(db: Session, username):
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
    if (user):
        if (user.credits - price < 0):
            db.query(models.User).filter(models.User.id == username).update({'credits': user.credits - price})
            db.commit()
            return True # payment success
        else:
            return False