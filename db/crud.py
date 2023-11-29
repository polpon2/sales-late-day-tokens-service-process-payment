from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.expression import func
from sqlalchemy.exc import NoResultFound
from . import models

async def create_user(db: AsyncSession, username: str, init_credits: int):
    user = await get_user_by_username(db=db, username=username)
    if user:
        return user
    else:
        db_user = models.User(username=username, credits=init_credits)
        db.add(db_user)
        await db.flush()
        return db_user

async def get_user_by_username(db: AsyncSession, username: str):
    result = await db.execute(models.User.__table__.select().where(models.User.username == username))
    if result is not None:
        return result.fetchone()
    return None

async def get_user(db: AsyncSession, user_id: int):
    result = await db.execute(models.User.__table__.select().where(models.User.id == user_id))
    return await result.scalar() if result else result

async def get_token_by_name(db: AsyncSession, token_name: str):
    result = await db.execute(models.Token.__table__.select().where(models.Token.token_name == token_name))
    if result is not None:
        return result.scalar()
    return result

async def process_payment(db: AsyncSession, username: str, price: int):
    result = await db.execute(models.User.__table__.select().where(models.User.username == username))
    user = result.fetchone()
    print(f'user: {user.username}')
    if user:
        print(f"user credit: {user.credits}")
        print(f"price: {price}")
        if user.credits - price >= 0:
            print(f"remaining credits: {user.credits - price}")
            await db.execute(models.User.__table__.update().where(models.User.username == username).values({'credits': user.credits - price}))
            # await db.flush()
            return True  # payment success
        return False
    return False

async def create_token(db: AsyncSession, token_name: str, price: str):
    result = await db.execute(models.Token.__table__.select().where(models.Token.token_name == token_name))
    token = await result.scalar()
    if not token:
        db_token = models.Token(token_name=token_name, price=price)
        async with db.begin():
            db.add(db_token)
            await db.flush()
        return db_token

async def init_token(db: AsyncSession):
    for i in range(10):
        await create_token(db=db, token_name="a" + str(i), price=str(i + 1))
