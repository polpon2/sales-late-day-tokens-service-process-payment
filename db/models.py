from sqlalchemy import Column, Integer, String

from .engine import Base

class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), unique=True, index=True)
    credits = Column(Integer, default=5)

    def __repr__(self):
        return f"<User(username={self.username}, credit={self.credits})>"


