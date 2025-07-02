# models.py
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Float, Date, Text, ForeignKey

Base = declarative_base()

class DimDate(Base):
    __tablename__ = 'dim_date'
    date_key = Column(Integer, primary_key=True)
    full_date = Column(Date, nullable=False)
    rental_month = Column(Integer, nullable=False)
    rental_week = Column(Integer, nullable=False)

class DimStore(Base):
    __tablename__ = 'dim_store'
    store_key = Column(Integer, primary_key=True)
    address = Column(String(50), nullable=False)
    city = Column(String(50), nullable=False)
    state = Column(String(50), nullable=False)
    country = Column(String(50), nullable=False)

class DimFilm(Base):
    __tablename__ = 'dim_film'
    film_key = Column(Integer, primary_key=True)
    title = Column(String(200), nullable=False)
    description = Column(Text, nullable=False)
    length = Column(Integer)
    category = Column(String(50))

class DimClient(Base):
    __tablename__ = 'dim_client'
    customer_key = Column(Integer, primary_key=True)
    store_id = Column(Integer)
    full_name = Column(String(100))
    email = Column(String(100), nullable=False)
    city = Column(String(50))
    state = Column(String(50))
    country = Column(String(50))


class FactRental(Base):
    __tablename__ = 'fact_rental'
    rental_key = Column(Integer, primary_key=True, autoincrement=True)
    customer_key = Column(Integer, ForeignKey('dim_client.customer_key'))
    film_key = Column(Integer, ForeignKey('dim_film.film_key'))
    store_key = Column(Integer, ForeignKey('dim_store.store_key'))
    date_key = Column(Integer, ForeignKey('dim_date.date_key'))
    amount = Column(Float, nullable=False)

    customer = relationship("DimClient")
    film = relationship("DimFilm")
    store = relationship("DimStore")
    date = relationship("DimDate")