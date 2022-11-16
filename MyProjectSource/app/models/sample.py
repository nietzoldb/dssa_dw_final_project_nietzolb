from sqlalchemy import Column, Integer, String, ForeignKey, VARCHAR, Numeric, DateTime
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

# Declarative Base
# https://docs.sqlalchemy.org/en/14/orm/declarative_tables.html

# Relationships
# https://docs.sqlalchemy.org/en/14/orm/basic_relationships.html#one-to-many
Base = declarative_base()

class Customer(Base):
    __table_args__ = {"schema": "prof"}
    __tablename__ = 'dim_customer'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    address = Column(VARCHAR(length=50))
    zip = Column(VARCHAR(length=10))
    city = Column(VARCHAR(length=50))
    country = Column(VARCHAR(length=50))
    payments = relationship("prof.dim_payments", back_populates = "prof.dim_customer")

class Payments(Base):
    __table_args__ = {"schema": "prof"}
    __tablename__ = 'dim_payments'

    payment_id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('prof.dim_customer.id'))
    amount = Column(Numeric(precision=5,scale=2))
    payment_date = Column(DateTime)
    customer = relationship("prof.dim_customer", back_populates = "prof.dim_payments")

class MonthlyRevenue(Base):
    __table_args__ = {"schema": "prof"}
    __tablename__ = "monthly_revenue"

    id = Column(Integer(), primary_key=True)
    customer_id = Column(Integer, ForeignKey('prof.dim_customer.id'))
    year = Column(Integer)
    month = Column(Integer)
    total_sales = Column(Numeric(precision=5,scale=2))
    customer = relationship("prof.dim_customer", back_populates = "prof.monthly_revenue")