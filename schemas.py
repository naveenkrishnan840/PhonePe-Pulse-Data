from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, INTEGER, String, ForeignKey, DECIMAL, Float


Base = declarative_base()


class States(Base):
    __tablename__ = "tbl_states"
    state_id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_name = Column(String(50), nullable=False)


class Districts(Base):
    __tablename__ = "tbl_districts"
    district_id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    district_name = Column(String(50), nullable=False)


class AggregatedTransaction(Base):
    __tablename__ = "tbl_aggregated_transaction"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    transaction_type = Column(String(50), nullable=False)
    transaction_count = Column(INTEGER, nullable=False)
    transaction_amount = Column(Float, nullable=False)


class AggregatedInsurance(Base):
    __tablename__ = "tbl_aggregated_insurance"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    insurance_count = Column(INTEGER, nullable=False)
    insurance_amount = Column(Float, nullable=False)


class AggregatedUser(Base):
    __tablename__ = "tbl_aggregated_user"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    brand_type = Column(String(50), nullable=False)
    count = Column(INTEGER, nullable=False)
    percentage = Column(DECIMAL(precision=5, scale=5), nullable=False)


class MapTransaction(Base):
    __tablename__ = "tbl_map_transaction"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    district_id = Column(INTEGER, ForeignKey("tbl_districts.district_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    transaction_count = Column(INTEGER, nullable=False)
    transaction_amount = Column(Float, nullable=False)


class MapInsurance(Base):
    __tablename__ = "tbl_map_insurance"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    district_id = Column(INTEGER, ForeignKey("tbl_districts.district_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    insurance_count = Column(INTEGER, nullable=False)
    insurance_amount = Column(Float, nullable=False)


class MapUser(Base):
    __tablename__ = "tbl_map_user"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    district_id = Column(INTEGER, ForeignKey("tbl_districts.district_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    registered_users = Column(INTEGER, nullable=False)
    app_opens = Column(INTEGER, nullable=False)


class TopTransactionDistricts(Base):
    __tablename__ = "tbl_top_transaction_district"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    district_id = Column(INTEGER, ForeignKey("tbl_districts.district_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    transaction_count = Column(String(50), nullable=False)
    transaction_amount = Column(Float, nullable=False)


class TopTransactionPincode(Base):
    __tablename__ = "tbl_top_transaction_pincode"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    pincode = Column(String(50), nullable=False)
    transaction_count = Column(INTEGER, nullable=False)
    transaction_amount = Column(Float, nullable=False)


class TopInsuranceDistrict(Base):
    __tablename__ = "tbl_top_insurance_district"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    district_id = Column(INTEGER, ForeignKey("tbl_districts.district_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    insurance_count = Column(INTEGER, nullable=False)
    insurance_amount = Column(Float, nullable=False)


class TopInsurancePincode(Base):
    __tablename__ = "tbl_top_insurance_pincode"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    pincode = Column(String(50), nullable=False)
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    insurance_count = Column(INTEGER, nullable=False)
    insurance_amount = Column(Float, nullable=False)


class TopUserDistricts(Base):
    __tablename__ = "tbl_top_user_district"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    district_id = Column(INTEGER, ForeignKey("tbl_districts.district_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    registered_users = Column(INTEGER, nullable=False)


class TopUserPincode(Base):
    __tablename__ = "tbl_top_user_pincode"
    id = Column(INTEGER, unique=True, primary_key=True, autoincrement=True)
    state_id = Column(INTEGER, ForeignKey("tbl_states.state_id"))
    year = Column(INTEGER, nullable=False)
    quarter = Column(INTEGER, nullable=False)
    pincode = Column(String(50), nullable=False)
    registered_users = Column(INTEGER, nullable=False)


