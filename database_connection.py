import streamlit as st
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from typing import Any
from schemas import *


class BuildDataBase:
    def __init__(self, extract_data):
        self.connection = st.connection(name="local_db", type="sql",
                                        url="mysql+pymysql://root:test@localhost:3306/phone_pe_pulse_data")
        self.session = self.connection.session
        # self.drop_tables()
        # Base.metadata.create_all(self.connection.engine)
        # self.insert_records(extract_data)

    def drop_tables(self):
        self.session.begin()
        self.session.execute(text("drop table if exists tbl_top_insurance"))
        self.session.execute(text("drop table if exists tbl_top_user_pincode"))
        self.session.execute(text("drop table if exists tbl_top_user_district"))
        self.session.execute(text("drop table if exists tbl_top_transaction_pincode"))
        self.session.execute(text("drop table if exists tbl_top_transaction_district"))
        self.session.execute(text("drop table if exists tbl_map_user"))
        self.session.execute(text("drop table if exists tbl_map_insurance"))
        self.session.execute(text("drop table if exists tbl_aggregated_transaction"))
        self.session.execute(text("drop table if exists tbl_aggregated_insurance"))
        self.session.execute(text("drop table if exists tbl_aggregated_user"))
        self.session.execute(text("drop table if exists tbl_map_transaction"))
        self.session.execute(text("drop table if exists tbl_states"))
        self.session.execute(text("drop table if exists tbl_districts"))
        # self.session.commit()
        self.session.close()

    def insert_records(self, extract_data: Any):
        with self.session as session:
            try:
                session.begin()
                session.bulk_insert_mappings(States, extract_data.state_df.to_dict(orient="records"))
                session.bulk_insert_mappings(Districts, extract_data.district_df.to_dict(orient="records"))
                session.bulk_insert_mappings(AggregatedTransaction,
                                             extract_data.agg_transaction_df.to_dict(orient="records"))
                session.bulk_insert_mappings(AggregatedInsurance,
                                             extract_data.agg_insurance_df.to_dict(orient="records"))
                session.bulk_insert_mappings(AggregatedUser, extract_data.agg_user_brand_df.to_dict(orient="records"))
                session.bulk_insert_mappings(MapTransaction, extract_data.map_transaction_df.to_dict(orient="records"))
                session.bulk_insert_mappings(MapInsurance, extract_data.insurance_map_df.to_dict(orient="records"))
                session.bulk_insert_mappings(MapUser, extract_data.map_user_df.to_dict(orient="records"))
                session.bulk_insert_mappings(TopTransactionDistricts,
                                             extract_data.top_transaction_district_df.to_dict(orient="records"))
                session.bulk_insert_mappings(TopTransactionPincode,
                                             extract_data.top_transaction_pincode_df.to_dict(orient="records"))
                session.bulk_insert_mappings(TopInsuranceDistrict,
                                             extract_data.top_insurance_district_df.to_dict(orient="records"))
                session.bulk_insert_mappings(TopInsurancePincode,
                                             extract_data.top_insurance_pincode_df.to_dict(orient="records"))
                session.bulk_insert_mappings(TopUserDistricts,
                                             extract_data.top_user_district_df.to_dict(orient="records"))
                session.bulk_insert_mappings(TopUserPincode, extract_data.top_user_pincode_df.to_dict(orient="records"))
                session.commit()
                session.close()
            except SQLAlchemyError as e:
                session.rollback()
                session.close()
                raise e


# database = BuildDataBase(extract_datas)

