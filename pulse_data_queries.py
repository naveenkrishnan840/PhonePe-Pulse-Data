from sqlalchemy import func
from schemas import *
import pandas as pd
from sqlalchemy import desc


def get_all_records_from_all_tables(database, year, quarter, action_type):
    if action_type == "Transaction":
        return (
            # map transaction for each states
            pd.DataFrame(database.session.
                         query(States.state_name.label("State"),
                               func.sum(MapTransaction.transaction_count).label("Transaction Count").cast(INTEGER),
                               func.sum(MapTransaction.transaction_amount).label("Total Payment").cast(INTEGER),
                               func.avg(MapTransaction.transaction_amount).label("Average Payment").cast(INTEGER)).
                         join(States, (MapTransaction.state_id == States.state_id)).
                         group_by(MapTransaction.year, MapTransaction.quarter, MapTransaction.state_id).
                         filter(MapTransaction.year == year, MapTransaction.quarter == quarter),
                         columns=["State", "Transaction Count", "Total Payment", "Average Payment"]).
            style.format({"Transaction Count": "{:,d}"}).data,

            # Total Transaction
            pd.DataFrame(database.connection.session.
                         query(func.sum(AggregatedTransaction.transaction_count).cast(INTEGER),
                               func.avg(AggregatedTransaction.transaction_amount).label("avg_amount").cast(INTEGER),
                               func.sum(AggregatedTransaction.transaction_amount)).group_by("year", "quarter").
                         filter(AggregatedTransaction.year == year,
                                AggregatedTransaction.quarter == quarter),
                         columns=["transaction_count", "avg_amount", "transaction_amount"]),
            # Categories
            pd.DataFrame(database.connection.session.
                         query(AggregatedTransaction.transaction_type.label("Category"),
                               func.sum(AggregatedTransaction.transaction_count).cast(INTEGER).label("Total Payment")).
                         group_by("year", "quarter", "transaction_type").
                         filter(AggregatedTransaction.year == year, AggregatedTransaction.quarter == quarter),
                         columns=["Category", "Total Payment"]),
            # Top 10 States
            pd.DataFrame(database.connection.session.
                         query(States.state_name.label("States"),
                               func.sum(TopTransactionDistricts.transaction_count).label("Total Transaction")).
                         join(States, (TopTransactionDistricts.state_id == States.state_id)).
                         group_by(TopTransactionDistricts.year, TopTransactionDistricts.quarter,
                                  TopTransactionDistricts.state_id).
                         filter(TopTransactionDistricts.year == year,
                                TopTransactionDistricts.quarter == quarter).order_by(desc("Total Transaction")).
                         limit(10), columns=["States", "Total Transaction"]),

            # Top 10 Districts
            pd.DataFrame(database.connection.session.
                         query(Districts.district_name.label("Districts"),
                               func.sum(TopTransactionDistricts.transaction_count).label("Total Transaction")).
                         join(Districts, (TopTransactionDistricts.district_id == Districts.district_id)).
                         group_by(TopTransactionDistricts.year, TopTransactionDistricts.quarter,
                                  TopTransactionDistricts.district_id).
                         filter(TopTransactionDistricts.year == year,
                                TopTransactionDistricts.quarter == quarter).
                         order_by(desc("Total Transaction")).limit(10), columns=["Districts", "Total Transaction"]),

            # Top 10 Pincode
            pd.DataFrame(database.connection.session.
                         query(TopTransactionPincode.pincode.label("PinCode"),
                               func.sum(TopTransactionPincode.transaction_count).label("Total Transaction")).
                         group_by(TopTransactionPincode.year, TopTransactionPincode.quarter,
                                  TopTransactionPincode.pincode).
                         filter(TopTransactionPincode.year == year,
                                TopTransactionPincode.quarter == quarter).
                         order_by(desc("Total Transaction")).limit(10), columns=["PinCode", "Total Transaction"]))
    elif action_type == "Users":
        return (
            # EACH STATE REGISTERED USER
            pd.DataFrame(database.connection.session.
                         query(States.state_name, func.sum(MapUser.registered_users),
                               func.sum(MapUser.app_opens)).
                         join(States, (MapUser.state_id == States.state_id)).
                         group_by(MapUser.year, MapUser.quarter, MapUser.state_id).
                         filter(MapUser.year == year, MapUser.quarter == quarter),
                         columns=["State", "Registered Users", "App Opens"]),
            # ALL REGISTERED USER
            pd.DataFrame(database.connection.session.query(func.sum(MapUser.registered_users),
                                                           func.avg(MapUser.app_opens)).
                         group_by("year", "quarter").filter(MapUser.year == year, MapUser.quarter == quarter),
                         columns=["registered_users", "app_opens"]),
            # Mobile user
            pd.DataFrame(database.connection.session.
                         query(AggregatedUser.brand_type, func.sum(AggregatedUser.count).label("Total Users"),
                               func.concat(func.round(func.avg(AggregatedUser.percentage) * 100), '', "%")).
                         group_by("year", "quarter", "brand_type").filter(AggregatedUser.year == year,
                                                                          AggregatedUser.quarter == quarter).
                         order_by(desc("Total Users")),
                         columns=["Device", "Total Users", "Percentage"]),
            # top 10 state registered users
            pd.DataFrame(database.connection.session.
                         query(States.state_name, func.sum(TopUserDistricts.registered_users)).
                         join(States, (States.state_id == TopUserDistricts.state_id)).
                         group_by(TopUserDistricts.year, TopUserDistricts.quarter, TopUserDistricts.state_id).
                         filter(TopUserDistricts.year == year, TopUserDistricts.quarter == quarter).
                         order_by(desc("registered_users")).limit(10), columns=["States", "Registered Users"]),
            # top 10 districts registered users
            pd.DataFrame(database.connection.session.
                         query(Districts.district_name, func.sum(TopUserDistricts.registered_users)).
                         join(Districts, (Districts.district_id == TopUserDistricts.district_id)).
                         group_by(TopUserDistricts.year, TopUserDistricts.quarter, TopUserDistricts.district_id).
                         filter(TopUserDistricts.year == year, TopUserDistricts.quarter == quarter).
                         order_by(desc("registered_users")).limit(10), columns=["Districts", "Registered Users"]),
            # top 10 pincode registered users
            pd.DataFrame(database.connection.session.
                         query(TopUserPincode.pincode, func.sum(TopUserPincode.registered_users)).
                         group_by("year", "quarter", "pincode").
                         filter(TopUserPincode.year == year, TopUserPincode.quarter == quarter).
                         order_by(desc(TopUserPincode.pincode)).limit(10), columns=["Pincode", "Registered Users"])
        )
    else:
        return (
            # Each sate wise insurance
            pd.DataFrame(database.connection.session.
                         query(States.state_name,
                               func.sum(MapInsurance.insurance_count).label("no_polcies"),
                               func.sum(MapInsurance.insurance_amount).label("total_preium"),
                               func.round(func.avg(MapInsurance.insurance_amount).label("avg"))).
                         join(States, (States.state_id == MapInsurance.state_id)).
                         group_by("year", "quarter", "state_id").
                         filter(MapInsurance.year == year, MapInsurance.quarter == quarter),
                         columns=["State", "No Of Policies", "Total Premium", "Average Premium"]),
            # Total Insurance
            pd.DataFrame(database.connection.session.
                         query(func.sum(AggregatedInsurance.insurance_count).label("noofpolicies"),
                               func.sum(AggregatedInsurance.insurance_amount.label("total_premium")),
                               func.avg(AggregatedInsurance.insurance_amount).label("avg")).
                         group_by("year", "quarter").
                         filter(AggregatedInsurance.year == year, AggregatedInsurance.quarter == quarter),
                         columns=["no_of_policies", "total_premium", 'avg_premium']),
            # top 10 state
            pd.DataFrame(database.connection.session.
                         query(States.state_name,
                               func.sum(TopInsuranceDistrict.insurance_count).label("total_preium")).
                         join(States, (States.state_id == TopInsuranceDistrict.state_id)).
                         group_by("year", "quarter", "state_id").
                         filter(TopInsuranceDistrict.year == year, TopInsuranceDistrict.quarter == quarter).
                         order_by(desc("total_preium")).limit(10),
                         columns=["States", "Total Premium"]),
            # top 10 districts
            pd.DataFrame(database.connection.session.
                         query(Districts.district_name,
                               func.sum(TopInsuranceDistrict.insurance_count).label("total_preium")).
                         join(Districts, (Districts.district_id == TopInsuranceDistrict.district_id)).
                         group_by("year", "quarter", "district_id").
                         filter(TopInsuranceDistrict.year == year, TopInsuranceDistrict.quarter == quarter).
                         order_by(desc("total_preium")).limit(10),
                         columns=["Districts", "Total Premium"]),
            # top 10 pincode
            pd.DataFrame(database.connection.session.
                         query(TopInsurancePincode.pincode,
                               func.sum(TopInsurancePincode.insurance_count).label("total_preium")).
                         group_by("year", "quarter", "pincode").
                         filter(TopInsurancePincode.year == year, TopInsurancePincode.quarter == quarter).
                         order_by(desc("total_preium")).limit(10),
                         columns=["pincode", "Total Premium"])
        )
