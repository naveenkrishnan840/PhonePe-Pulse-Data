import streamlit as st
from database_connection import BuildDataBase
from schemas import *
import pandas as pd
from sqlalchemy import func, case, desc
import plotly.express as px

select_option = st.selectbox(label="Queries", options=(
    ["Select",
     "How to find maximum transaction Count & Transaction Amount based on corresponding year in the "
     "TamilNadu State for Categories ?",
     "Which Mobile Brand have Lowest No Of Sales & highest percentage for corresponding quarter?",
     "What is the Average Insurance Premium for corresponding States?",
     "How to find Number of transaction happens for each State & Year & Quarter?",
     "What is the Maximum registered users & app opens there for corresponding State & Year?",
     "How munch insurance premium there for each quarter?",
     "What are top 10 districts have maximum transaction amount for corresponding districts?",
     "What are the total number of pincode for each quarter?",
     "What are the top 10 pincode have maximum registered users for each state & pincode?",
     "What are the top three districts have average insurance premium for each district in the maharashtra?"
     ]), placeholder="Select The Query")


if select_option != "Select":
    connection_object = BuildDataBase(queries_action=True)
    df = pd.DataFrame()
    single_title = None
    single_tab = False
    if select_option == ("How to find maximum transaction Count & Transaction Amount based on corresponding year "
                         "in the TamilNadu State for Categories ?"):
        df = pd.DataFrame((connection_object.connection.session.
                           query(AggregatedTransaction.year.cast(String),
                                 case((AggregatedTransaction.quarter == 1, "Q1 (Jan - Mar)"),
                                      (AggregatedTransaction.quarter == 2, "Q2 (Apr - Jun)"),
                                      (AggregatedTransaction.quarter == 3, "Q3 (Jul - Sep)"), else_="Q4 (Oct - Dec)").
                                 label("Quarter"),
                                 AggregatedTransaction.transaction_type,
                                 func.max(AggregatedTransaction.transaction_count),
                                 func.max(AggregatedTransaction.transaction_amount)).join(States).
                           filter(States.state_name == "tamil-nadu").group_by(AggregatedTransaction.year)),
                          columns=["Year", "Quarter", "Transaction Type", "Transaction Count", "Transaction Amount"])

    elif select_option == "Which Mobile Brand have Lowest No Of Sales & highest percentage for corresponding quarter?":

        df = pd.DataFrame(connection_object.connection.session.
                          query(AggregatedUser.year.cast(String), AggregatedUser.brand_type,
                                func.min(AggregatedUser.count),
                                func.concat(func.round(func.max(AggregatedUser.percentage) * 100), '', "%")).
                          group_by(AggregatedUser.quarter).order_by(AggregatedUser.year),
                          columns=["Year", "Brand", "Lowest No Of Sales", "Highest Percentage"])

    elif select_option == "What is the Average Insurance Premium for corresponding States?":

        df = pd.DataFrame(connection_object.connection.session.
                          query(States.state_name, func.round(func.avg(AggregatedInsurance.insurance_amount))).
                          join(States).group_by(AggregatedInsurance.state_id).order_by(States.state_name),
                          columns=["State", "Average Insurance Premium"])
        single_title = "State"
        single_tab = False

    elif select_option == "How to find Number of transaction happens for each State & Year & Quarter?":
        df = pd.DataFrame(connection_object.connection.session.
                          query(States.state_name, MapTransaction.year.cast(String),
                                case((MapTransaction.quarter == 1, "Q1 (Jan - Mar)"),
                                     (MapTransaction.quarter == 2, "Q2 (Apr - Jun)"),
                                     (MapTransaction.quarter == 3, "Q3 (Jul - Sep)"), else_="Q4 (Oct - Dec)").
                                label("Quarter"),
                                func.sum(MapTransaction.transaction_count)).
                          join(States).
                          group_by(MapTransaction.state_id, MapTransaction.year, MapTransaction.quarter).
                          order_by(States.state_name, MapTransaction.year, MapTransaction.quarter),
                          columns=["State", "Year", "Quarter", "Total Transaction"])
        single_title = "State"

    elif select_option == "What is the Maximum registered users & app opens there for corresponding State & Year?":
        df = pd.DataFrame(connection_object.connection.session.
                          query(States.state_name, MapUser.year.cast(String), func.max(MapUser.registered_users),
                                func.max(MapUser.app_opens)).
                          join(States).
                          group_by(MapUser.state_id, MapUser.year).
                          order_by(States.state_name, MapUser.year),
                          columns=["State", "Year", "Maximum Registered Users", "Maximum App Opens"])
        single_title = "State"

    elif select_option == "How munch insurance premium there for each quarter?":
        df = pd.DataFrame(connection_object.connection.session.
                          query(case((AggregatedInsurance.quarter == 1, "Q1 (Jan - Mar)"),
                                     (AggregatedInsurance.quarter == 2, "Q2 (Apr - Jun)"),
                                     (AggregatedInsurance.quarter == 3, "Q3 (Jul - Sep)"), else_="Q4 (Oct - Dec)").
                                label("Quarter"), func.sum(AggregatedInsurance.insurance_amount)).
                          group_by(AggregatedInsurance.quarter).
                          order_by(AggregatedInsurance.quarter),
                          columns=["Quarter", "Insurance Premium"])
        single_tab = False
    elif select_option == "What are top 10 districts have maximum transaction amount for corresponding districts?":
        df = pd.DataFrame(connection_object.connection.session.
                          query(Districts.district_name,
                                func.max(TopTransactionDistricts.transaction_amount).label("amount")).
                          join(Districts).
                          group_by(TopTransactionDistricts.district_id).
                          order_by(desc("amount")).limit(10),
                          columns=["District", "Maximum Transaction Amount"])
        single_title = "District"
        single_tab = False

    elif select_option == "What are the total number of pincode for each quarter?":
        df = pd.DataFrame(connection_object.connection.session.
                          query(case((TopTransactionPincode.quarter == 1, "Q1 (Jan - Mar)"),
                                     (TopTransactionPincode.quarter == 2, "Q2 (Apr - Jun)"),
                                     (TopTransactionPincode.quarter == 3, "Q3 (Jul - Sep)"), else_="Q4 (Oct - Dec)").
                                label("Quarter"), func.count(TopTransactionPincode.pincode)).
                          group_by("Quarter").
                          order_by("Quarter"), columns=["Quarter", "Pincode"])
        single_tab = False

    elif select_option == "What are the top 10 pincode have maximum registered users for each state & pincode?":
        df = pd.DataFrame(connection_object.connection.session.
                          query(States.state_name, TopUserPincode.pincode,
                                func.max(TopUserPincode.registered_users).label("max_reg_users")).
                          join(States).
                          group_by(TopUserPincode.state_id, TopUserPincode.pincode).
                          order_by(desc("max_reg_users")).limit(10),
                          columns=["State", "Pincode", "Maximum Registered Users"])
        single_title = "State"

    elif select_option == ("What are the top three districts have average insurance premium for each district "
                           "in the maharashtra?"):
        df = pd.DataFrame(connection_object.connection.session.
                          query(Districts.district_name,
                                func.round(func.avg(TopInsuranceDistrict.insurance_amount)).label("average")).
                          join(Districts, Districts.district_id == TopInsuranceDistrict.district_id).
                          join(States, States.state_id == TopInsuranceDistrict.state_id).
                          filter(States.state_name == "maharashtra").
                          group_by(TopInsuranceDistrict.district_id).order_by(desc("average")).limit(3),
                          columns=["District", "Average Insurance Premium"])
        single_title = "District"
        single_tab = False

    if single_title == "State":
        df["State"] = df["State"].replace(to_replace=r'[-&--]', value=' ', regex=True
                                          ).replace(to_replace=r'\s+', value=' ',
                                                    regex=True).str.title()
    elif single_title == "District":
        df["District"] = df["District"].str.title()

    if single_tab:
        st.dataframe(df, hide_index=True, use_container_width=True)
    else:
        tab = st.tabs(["Dataframe", "Plots"])
        with tab[0]:
            st.dataframe(df, hide_index=True, use_container_width=True)
        with tab[1]:
            st.plotly_chart(px.bar(df, x=df.columns[0], y=df.columns[1]))

