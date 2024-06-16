import streamlit as st
from typing import List, Dict, Any
from sqlalchemy import text
from pulse_data_queries import *
import plotly.express as px
from extraction_data import ExtractionData
from database_connection import BuildDataBase
st.set_page_config(layout="wide")
st.title("Phone Pe Pulse Data Visualization")


@st.cache_resource
def cache_data():
    """
    :return: Cache Database
    """
    # Extraction Data from JSON Format
    extract_data = ExtractionData()

    # Connect Database & Insert Datas
    database = BuildDataBase(extract_data)
    return database


cols = st.columns(3, gap="large")
with cols[0]:
    select_value = st.selectbox(label="", options=("Transaction", "Users", "Insurance"), label_visibility="hidden")
with cols[1]:
    year = st.selectbox('', options=["2024", "2023", "2022", "2021", "2020", "2019", "2018"], label_visibility="hidden")
with cols[2]:
    quarter = st.selectbox('', options=["Q1 (Jan - Mar)", "Q2 (Apr - Jun)", "Q3 (Jul - Sep)",
                                        "Q4 (Oct - Dec)"], label_visibility="hidden")
quarter_value = 1 if quarter == "Q1 (Jan - Mar)" else 2 if quarter == "Q2 (Apr - Jun)" else 3 \
    if quarter == "Q3 (Jul - Sep)" else 4
click_btn = st.button("Get Pulse Data")
if click_btn:
    result_data = get_all_records_from_all_tables(cache_data(), int(year), quarter_value, select_value)
    if not result_data[0].empty:
        state_df = result_data[0]
        main_all_values = result_data[1].to_dict(orient="records")[0]
        bar_plots_x = "State"
        if select_value == "Transaction":
            map_col_values = ["State", "Transaction Count", "Total Payment", "Average Payment"]
            bar_plots_y = "Total Payment"
            color_col = "Average Payment"
            category_value = result_data[2]
        elif select_value == "Users":
            map_col_values = ["State", "Registered Users", "App Opens"]
            bar_plots_y = "Registered Users"
            color_col = "App Opens"
            category_value = result_data[2]
        else:
            map_col_values = ["State", "No Of Policies", "Total Premium", "Average Premium"]
            bar_plots_y = "Total Premium"
            color_col = "Average Premium"
        grp_panel = st.columns([3, 1])
        with (grp_panel[0]):
            state_df["State"] = state_df["State"].replace(to_replace=r'[-&--]', value=' ', regex=True
                                                          ).replace(to_replace=r'\s+', value=' ',
                                                                    regex=True).str.title()
            plots_tab = st.tabs(["Map Plots", "Bar Plots", "Pie Plots", "Histograms"])
            with plots_tab[0]:
                with st.container(border=True, height=1200):
                    fig = px.choropleth(
                        data_frame=state_df,
                        geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/"
                                "e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        color='State',
                        hover_data=map_col_values,
                        color_continuous_scale="Viridis",
                        range_color=(0, 36),
                        center={"lat": 37.0902, "lon": -95.7129},
                        projection="mercator",
                        title="India Map",
                        height=1200
                    )
                    fig.update_geos(fitbounds="locations", visible=True)
                    fig.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
                    st.plotly_chart(fig)
            with plots_tab[1]:
                with st.container(border=True, height=1200):
                    fig = px.bar(state_df, x=bar_plots_x, y=bar_plots_y, color=color_col)
                    st.plotly_chart(fig)
            with plots_tab[2]:
                with st.container(border=True, height=1200):
                    fig = px.pie(state_df, names=bar_plots_x, values=bar_plots_y,
                                 color_discrete_sequence=px.colors.sequential.RdBu, color=color_col)
                    st.plotly_chart(fig)
            with plots_tab[3]:
                with st.container(border=True, height=1200):
                    fig = px.histogram(state_df, x=bar_plots_x, y=bar_plots_y, color=color_col)
                    st.plotly_chart(fig)
        with grp_panel[1]:
            with st.container(border=True):
                st.markdown(f"<p style='color:#05c3de; font-size:30px;'><b>{select_value}</b></p>",
                            unsafe_allow_html=True)
                # st.write(title_text)
                if select_value == "Transaction":
                    st.markdown("<p><b>All PhonePe transactions (UPI + Cards + Wallets)</b></p>",
                                unsafe_allow_html=True)
                    st.markdown(f"<p style='color:#05c3de; font-size:30px;'><b>"
                                f"{format(main_all_values["transaction_count"], ",d")}</b></p>",
                                unsafe_allow_html=True)
                    sub_col = st.columns(2)
                    with sub_col[0]:
                        st.write("Total Payment Value")
                        st.write(f"**:blue[₹{format(int(main_all_values["transaction_amount"]), ",d")}]**")
                    with sub_col[1]:
                        st.write("Avg. Transaction Value")
                        st.write(f"**:blue[₹{format(int(main_all_values["avg_amount"]), ",d")}]**")
                elif select_value == "Users":
                    title_info = quarter.split(" ")[0] + " " + year
                    st.markdown(f"<p><b>Registered PhonePe users till {title_info}</b></p>",
                                unsafe_allow_html=True)
                    st.markdown(f"<p style='color:#05c3de; font-size:28px;'><b>"
                                f"{format(int(main_all_values["registered_users"]), ",d")}"
                                f"</b></p>",
                                unsafe_allow_html=True)
                    st.write(f"PhonePe app opens in {title_info}")
                    st.markdown(f"<p style='color:#05c3de; font-size:25px;'><b>"
                                f"{format(int(main_all_values["app_opens"]), ",d")}</b></p>",
                                unsafe_allow_html=True)
                else:
                    st.markdown(f"<p><b>All India Insurance Policies Purchased (Nos.)</b></p>",
                                unsafe_allow_html=True)
                    st.markdown(f"<p style='color:#05c3de; font-size:28px;'><b>"
                                f"{format(int(main_all_values["no_of_policies"]), ",d")}"
                                f"</b></p>",
                                unsafe_allow_html=True)
                    sub_col = st.columns(2)
                    with sub_col[0]:
                        st.write("Total Premium Value")
                        st.markdown(f"<p style='color:#05c3de; font-size:25px;'><b>"
                                    f"₹{format(int(main_all_values["total_premium"]), ",d")}"
                                    f"</b></p>", unsafe_allow_html=True)
                    with sub_col[1]:
                        st.write("Average Premium Value")
                        st.markdown(f"<p style='color:#05c3de; font-size:25px;'><b>"
                                    f"₹{format(int(main_all_values["avg_premium"]), ",d")}"
                                    f"</b></p>", unsafe_allow_html=True)
                st.divider()
                if select_value == "Transaction":
                    st.markdown(f"<p style='color:#05c3de; font-size:30px;'><b>Categories</b></p>",
                                unsafe_allow_html=True)
                elif select_value == "Users":
                    st.markdown(f"<p style='color:#05c3de; font-size:30px;'><b>Users By Device</b></p>",
                                unsafe_allow_html=True)
                if select_value in ["Transaction", "Users"]:
                    st.dataframe(category_value, hide_index=True, use_container_width=True)
                    st.divider()
                three_tabs = st.tabs(["States", "Districts", "Postal Code"])
                with three_tabs[0]:
                    st.header("Top 10 State")
                    states_list = result_data[3] if select_value in ["Transaction", "Users"] else result_data[2]
                    # states_list["amount"] = states_list["amount"].apply(lambda x: str((int(x * (10 ** 2)) / 10 ** 2)/1e7) + "Cr")
                    states_list["States"] = states_list["States"].str.replace("-", " ").str.title()
                    states_list.index = range(1, 11)
                    st.dataframe(states_list, use_container_width=True)
                with three_tabs[1]:
                    st.header("Top 10 Districts")
                    districts_list = result_data[4] if select_value in ["Transaction", "Users"] else result_data[3]
                    districts_list["Districts"] = districts_list["Districts"].str.title()
                    districts_list.index = range(1, 11)
                    st.dataframe(districts_list, use_container_width=True)
                with three_tabs[2]:
                    st.header("Top 10 Postal Code")
                    pincode_list = result_data[5] if select_value in ["Transaction", "Users"] else result_data[4]
                    pincode_list.index = range(1, 11)
                    st.dataframe(pincode_list, use_container_width=True)

    else:
        st.write("No Record Found.")

