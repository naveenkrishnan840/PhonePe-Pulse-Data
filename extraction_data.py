from collections import defaultdict
from pathlib import Path
from typing import List, Dict
import pandas as pd
import json


class ExtractionData:
    def __init__(self):
        self.top_user_district_df = None
        self.top_insurance_pincode_df = None
        self.top_insurance_district_df = None
        self.top_transaction_pincode_df = None
        self.top_transaction_district_df = None
        self.map_user_df = None
        self.insurance_map_df = None
        self.map_transaction_df = None
        self.agg_user_brand_df = None
        self.agg_insurance_df = None
        self.state_df = None
        self.district_df = None
        self.agg_transaction_df = None
        self.top_user_pincode_df = None
        self.all_path = {"aggregated_path": ["pulse/data/aggregated/transaction/country/india/state",
                                             "pulse/data/aggregated/insurance/country/india/state",
                                             "pulse/data/aggregated/user/country/india/state"],
                         "map_path": ["pulse/data/map/transaction/hover/country/india/state",
                                      "pulse/data/map/insurance/hover/country/india/state",
                                      "pulse/data/map/user/hover/country/india/state"],
                         "top_path": ["pulse/data/top/transaction/country/india/state",
                                      "pulse/data/top/insurance/country/india/state",
                                      "pulse/data/top/user/country/india/state"]}
        self.preprocessing_data_for_pulse()

    def preprocessing_data_for_pulse(self):
        for action, paths in self.all_path.items():
            if action == "aggregated_path":
                self.aggregated_path_data(paths)
            elif action == "map_path":
                self.map_path_data(paths)
            else:
                self.top_path_data(paths)

    def aggregated_path_data(self, paths: List):
        for index, path in enumerate(paths):
            if index == 0:
                self.agg_transaction(path)
            elif index == 1:
                self.agg_insurance(path)
            else:
                self.agg_user(path)

    def map_path_data(self, paths: List):
        for index, path in enumerate(paths):
            if index == 0:
                self.map_transaction(path)
            elif index == 1:
                self.map_insurance(path)
            else:
                self.map_user(path)

    def top_path_data(self, paths: List):
        for index, path in enumerate(paths):
            if index == 0:
                self.top_transaction(path)
            elif index == 1:
                self.top_insurance(path)
            else:
                self.top_user(path)

    def agg_transaction(self, path: str):
        data_path = Path(path)
        agg_transaction_dict = defaultdict(list)
        states = {"state_id": [], "state_name": []}
        state_cnt = 0
        for file in data_path.glob("**/*.json"):
            state = file.parts[-3]
            if state not in states.get("state_name"):
                states.get("state_name").append(state)
                state_cnt += 1
                states.get("state_id").append(state_cnt)
            with file.open() as files:
                for i in json.loads(files.read())["data"]["transactionData"]:
                    agg_transaction_dict["transaction_type"].append(i["name"])
                    agg_transaction_dict["transaction_count"].append(i["paymentInstruments"][0]["count"])
                    agg_transaction_dict["transaction_amount"].append(i["paymentInstruments"][0]["amount"])
                    agg_transaction_dict["state_id"].append(state_cnt)
                    agg_transaction_dict["year"].append(int(file.parts[-2]))
                    agg_transaction_dict["quarter"].append(int(file.parts[-1].split(".")[0]))

        self.agg_transaction_df = (pd.DataFrame(agg_transaction_dict,
                                                columns=["state_id", "year", "quarter", "transaction_type",
                                                         "transaction_count", "transaction_amount"]).
                                   sort_values(by=["year", "quarter"]).reset_index(drop=True))
        self.state_df = pd.DataFrame(states, columns=["state_id", "state_name"]).sort_values("state_id").reset_index(
            drop=True)

    def agg_insurance(self, path: str):
        data_path = Path(path)
        agg_insurance_dict = defaultdict(list)
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            with file.open() as files:
                for data in json.load(files)["data"]["transactionData"]:
                    agg_insurance_dict["state_id"].append(state_id)
                    agg_insurance_dict["year"].append(int(file.parts[-2]))
                    agg_insurance_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                    agg_insurance_dict["insurance_count"].append(data["paymentInstruments"][0]["count"])
                    agg_insurance_dict["insurance_amount"].append(data["paymentInstruments"][0]["amount"])

        self.agg_insurance_df = pd.DataFrame(agg_insurance_dict,
                                             columns=["state_id", "year", "quarter", "insurance_count",
                                                      "insurance_amount"])

    def agg_user(self, path: str):
        data_path = Path(path)
        agg_user_dict = defaultdict(list)
        state_cnt = 0
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            state_cnt += 1
            with file.open() as files:
                usersByDevice = json.load(files)["data"]["usersByDevice"]
                if usersByDevice:
                    for user in usersByDevice:
                        agg_user_dict["state_id"].append(state_id)
                        agg_user_dict["year"].append(int(file.parts[-2]))
                        agg_user_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                        agg_user_dict["brand_type"].append(user["brand"])
                        agg_user_dict["count"].append(user["count"])
                        agg_user_dict["percentage"].append(user["percentage"])

        self.agg_user_brand_df = (pd.DataFrame(agg_user_dict, columns=["state_id", "year", "quarter", "brand_type",
                                                                       "count", "percentage"]).
                                  sort_values(by=["brand_type"]).reset_index(drop=True))

    def map_transaction(self, path: str):
        data_path = Path(path)
        map_transaction_dict = defaultdict(list)
        districts_dic = {"district_id": [], "district_name": []}
        district_cnt = 0
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            with file.open() as files:
                for data in json.load(files)["data"]["hoverDataList"]:
                    district_name = data["name"].replace("district", "").strip()
                    if district_name not in districts_dic.get("district_name"):
                        districts_dic.get("district_name").append(district_name)
                        district_cnt += 1
                        districts_dic.get("district_id").append(district_cnt)
                    map_transaction_dict["state_id"].append(state_id)
                    map_transaction_dict["year"].append(int(file.parts[-2]))
                    map_transaction_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                    map_transaction_dict["district_id"].append(district_cnt)
                    map_transaction_dict["transaction_count"].append(data["metric"][0]["count"])
                    map_transaction_dict["transaction_amount"].append(data["metric"][0]["amount"])

        self.map_transaction_df = pd.DataFrame(map_transaction_dict,
                                               columns=["state_id", "year", "quarter", "district_id",
                                                        "transaction_count", "transaction_amount"])
        self.district_df = pd.DataFrame(districts_dic, columns=["district_id", "district_name"])

    def map_insurance(self, path: str):
        data_path = Path(path)
        map_insurance_dict = defaultdict(list)
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            with file.open() as files:
                for data in json.load(files)["data"]["hoverDataList"]:
                    map_insurance_dict["state_id"].append(state_id)
                    map_insurance_dict["year"].append(int(file.parts[-2]))
                    map_insurance_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                    map_insurance_dict["district_id"].append(
                        self.district_df[
                            self.district_df['district_name'] == data["name"].replace("district", "").strip()][
                            "district_id"].to_list()[0])
                    map_insurance_dict["insurance_count"].append(data["metric"][0]["count"])
                    map_insurance_dict["insurance_amount"].append(data["metric"][0]["amount"])

        self.insurance_map_df = pd.DataFrame(map_insurance_dict, columns=["state_id", "year", "quarter", "district_id",
                                                                          "insurance_count", "insurance_amount"])

    def map_user(self, path: str):
        data_path = Path(path)
        map_user_dict = defaultdict(list)
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            with file.open() as files:
                for district, data in json.load(files)["data"]["hoverData"].items():
                    district = district.replace("district", "").strip()
                    map_user_dict["state_id"].append(state_id)
                    map_user_dict["year"].append(int(file.parts[-2]))
                    map_user_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                    map_user_dict["district_id"].append(
                        self.district_df.loc[self.district_df['district_name'] == district, "district_id"].to_list()[0])
                    map_user_dict["registered_users"].append(data["registeredUsers"])
                    map_user_dict["app_opens"].append(data["appOpens"])

        self.map_user_df = pd.DataFrame(map_user_dict, columns=["state_id", "year", "quarter", "district_id",
                                                                "registered_users", "app_opens"])

    def top_transaction(self, path: str):
        data_path = Path(path)
        top_transaction_dict = defaultdict(list)
        top_pincode_dict = defaultdict(list)
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            with file.open() as files:
                for name, data in json.load(files)["data"].items():
                    try:
                        if name == "districts":
                            for i in data:
                                top_transaction_dict["state_id"].append(state_id)
                                top_transaction_dict["year"].append(int(file.parts[-2]))
                                top_transaction_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                                top_transaction_dict["district_id"].append(
                                    self.district_df.loc[self.district_df['district_name'] ==
                                                         i["entityName"], "district_id"].to_list()[0])
                                top_transaction_dict["transaction_count"].append(i["metric"]["count"])
                                top_transaction_dict["transaction_amount"].append(i["metric"]["amount"])
                        elif name == "pincodes":
                            for i in data:
                                top_pincode_dict["state_id"].append(state_id)
                                top_pincode_dict["year"].append(int(file.parts[-2]))
                                top_pincode_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                                top_pincode_dict["pincode"].append(i["entityName"])
                                top_pincode_dict["transaction_count"].append(i["metric"]["count"])
                                top_pincode_dict["transaction_amount"].append(i["metric"]["amount"])
                    except Exception as e:
                        print(i["entityName"])
                        print(self.district_df.loc[self.district_df['district_name'] == i["entityName"], "district_id"])
                        raise e

        self.top_transaction_district_df = pd.DataFrame(top_transaction_dict,
                                                        columns=["state_id", "year", "quarter", "district_id",
                                                                 "transaction_count", "transaction_amount"])

        self.top_transaction_pincode_df = pd.DataFrame(top_pincode_dict,
                                                       columns=["state_id", "year", "quarter", "pincode",
                                                                "transaction_count", "transaction_amount"])

    def top_insurance(self, path: str):
        data_path = Path(path)
        top_insurance_district_dict = defaultdict(list)
        top_insurance_pincode_dict = defaultdict(list)
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            with file.open() as files:
                for name, datas in json.load(files)["data"].items():
                    if name == "districts":
                        for data in datas:
                            top_insurance_district_dict["state_id"].append(state_id)
                            top_insurance_district_dict["year"].append(int(file.parts[-2]))
                            top_insurance_district_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                            top_insurance_district_dict["district_id"].append(
                                self.district_df.loc[self.district_df['district_name'] ==
                                                     data["entityName"], "district_id"].to_list()[0])
                            top_insurance_district_dict["insurance_count"].append(data["metric"]["count"])
                            top_insurance_district_dict["insurance_amount"].append(data["metric"]["amount"])
                    elif name == "pincodes":
                        for data in datas:
                            top_insurance_pincode_dict["state_id"].append(state_id)
                            top_insurance_pincode_dict["year"].append(int(file.parts[-2]))
                            top_insurance_pincode_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                            top_insurance_pincode_dict["pincode"].append(data["entityName"])
                            top_insurance_pincode_dict["insurance_count"].append(data["metric"]["count"])
                            top_insurance_pincode_dict["insurance_amount"].append(data["metric"]["amount"])

        self.top_insurance_district_df = pd.DataFrame(top_insurance_district_dict,
                                                      columns=["state_id", "year", "quarter",
                                                               "district_id", "insurance_count",
                                                               "insurance_amount"])
        self.top_insurance_pincode_df = pd.DataFrame(top_insurance_pincode_dict,
                                                     columns=["state_id", "year", "quarter",
                                                              "pincode", "insurance_count",
                                                              "insurance_amount"])

    def top_user(self, path: str):
        data_path = Path(path)
        top_user_transaction_dict = defaultdict(list)
        top_user_pincode_dict = defaultdict(list)
        for file in data_path.glob("**/*.json"):
            state_name = file.parts[-3]
            state_id = self.state_df[self.state_df['state_name'] == state_name]["state_id"].to_list()[0]
            with file.open() as files:
                datas = json.load(files)["data"].items()
                for name, data in datas:
                    try:
                        if name == "districts":
                            for i in data:
                                top_user_transaction_dict["state_id"].append(state_id)
                                top_user_transaction_dict["year"].append(int(file.parts[-2]))
                                top_user_transaction_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                                top_user_transaction_dict["district_id"].append(
                                    self.district_df.loc[self.district_df['district_name'] ==
                                                         i["name"], "district_id"].to_list()[0])
                                top_user_transaction_dict["registered_users"].append(i["registeredUsers"])
                        elif name == "pincodes":
                            for i in data:
                                top_user_pincode_dict["state_id"].append(state_id)
                                top_user_pincode_dict["year"].append(int(file.parts[-2]))
                                top_user_pincode_dict["quarter"].append(int(file.parts[-1].split(".")[0]))
                                top_user_pincode_dict["pincode"].append(i["name"])
                                top_user_pincode_dict["registered_users"].append(i["registeredUsers"])
                    except Exception as e:
                        print(i["entityName"])
                        print(self.district_df.loc[self.district_df['district_name'] == i["entityName"], "district_id"])
                        raise e

        self.top_user_district_df = pd.DataFrame(top_user_transaction_dict, columns=["state_id", "year", "quarter",
                                                                                     "district_id", "registered_users"])

        self.top_user_pincode_df = pd.DataFrame(top_user_pincode_dict, columns=["state_id", "year", "quarter",
                                                                                "pincode", "registered_users"])



