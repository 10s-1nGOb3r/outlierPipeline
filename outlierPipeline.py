import pandas as pd
import numpy as np
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
save_at = os.path.join(script_dir,"output","dfsFlightHourDetails.csv")
save_at2 = os.path.join(script_dir,"output","aircrewFlightHourPerMonthPerYear.csv")
save_at3 = os.path.join(script_dir,"output","aircrewOutlierView.csv")
file_path = os.path.join(script_dir,"input","dfsOutlierReport.csv")
file_path2 = os.path.join(script_dir,"input","namelistAircrew.csv")
file_path3 = os.path.join(script_dir,"input","insStructural.csv")

df = pd.read_csv(file_path, sep=";")

def dataCleansing():
    df["DATE"] = pd.to_datetime(df["DATE"], format="%d/%m/%Y", errors="coerce")
    df["DATE"] = df["DATE"].ffill()

    cleanField = ["FLT","TYPE","REG","AC","DEP","ARR"]
    for field in cleanField:
        df[field] = df[field].ffill()
        df[field] = df[field].astype(str)

    cleanField2 = ["STD","STA","ATD","ATA"]
    for field2 in cleanField2:
        df[field2] = df[field2].ffill()
        df[field2] = pd.to_datetime(df["DATE"].dt.strftime("%Y-%m-%d") + " " + df[field2])

    df["BLOCK"] = pd.to_timedelta(df["BLOCK"].astype(str) + ":00",errors="coerce")
    df["BLOCK"] = df["BLOCK"].ffill()
    df["BLOCK_DEC"] = df["BLOCK"].dt.total_seconds() / 3600
    df["BLOCK_DEC"] = df["BLOCK_DEC"].round(3)

    conditions = [
        df["Crew"].str.contains("CPT", na=False),
        df["Crew"].str.contains("FO", na=False),
        df["Crew"].str.contains("FA1", na=False),
        df["Crew"].str.contains("FA", na=False)
    ]

    choices = ["CPT","FO","FA1","FA"]

    df["RANK"] = np.select(conditions,choices,default="0")

    conditions2 = [
        df["Crew"].str.contains("CPT", na=False),
        df["Crew"].str.contains("FA1", na=False),
        df["Crew"].str.contains("FO", na=False),
        df["Crew"].str.contains("FA", na=False)
    ]

    choices2 = [df["Crew"].str[7:13],
                df["Crew"].str[7:13],
                df["Crew"].str[6:12],
                df["Crew"].str[6:12]
    ]
    
    df["Crew"] = np.select(conditions2,choices2,default="0")
    df["Crew"] = df["Crew"].astype(str).str.strip()

    df["Crew #"] = df["Crew #"].ffill()
    df["Crew #"] = df["Crew #"].astype(str)

    df["MONTH"] = df["DATE"].dt.month
    df["MONTH"] = df["MONTH"].astype(str)
    df["YEAR"] = df["DATE"].dt.year
    df["YEAR"] = df["YEAR"].astype(str)

    df["KEY"] = df["Crew"] + "." + df["MONTH"] + "." + df["YEAR"]
    df["KEY"] = df["KEY"].astype(str)

dataCleansing()

df2 = df[["KEY","Crew","MONTH","YEAR","BLOCK_DEC"]]
flightHourPerCrew = df2.groupby(["KEY","Crew","MONTH","YEAR"]).agg(
    totalFlightHour = ("BLOCK_DEC","sum")
).reset_index()
flightHourPerCrew["totalFlightHour"] = flightHourPerCrew["totalFlightHour"].round(2)

df3 = pd.read_csv(file_path2, sep=";")

cleanField3 = ["MONTH","YEAR","ID"]

for field3 in cleanField3:
    df3[field3] = df3[field3].astype(str)

df3["KEY_DFS"] = df3["ID"] + "." + df3["MONTH"] + "." + df3["YEAR"]
df3["KEY_DFS"] = df3["KEY_DFS"].astype(str)

fh_merge = flightHourPerCrew[["KEY","totalFlightHour"]]

df4 = pd.merge(df3,fh_merge,how="left",left_on="KEY_DFS",right_on="KEY")

insStruct = pd.read_csv(file_path3, sep=";")
insStruct["ID"] = insStruct["ID"].astype(str)
insStruct["MONTH"] = insStruct["MONTH"].astype(str)
insStruct["YEAR"] = insStruct["YEAR"].astype(str)
insStruct["KEY_INSSTR"] = insStruct["ID"] + "." + insStruct["MONTH"] + "." + insStruct["YEAR"]
insStruct["KEY_INSSTR"] = insStruct["KEY_INSSTR"].astype(str)

insStruct_merge = insStruct[["KEY_INSSTR","STATUS"]]

df4 = pd.merge(df4,insStruct_merge,how="left",left_on="KEY_DFS",right_on="KEY_INSSTR")

df4["KEY"] = df4["KEY"].fillna("0")
df4["KEY"] = df4["KEY"].astype("str")
df4["MONTH"] = df4["MONTH"].astype("str")
df4["YEAR"] = df4["YEAR"].astype("str")
df4["BASE"] = df4["BASE"].str.replace(" ","",regex=False)
df4["AC"] = df4["AC"].str.replace(" ","",regex=False)
df4["totalFlightHour"] = df4["totalFlightHour"].fillna(0.00)
df4["totalFlightHour"] = df4["totalFlightHour"].astype(float)
df4["totalFlightHour"] = df4["totalFlightHour"].round(2)

df4["KEY_INSSTR"] = df4["KEY_INSSTR"].fillna("0")
df4["KEY_INSSTR"] = df4["KEY_INSSTR"].astype(str)
df4["STATUS"] = df4["STATUS"].fillna("0")
df4["STATUS"] = df4["STATUS"].astype(str)


conditions4 = [
               (df4["totalFlightHour"] == 0.00) & (df4["STATUS"] == "0"),
               (df4["totalFlightHour"] == 0.00) & (df4["STATUS"] != "0"),
               (df4["totalFlightHour"] > 0.00) & (df4["STATUS"] != "0"),
               (df4["totalFlightHour"] > 0.00) & (df4["STATUS"] == "0")       
]

choices4 = [
            "0",
            "0",
            "0",
            df4["BASE"] + "." + df4["AC"] + "." + df4["POS"] + "." + df4["MONTH"] +"." + df4["YEAR"]
]

df4["union"] = np.select(conditions4,choices4,default="0")

average = df4.groupby(["union"])["totalFlightHour"].mean().reset_index()
average = average.rename(columns={"totalFlightHour": "Average"})
df4 = df4.merge(average, on="union",how="left")
df4["Average"] = df4["Average"].fillna(0.00)
df4["Average"] = df4["Average"].astype(float)
df4["Average"] = df4["Average"].round(2)

conditions3 = [
               (df4["KEY"] == "0"),
               (df4["STATUS"] != "0"),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 10)
]

choices3 = [
            "0",
            "0",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent"
]

df4["outlierType"] = np.select(conditions3,choices3,default="0")

df.to_csv(save_at,sep=";",index=False)
flightHourPerCrew.to_csv(save_at2,sep=";",index=False)
df4.to_csv(save_at3,sep=";",index=False)

#df.info()
