import pandas as pd
import numpy as np
import os

# Making file path for reading files and saving files
script_dir = os.path.dirname(os.path.abspath(__file__))
save_at = os.path.join(script_dir,"output","dfsFlightHourDetails.csv")
save_at2 = os.path.join(script_dir,"output","aircrewFlightHourPerMonthPerYear.csv")
save_at3 = os.path.join(script_dir,"output","aircrewOutlierView.csv")
save_at4 = os.path.join(script_dir,"output","outlierAggregationPerMonthPerUnion.csv")
save_at5 = os.path.join(script_dir,"output","outlierAggregationPerMonthPerAircrewType.csv")
file_path = os.path.join(script_dir,"input","dfsOutlierReport.csv")
file_path2 = os.path.join(script_dir,"input","namelistAircrew.csv")
file_path3 = os.path.join(script_dir,"input","insStructural.csv")

# Processing data from AIMS Reporting Features 
# 1.2.1. Daily Flight Schedule
# Fields taken such as DATE, FLT, TYPE, REG, AC, DEP, ARR, STD, STA, 
# ATD, ATA
# (Cont) BLOCK, Crew #, Crew
df = pd.read_csv(file_path, sep=";")

def dataCleansing():
    # A process for data cleaning and the table contains Aircrew's Flight Hours Per Leg
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

    choices2 = [df["Crew"].str[7:14],
                df["Crew"].str[7:14],
                df["Crew"].str[6:13],
                df["Crew"].str[6:13]
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

# Aggregation process from the cleaned Daily Flight Schedule Table
df2 = df[["KEY","Crew","MONTH","YEAR","BLOCK_DEC"]]
flightHourPerCrew = df2.groupby(["KEY","Crew","MONTH","YEAR"]).agg(
    totalFlightHour = ("BLOCK_DEC","sum")
).reset_index()
flightHourPerCrew["totalFlightHour"] = flightHourPerCrew["totalFlightHour"].round(2)

# Reading files for Aircrew's namelist extracting 
# AIMS Report 4.4.2.5. Block - Duty Time Totals
df3 = pd.read_csv(file_path2, sep=";")

# File cleaning & creating a primary key to be used for table merging
cleanField3 = ["MONTH","YEAR","ID"]

for field3 in cleanField3:
    df3[field3] = df3[field3].astype(str)

df3["KEY_DFS"] = df3["ID"] + "." + df3["MONTH"] + "." + df3["YEAR"]
df3["KEY_DFS"] = df3["KEY_DFS"].astype(str)

# Selecting fields to be merged 
fh_merge = flightHourPerCrew[["KEY","totalFlightHour"]]

# Merging the aggregated tables from 
# AIMS Report 1.2.1. Daily Flight Schedule
# And AIMS Report 4.4.2.5. Block - Duty Time Totals
df4 = pd.merge(df3,fh_merge,how="left",left_on="KEY_DFS",right_on="KEY")

# Reading database file for list of Instructor, Structural Aircrew
insStruct = pd.read_csv(file_path3, sep=";")

# Cleaning the database and creating Primary Key to be merged with
# AIMS Report 4.4.2.5. Block - Duty Time Totals
cleanField4 = ["ID","MONTH","YEAR"]
for field3 in cleanField4:
    insStruct[field3] = insStruct[field3].astype(str)

insStruct["KEY_INSSTR"] = insStruct["ID"] + "." + insStruct["MONTH"] + "." + insStruct["YEAR"]
insStruct["KEY_INSSTR"] = insStruct["KEY_INSSTR"].astype(str)

insStruct_merge = insStruct[["KEY_INSSTR","STATUS"]]

# Merging the Structural, Instructor Aircrew Database and
# table produced from AIMS Report 4.4.2.5. Block - Duty Time Totals
df4 = pd.merge(df4,insStruct_merge,how="left",left_on="KEY_DFS",right_on="KEY_INSSTR")

# Clean up the table once again
df4["KEY"] = df4["KEY"].fillna("0")
df4["KEY"] = df4["KEY"].astype("str")
df4["MONTH"] = df4["MONTH"].astype("str")
df4["YEAR"] = df4["YEAR"].astype("str")
df4["BASE"] = df4["BASE"].str.replace(" ","",regex=False)
df4["AC"] = df4["AC"].str.replace(" ","",regex=False)
df4["totalFlightHour"] = df4["totalFlightHour"].fillna(0.00)
df4["totalFlightHour"] = df4["totalFlightHour"].astype(float)
df4["totalFlightHour"] = df4["totalFlightHour"].round(2)

cleanField5 = ["KEY_INSSTR","STATUS"]
for field4 in cleanField5:
    df4[cleanField5] = df4[cleanField5].fillna("0")
    df4[cleanField5] = df4[cleanField5].astype(str)

# union being made in order to clarify the average, 
# standard deviations, & produced outlier exponents
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

df4["totalFlightHour"] = np.where(df4["union"] == "0",0.00,df4["totalFlightHour"])

cleanField7 = ["YEAR","MONTH","BASE","AC","POS","ID","NAME"]
for field6 in cleanField7:
    df4[field6] = np.where(df4["totalFlightHour"] == 0.00, "0", df4[field6])
    df4[field6] = df4[field6].astype("str")

# The brain part : Determining Average Hours, Standard Deviation, 
# Outlier Exponents
average = df4.groupby(["union"])["totalFlightHour"].mean().reset_index()
average = average.rename(columns={"totalFlightHour": "Average"})
df4 = df4.merge(average, on="union",how="left")
df4["Average"] = df4["Average"].fillna(0.00)
df4["Average"] = df4["Average"].astype(float)
df4["Average"] = df4["Average"].round(2)

standardDev = df4.groupby(["union"])["totalFlightHour"].std().reset_index()
standardDev = standardDev.rename(columns={"totalFlightHour": "standardDeviation"})
df4 = df4.merge(standardDev, on="union",how="left")
df4["standardDeviation"] = df4["standardDeviation"].fillna(0.00)
df4["standardDeviation"] = df4["standardDeviation"].astype(float)
df4["standardDeviation"] = df4["standardDeviation"].round(2)

cleanField6 = ["Average","standardDeviation"]
for field5 in cleanField6:
    df4[field5] = np.where(df4["union"] == "0",df4[field5] == "0",df4[field5])

conditions3 = [
               (df4["KEY"] == "0"),
               (df4["STATUS"] != "0"),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 10.82),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 10.82),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 10.82),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 10.82) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 10.82)
]

choices3 = [
            "0",
            "0",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent"
]

df4["outlierType"] = np.select(conditions3,choices3,default="0")

conditions5 = [
               (df4["KEY"] == "0"),
               (df4["STATUS"] != "0"),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 21.64) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 21.64) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 21.64),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 21.64) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 21.64) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 21.64),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 23.76) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 23.76) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 23.76),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 23.76) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 23.76) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 23.76)
]

choices5 = [
            "0",
            "0",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent"
]

df4["outlierType2"] = np.select(conditions5,choices5,default="0")

conditions6 = [
               (df4["KEY"] == "0"),
               (df4["STATUS"] != "0"),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 32.46) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 32.46) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "CPT") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 32.46),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 32.46) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 32.46) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FO") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 32.46),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 35.65) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 35.65) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA1") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 35.65),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 35.65) & (df4["totalFlightHour"] >= df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() > 35.65) & (df4["totalFlightHour"] < df4["Average"]),
               (df4["KEY"] != "0") & (df4["STATUS"] == "0") & (df4["POS"] == "FA") & ((df4["totalFlightHour"] - df4["Average"]).abs() <= 35.65)
]

choices6 = [
            "0",
            "0",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent",
            "outlierAbove",
            "outlierBelow",
            "distributedExponent"
]

df4["outlierType3"] = np.select(conditions6,choices6,default="0")

# Determining Aircrew Type exponents and Assignable exponents
conditions4 = [
               (df4["POS"] == "CPT") & (df4["outlierType"] != "0"),
               (df4["POS"] == "FO") & (df4["outlierType"] != "0"),
               (df4["POS"] == "FA1") & (df4["outlierType"] != "0"),
               (df4["POS"] == "FA") & (df4["outlierType"] != "0")
]

choices4 = [
            "cockpit",
            "cockpit",
            "cabin",
            "cabin"
]

df4["aircrewType"] = np.select(conditions4,choices4,default="0")

df4["assignableValidation"] = np.where(df4["aircrewType"] != "0","assignable","0")
df4["assignableValidation"] = df4["assignableValidation"].astype(str)

# Aggregation process
outlierCal = df4.groupby(["YEAR","MONTH","BASE","AC","POS","union"]).agg(
    totalExponent = ("assignableValidation",lambda x: (x == "assignable").sum()),
    totalBlockHour = ("totalFlightHour","sum"),
    standardDeviation = ("standardDeviation","mean"),
    averageFlightHours = ("totalFlightHour","mean"),
    totalOutlierAboveSD1 = ("outlierType",lambda x: (x == "outlierAbove").sum()),
    totalOutlierBelowSD1 = ("outlierType",lambda x: (x == "outlierBelow").sum()),
    totalDistributedExponentSD1 = ("outlierType",lambda x: (x == "distributedExponent").sum()),
    totalOutlierAboveSD2 = ("outlierType2",lambda x: (x == "outlierAbove").sum()),
    totalOutlierBelowSD2 = ("outlierType2",lambda x: (x == "outlierBelow").sum()),
    totalDistributedExponentSD2 = ("outlierType2",lambda x: (x == "distributedExponent").sum()),
    totalOutlierAboveSD3 = ("outlierType3",lambda x: (x == "outlierAbove").sum()),
    totalOutlierBelowSD3 = ("outlierType3",lambda x: (x == "outlierBelow").sum()),
    totalDistributedExponentSD3 = ("outlierType3",lambda x: (x == "distributedExponent").sum())
).reset_index()

outlierCal["standardDeviation"] = outlierCal["standardDeviation"].astype(float)
outlierCal["standardDeviation"] = outlierCal["standardDeviation"].round(2)
outlierCal["standardDeviation"] = np.where(outlierCal["union"] == "0",outlierCal["standardDeviation"] == 0,outlierCal["standardDeviation"])
outlierCal["ceofficientVariant"] = round((outlierCal["standardDeviation"] / outlierCal["averageFlightHours"]) * 100,2)
outlierCal["averageFlightHours"] = outlierCal["averageFlightHours"].astype(float)
outlierCal["averageFlightHours"] = outlierCal["averageFlightHours"].round(2)
outlierCal["outlierAbovePercSD1"] = round((outlierCal["totalOutlierAboveSD1"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["outlierBelowPercSD1"] = round((outlierCal["totalOutlierBelowSD1"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["distributedExponentPercSD1"] = round((outlierCal["totalDistributedExponentSD1"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["outlierAbovePercSD2"] = round((outlierCal["totalOutlierAboveSD2"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["outlierBelowPercSD2"] = round((outlierCal["totalOutlierBelowSD2"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["distributedExponentPercSD2"] = round((outlierCal["totalDistributedExponentSD2"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["outlierAbovePercSD3"] = round((outlierCal["totalOutlierAboveSD3"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["outlierBelowPercSD3"] = round((outlierCal["totalOutlierBelowSD3"] / outlierCal["totalExponent"]) * 100,2)
outlierCal["distributedExponentPercSD3"] = round((outlierCal["totalDistributedExponentSD3"] / outlierCal["totalExponent"]) * 100,2)

outlierCal2 = df4.groupby(["YEAR","MONTH","aircrewType"]).agg(
    totalExponent = ("assignableValidation",lambda x: (x == "assignable").sum()),
    totalDistributedExponent = ("outlierType",lambda x: (x == "distributedExponent").sum()),
    standardDeviation = ("standardDeviation","mean"),
    totalBlockHour = ("totalFlightHour","sum"),
    totalOutlierAboveSD1 = ("outlierType",lambda x: (x == "outlierAbove").sum()),
    totalOutlierBelowSD1 = ("outlierType",lambda x: (x == "outlierBelow").sum()),
    totalDistributedExponentSD1 = ("outlierType",lambda x: (x == "distributedExponent").sum()),
    totalOutlierAboveSD2 = ("outlierType2",lambda x: (x == "outlierAbove").sum()),
    totalOutlierBelowSD2 = ("outlierType2",lambda x: (x == "outlierBelow").sum()),
    totalDistributedExponentSD2 = ("outlierType2",lambda x: (x == "distributedExponent").sum()),
    totalOutlierAboveSD3 = ("outlierType3",lambda x: (x == "outlierAbove").sum()),
    totalOutlierBelowSD3 = ("outlierType3",lambda x: (x == "outlierBelow").sum()),
    totalDistributedExponentSD3 = ("outlierType3",lambda x: (x == "distributedExponent").sum())
).reset_index()

outlierCal2["standardDeviation"] = outlierCal2["standardDeviation"].astype(float)
outlierCal2["standardDeviation"] = outlierCal2["standardDeviation"].round(2)
outlierCal2["standardDeviation"] = np.where(outlierCal2["aircrewType"] == "0",outlierCal2["standardDeviation"] == 0,outlierCal2["standardDeviation"])
outlierCal2["distributedExponentPerc"] = round((outlierCal2["totalDistributedExponent"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["averageHours"] = outlierCal2["totalBlockHour"] / outlierCal2["totalExponent"]
outlierCal2["averageHours"] = outlierCal2["averageHours"].astype(float)
outlierCal2["averageHours"] = outlierCal2["averageHours"].round(2)
outlierCal2["outlierAbovePercSD1"] = round((outlierCal2["totalOutlierAboveSD1"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["outlierBelowPercSD1"] = round((outlierCal2["totalOutlierBelowSD1"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["distributedExponentPercSD1"] = round((outlierCal2["totalDistributedExponentSD1"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["outlierAbovePercSD2"] = round((outlierCal2["totalOutlierAboveSD2"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["outlierBelowPercSD2"] = round((outlierCal2["totalOutlierBelowSD2"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["distributedExponentPercSD2"] = round((outlierCal2["totalDistributedExponentSD2"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["outlierAbovePercSD3"] = round((outlierCal2["totalOutlierAboveSD3"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["outlierBelowPercSD3"] = round((outlierCal2["totalOutlierBelowSD3"] / outlierCal2["totalExponent"]) * 100,2)
outlierCal2["distributedExponentPercSD3"] = round((outlierCal2["totalDistributedExponentSD3"] / outlierCal2["totalExponent"]) * 100,2)

# Producing the calculated reports
df.to_csv(save_at,sep=";",index=False)
flightHourPerCrew.to_csv(save_at2,sep=";",index=False)
df4.to_csv(save_at3,sep=";",index=False)
outlierCal.to_csv(save_at4,sep=";",index=False)
outlierCal2.to_csv(save_at5,sep=";",index=False)

