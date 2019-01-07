#!/usr/bin/python
# -*- coding:utf-8 -*-
import pandas as pd
country1 = pd.Series({"Name":"中国",
                      "Language":"Chinese",
                      "Area":"9.60M km2",
                      "Happiness Rank":"79",})
country2 = pd.Series({"Name":"美国",
                      "Language":"English",
                      "Area":"9.20M km2",
                      "Happiness Rank":"14",})
country3 = pd.Series({"Name":"澳大利亚",
                      "Language":"Eng(AU)",
                      "Area":"8.30M km2",
                      "Happiness Rank":"9",})
df = pd.DataFrame([country1 ,country2 ,country3] ,index=["CH","US","AU"])
print(df)
print("---------------------------------------------------------------")

"""
添加数据，如果个数小于要求的个数，会自动进行广播操作
如果大于要求的个数，会报错
"""
df["Location"] = "地球"
print(df)
print("---------------------------------------------------------------")
df["Region"] = ["亚洲","北美洲","大洋洲"]
print(df)
print("---------------------------------------------------------------")

print("行索引，loc")
# print(df.loc["CH"])
print(df.iloc[0])
print("---------------------------------------------------------------")

print("列索引")
print(df["Area"])
print("---------------------------------------------------------------")

#获取不连续的列数据
print(df[["Name","Area"]])
print("---------------------------------------------------------------")

#混合索引,注意写法上的区别
print("先去列， 再去行：")
print(df["Area"]["CH"])
print("---------------------------------------------------------------")
print(df.loc["CH"]["Area"])
print("---------------------------------------------------------------")
print(df.iloc[0]["Area"])
print("---------------------------------------------------------------")

#转换行和列
print(df.T)

print("----------------------------")
"""
删除数据，注意drop操作只是修改后的数据copy一份，
而不会对原始数据进行修改,copy（备份）
"""

print("删除数据\n",df.drop(["CH"]))
print("----------------------------")
print("原数据\n",df)
print("----------------------------")
"""
如果使用inplace = Ture ,会在原始数据上进行修改，
同时不会返回一个copy
"""
print(df.drop(["CH"] ,inplace=True))
print("----------------------------")
print(df)
print("----------------------------")
"""
如果需要删除列，指定axis=1   0 行
"""
print(df.drop(["Area"] ,axis=1))
print(df)
print("----------------------------")

del df["Name"]
print(df)

print("----------------------------")

report_2015_df = pd.read_csv("../data/2015.csv")
print(report_2015_df.head())
print("----------------------------")
print(report_2015_df.info()) #信息
print("----------------------------")
report_2015_df = pd.read_csv("../data/2015.csv" ,index_col="Country",

                             usecols=["Country","Happiness Rank","Happiness Score",
                                      "Region"
                                      ])

print(report_2015_df.head())

print("----------------------------")
print("列名",report_2015_df.columns)
print("行名",report_2015_df.index)
print("----------------------------")