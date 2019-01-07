#!/usr/bin/python
# -*- coding:utf-8 -*-

import csv
import numpy as np
import matplotlib.pyplot as plt # 可视化方法
import pandas as pd
from sklearn.model_selection import train_test_split #数据集划分：训练集，测试集
from sklearn.linear_model import LinearRegression #线性预测模型库

if __name__ == '__main__':
    path = '../data/Advertising.csv'
    data = pd.read_csv(path)
    x = data[["TV","Radio","Newspaper"]]
    y = data["Sales"]
    #print(x.head())
    #print("===========================")
    #print(y.head())

    # 绘制图列
    plt.plot(data["TV"],y,"ro",label = "TV") #data["TV"]: x轴的TV数据
    plt.plot(data["Radio"],y,"g^",label = "Radio")
    plt.plot(data["Newspaper"],y,"mv",label = "Newspaper")
    plt.legend(loc = "lower right") # 把 TV Radio Newspaper 放在右下角
    plt.grid() #网格输出
    #plt.show()

    # 绘制图例2
    plt.figure(figsize=(9,10)) #图样式是长度为9，宽度为10
    plt.subplot(311) #3行1列第一个子图
    plt.plot(data["TV"], y, "ro")  # data["TV"]: 在x轴的数据分布
    plt.title("TV")
    plt.grid()

    plt.subplot(312)  # 3行1列第二个子图
    plt.plot(data["Radio"], y, "g^")  # data["TV"]: 在x轴的数据分布
    plt.title("Radio")
    plt.grid()

    plt.subplot(313)  # 3行1列第三个子图
    plt.plot(data["Newspaper"], y, "b*")  # data["TV"]: 在x轴的数据分布
    plt.title("Newspaper")
    plt.grid()

    plt.tight_layout()
    plt.show()

    # 二、分割数据集
    x_train, x_test, y_train, y_test = \
        train_test_split(x, y, random_state=1) #随机抓取数据的结果都是一致的

    # 三、选择/建立模型
    linreg = LinearRegression()
    # 四、训练模型
    model = linreg.fit(x_train,y_train)
    print(model)
    print("==========================")
    print(linreg.coef_) #权重系数
    print("==========================")
    print(linreg.intercept_) #截距项
    print("==========================")

    y_hat = linreg.predict(np.array(x_test))  # 输入x_test 的向量 ，得到y_test的标记
    mse = np.average((y_hat - np.array(y_test)) ** 2)  # Mean Squared Error
    rmse = np.sqrt(mse)  # 开根号

    print(mse, rmse)
    print("++++++++++++++++++++++++++++++++")

    t = np.arange(len(x_test))  # 样本中0~199
    plt.plot(t, y_test, "r-", linewidth=2, label="Test")
    plt.plot(t, y_hat, "g-", linewidth=2, label="Predict")
    plt.legend(loc="upper right")
    plt.grid()
    plt.show()
