#!/usr/bin/python
# -*- coding:utf-8 -*-
import numpy as np #科学计算库
import pandas as pd #转换表格
import matplotlib.pyplot as plt # 可视化法
import pandas as pd
import seaborn as sns
from sklearn.model_selection import train_test_split # 分割数据集
from sklearn.metrics import accuracy_score # 准确率得分
from sklearn.linear_model import LogisticRegression # 调库 逻辑斯蒂回归
from machine_learning.ml_visualization import plot_class_regions_for_classifier # 可视化分类

# 一、加载数据集
fruits_df = pd.read_table("../data/fruit_data_with_colors.txt")

X = fruits_df[['width', 'height']]

y = fruits_df['fruit_label'].copy()

"""
将不是apple的标签设为0，apple—>1  , 橙子—>0  , 橘子—>0  ,柠檬—>0  

"""

y[y !=1] = 0 # 2 3 4 -> 0

# 二、分割数据集
X_train, X_test, y_train, y_test = \
    train_test_split(X, y, test_size=1/4, random_state=0)

# 设置不同的超参数C
c_values = [0.1 , 1 , 100]

for c_value in c_values:
    # 三、建立模型
    lr_model = LogisticRegression(C = c_value)

    # 四、训练模型
    lr_model.fit(X_train,y_train)

    # 五、验证模型
    y_pred = lr_model.predict(X_test)

    # 六、应用模型（打分）
    acc = accuracy_score(y_test ,y_pred)
    print('C={}，准确率：{:.3f}'.format(c_value, acc))

    # 可视化
    plot_class_regions_for_classifier\
        (lr_model, X_test.values, y_test.values, title='C={}'.format(c_value))


