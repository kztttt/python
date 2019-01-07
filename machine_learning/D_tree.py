#!/usr/bin/python
# -*- coding:utf-8 -*-
import matplotlib.pyplot as plt
from sklearn.datasets import load_iris # 导入鸢尾花数据集
from sklearn.tree import  DecisionTreeClassifier # 决策树生成
from sklearn.model_selection import train_test_split # 分割数据集

# 一、加载数据集
iris = load_iris()
#print(iris)

# 二、分割数据集
X_train , X_test  , y_train , y_test =  \
    train_test_split(iris.data , iris.target , random_state=0 )

# 设置超参数max_depth
max_depth_values = [2 , 3 , 4]

for max_depth_val in max_depth_values:
    # 三、选择/建立模型
    dt_model = DecisionTreeClassifier(max_depth=max_depth_val) #设置超参数
    # 四、训练模型
    dt_model.fit(X_train , y_train)
    print("max_depth = ", max_depth_val)
    print("训练集上的准确率:{:.3f}".format(dt_model.score(X_train, y_train)))
    print("测试集上的准确率:{:.3f}".format(dt_model.score(X_test, y_test)))

from machine_learning.ml_visualization import plot_feature_importances

plot_feature_importances(dt_model, iris.feature_names)
plt.show()
