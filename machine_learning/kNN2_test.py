#!/usr/bin/python
# -*- coding:utf-8 -*-
import numpy as np # 科学计算库
import pandas as pd # 转换表格的形式
import matplotlib.pyplot as plt # 可视化方法
import seaborn as sns # 高级的可视化工具
import graphviz
from sklearn.model_selection import train_test_split # 分割数据集

# 一、加载数据集
fruits_df = pd.read_table('../data/fruit_data_with_colors.txt')
print(fruits_df.head(5))

print("===============================================")
print('样本个数：', len(fruits_df))
print("===============================================")

# 创建目标标签和名称的字典
fruit_name_dict = dict(zip(fruits_df['fruit_label'], fruits_df['fruit_name']))
print(fruit_name_dict)
print("===============================================")

# 二、划分数据集
X = fruits_df[['mass', 'width', 'height', 'color_score']] # 建立特征矩阵
y = fruits_df['fruit_label'] #建立标签矩阵
X_train, X_test, y_train, y_test = train_test_split(X, y,
        test_size=1/4, random_state=0) #保证随机抓取结果是一样的

print('数据集样本数：{}，训练集样本数：{}，测试集样本数：{}'
      .format(len(X), len(X_train), len(X_test)))

print("===============================================")
# 可视化查看数据集
#sns.pairplot(data=fruits_df, hue='fruit_name', vars=['mass', 'width', 'height', 'color_score'])
#plt.show()

# 3D查看
from mpl_toolkits.mplot3d import Axes3D

label_color_dict = {1: 'red', 2: 'green', 3: 'blue', 4: 'yellow'}
colors = list(map(lambda label: label_color_dict[label], y_train))

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
ax.scatter(X_train['width'], X_train['height'], X_train['color_score'], c=colors, marker='o', s=100)
ax.set_xlabel('width')
ax.set_ylabel('height')
ax.set_zlabel('color_score')
#plt.show()

#3.建立/选择模型
from sklearn.neighbors import KNeighborsClassifier

knn = KNeighborsClassifier(n_neighbors=5) # k=5

#4.训练模型:输入的是训练集合  ，x_train , y_train ，模型已经训练好了

knn.fit(X_train ,y_train)

#5测试模型
y_pred = knn.predict(X_test) #测试特征的样本

print(y_pred)
print('+++++++++++++++++++++++++++++++')
from sklearn.metrics import accuracy_score #评价指标中 ，准确率

acc = accuracy_score(y_test ,y_pred)
print("准确率:" , acc)
print('+++++++++++++++++++++++++++++++')

k_range = range(1,20)  #超参数是1~19
acc_scores = []
for k in k_range:
    knn=KNeighborsClassifier(n_neighbors=k) #建立/选择模型
    knn.fit(X_train ,y_train) #训练模型
    acc_scores.append(knn.score(X_test ,y_test))
plt.figure()#初始化模板
plt.xlabel("k") #超参数
plt.ylabel("accuracy") #准确率
plt.scatter(k_range ,acc_scores )
plt.xticks([0,5,10,15,20]) #在x轴的间隔
#plt.show()

# 查看width和height两列特征
from machine_learning.ml_visualization import plot_fruit_knn
plot_fruit_knn(X_train,y_train,1)
plot_fruit_knn(X_train,y_train,5)
plot_fruit_knn(X_train,y_train,10)
plt.show()