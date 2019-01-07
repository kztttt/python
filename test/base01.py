#!/usr/bin/python
# -*- coding:utf-8 -*-

# 1.Python常用的容器类型

# 1.1 list
l = [1,3,"baidu","新浪"]

# (type(l)) print (l)


# 2.python字符串操作

s = "算法工程师"

print("索引操作：") #从0个元素下表，不包括终止值

print("s[0]：" , s[-2:])

s2 = "hello kzt are you ok ?"
print(s2)

# python的日期处理
import datetime as dt
import time as tm

print("当前时间：", tm.time())

dt_now = dt.datetime.fromtimestamp(tm.time())
print(dt_now)

print("{}年{}月{}日".format(dt_now.year ,dt_now.month ,dt_now.day))

# 日期时间
delta = dt.timedelta(days = 100)
print("今天的前100天时间为：",dt_now.today() - delta) # 今天的前100天的日期
print(dt.date.today() >dt.date.today()-delta)

#3 map()函数

import math

l1=[5,7,9]
l2=[10,7,3]

mins = map(min ,l1 ,l2)

print(mins) #返回的是内存地址，因为没有访问到数据

for item in mins:
    print(item)

squared = map(math.sqrt , l2)
print(squared)
print(list(squared))

# 匿名函数lambda
# :的前面是匿名函数
# :的后面是方法体
my_min = lambda a,b,c:a * b
print(my_min(4,5,6))

l1 = []
for i in range(1000):
    if i%2 == 0:
        l1.append(i)
#print(l1)

l2 = [i for i in range(50) if i%2 ==0 ]
print(l2)

