import random as rd
import calendar

x = 1
a = rd.randint(1, 100)
print("猜猜我想的什么数字：")
while (x == 1):
    b = input()
    b = int(b)

    if (b < a):
        print("你输入的数字小了")
    if (b > a):
        print("你输入的数字大了")
    if (b == a):
        print("答对了！")
        x = 2
