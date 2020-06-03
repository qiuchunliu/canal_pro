
import sys

for i in sys.argv:
    print(i)
a = input("arg1 \n path=")

f = open(a, encoding='utf-8')
lines = f.readlines(100000)
for l in lines:
    print(l)
