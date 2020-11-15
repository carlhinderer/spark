
# Get number of CPUs

import os

print('CPUs: ', os.cpu_count())



# Web scraping in parallel

from datetime import date
from multiprocessing import Pool
from urllib import request

def days_between(start,stop):
    today = date(*start)
    stop = date(*stop)
    while today < stop:
        datestr = today.strftime("%m-%d-%Y")
        yield "http://jtwolohan.com/arch-rival-blog/"+datestr
        today = date.fromordinal(today.toordinal()+1)

def get_url(path):
    return request.urlopen(path).read()

# Parallel blog scraping
with Pool() as P:
    blog_posts = P.map(get_url, days_between((2000,1,1), (2011,1,1)))



# Numbers out of order, resulting data structure in order

def print_and_return(x):
    print(x); return x

with Pool() as P:
    P.map(print_and_return, range(20))



# Using state in parallel programming

class FizzBuzzer:
    def __init__(self):
        self.n = 0
  
    def foo(self,_):
        self.n += 1
        if (self.n % 3)  == 0:
            x = "buzz"
        else: 
            x = "fizz"
        print(x)
        return x

FB = FizzBuzzer()

# Classic for loop
for i in range(21):
    FB.foo(i)

# Gives us only fizz and no buzz since FB.n = 0 for all operations
with Pool() as P:
    P.map(FB.foo, range(21))

# Always produce a buzz instead by setting FB.n = 2
FB.n = 2
with Pool() as P:
    P.map(FB.foo, range(21))

# Put the state in some external variable instead
def foo(n):
    if (n % 3) == 0:
        x = "buzz"
    else: 
        x = "fizz"
    print(x)
    return x

with Pool() as P:
    print(P.map(foo, range(1,22)))