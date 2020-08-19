
# Simplest map example

import re

class PhoneFormatter:
  def __init__(self):
    self.r = re.compile(r"\d")

  def pretty_format(self, phone_number):
    phone_numbers = self.r.findall(phone_number)
    area_code = "".join(phone_numbers[-10:-7])
    first_3 = "".join(phone_numbers[-7:-4])
    last_4 = "".join(phone_numbers[-4:len(phone_numbers)])
    return "({}) {}-{}".format(area_code, first_3, last_4)


phone_numbers = [
  "(123) 456-7890",
  "1234567890",
  "123.456.7890",
  "+1 123 456-7890"
]

P = PhoneFormatter()
print(list(map(P.pretty_format, phone_numbers)))



# Scraping your rival's blog

from datetime import date
from urllib import request

# Generator to yield all the URLs to check for blog posts
def days_between(start, stop):
    today = date(*start)
    stop = date(*stop)
    while today < stop:
        datestr = today.strftime("%m-%d-%Y")
        yield "http://jtwolohan.com/arch-rival-blog/"+ datestr
        today = date.fromordinal(today.toordinal()+1)

# Get the html for a url
def get_url(path):
  return request.urlopen(path).read()

# Get all the blog posts in a range
blog_posts = map(get_url,days_between((2000,1,1),(2011,1,1)))