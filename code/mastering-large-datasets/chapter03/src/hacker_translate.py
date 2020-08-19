
# Hacker Translate


# Helper Functions

def replace_7t(s):
    return s.replace('7','t')

def replace_3e(s):
    return s.replace('3','e')

def replace_6g(s):
    return s.replace('6','g')

def replace_4a(s):
    return s.replace('4'.,'a')


# Chinese character matcher
import re

class ChineseMatcher:

    def __init__(self):
        self.r = re.compile(r'[\u4e00-\u9fff]+')

    def sub_chinese(self,s):
        return self.r.sub(s, " ")



# Chaining with sequence of map operations
C = chinese_matcher()

map(C.sub_chinese,
        map(replace_4a,
            map(replace_6g,
                map(replace_3e,
                    map(replace_7t, sample_messages)))))



# Chaining with compose
from toolz.functoolz import compose

hacker_translate = compose(C.sub_chinese, replace_4a, replace_6g,
                           replace_3e, replace_7t)

map(hacker_translate, sample_messages)



# Chaining with pipe
from toolz.functoolz import pipe

def hacker_translate(s):
        return pipe(s, replace_7t, replace_3e, replace_6g,
                       replace_4a, C.sub_chinese)

map(hacker_translate, sample_messages)