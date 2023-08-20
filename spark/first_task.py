#!/usr/bin/env python3.6

import re
rdd = sc.textFile("/data/wiki/en_articles_part")

rdd2 = rdd.map(lambda x: re.sub("^\W+|\W+$", "", x.strip().lower()))
rdd3 = rdd2.map(lambda x: re.sub(" ", "_", x.strip()))
bigrams = rdd3.map(lambda x: re.split(r'narodnaya_[a-z]+', x))
bigrams.take(10)
