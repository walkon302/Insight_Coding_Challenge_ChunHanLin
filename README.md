# Table of contents

1. My Approach
2. Dependencies

# My Approach

To detect the anomaly of purchase events within D degree of social network and T numbers of purchase events, I need two information, which are
1. The D degree of social network of the person, who made the purchase.
2. The latest at most T purchase events made by those friends in D degree of the social network.

I declared 2 classes, one is Person, which stores the information of each person's friends, events of purchases, and ID; and Purchase, which stores the amount of purchase, and the order of purchase event.

Initially, I builded a dictionary of persons storing information based on iterating the first batch_log file. Then, I iterated the stream_log for updating person's information and identifying anomaly of purchase. For identifying anomaly of purchase, I first collected all friends whthin D degree of social network of the person who made the purchase, then, collected all purchase events made by these friends and calculate the mean and standard deviation of these purchases, and finally determined whether this purchase is anomaly compared to other purchase events.

The final output is a file containing a list of flagged anomaly of purchases. The people_list is one output I don't use here, but it can be useful for future functions. For example, if we want to know how many friends and purchases certain person has. Then we may use this to identify who we may recommend.

# Dependencies
I imported python's internal libraries, json, os, and sys.
