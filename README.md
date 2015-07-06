# RecMR
the recommendation system is written by python, it can run hadoop cluster if your computer installed the mrjob package.

the run sequence is as followed
1. python loadToMongo.py 
2. python readFromMongo.py
3. python itemSame.py or cf.py -r hadoop input.txt >>results.txt
4. python formingJson.py


#liveRec
recommend live show based the data of huan dot com



