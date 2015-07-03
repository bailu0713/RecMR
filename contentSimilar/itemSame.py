#coding:UTF-8
from mrjob.job import MRJob
import copy

class step(MRJob):
    '''
    '''
    def parseMatrix(self, _, line):
#         str(videoName)+'|'+str(cid)+'|'+str(directors)+'|'+str(actors)+'|'+str(year)+'|'+str(description)+'|'+str(comment)+'|'+str(country)+'|'+str(types)
        line = (str(line))
        content = line.split('||')
        
        cid = content[0]
        actors = content[4]
#         year = content[2]
        types = content[3]
        description = content[1]
        directors = content[5]
        directorlevel = eval(content[6])
        actorlevel = eval(content[8])
        descriptionlevel = eval(content[7])
        if len(directors)!=0 and directors!='无':
            directors = directors.split(',')
            for director in directors:
                yield (director.decode('utf8'), types.decode('utf8'), 'directors;'), (cid.decode('utf8'),directorlevel,'directors;')
        if len(actors)!=0 and actors!='无':
            actors = actors.split(',')
            for actor in actors:
                yield (actor.decode('utf8'), types.decode('utf8'), 'actors;'), (cid.decode('utf8'), actorlevel,'actors;')   
        
        if len(description)!=0 and description!='无':
            description = description.split(',')
            for descript in description:
                yield (descript.decode('utf8'), types.decode('utf8'), 'description;'), (cid.decode('utf8'), descriptionlevel,'description;')
                            
    def contentSimilar(self, uid, cid):
        temp = list()
        for i in cid:
            if i not in temp:
                temp.append(i)
#         temp = tuple(temp)
        for i in temp:
            temptemp = copy.copy(temp)
            
            temptemp.remove(i)
            yield i, temptemp
    
    def rank(self, original, targets):
        original = original[0]
        temp = dict()
        reason = dict()
#         nameDict[original[0]] = original[1]
        for target in targets:
            for i in target:
                if i[0] in temp and i[0] in reason:
                    reason[i[0]] += i[2]
                    temp[i[0]]+=i[1]
                elif i[0] not in temp and i[0] not in reason:
                    reason[i[0]] = i[2]
                    temp[i[0]]=i[1]
        # judge if similar target exists
        if len(temp)>0:
            temp = sorted(temp.items(), key = lambda d:d[1], reverse = True)
            count = 0
            indexList = list()
            scoreList = list()
            reasonList = list()
            for index, score in temp:
                if count>=10:
                    break
                if score<=0:
                    break
                if index == original:
                    continue
                indexList.append(str(index))
                scoreList.append(str(score))
                #reasonList.append(str(reason[index]))
                count+=1
            writestr = str(original)+'//'+','.join(indexList)+'//'+','.join(scoreList)+'//'+','.join(reasonList)
            print writestr 
    def steps(self):
        return [self.mr(mapper = self.parseMatrix,
                reducer = self.contentSimilar),
                self.mr(reducer = self.rank)]
    
if __name__=='__main__':
    

    step.run()
