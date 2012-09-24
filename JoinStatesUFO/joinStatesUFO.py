from mrjob.job import MRJob
from math import sqrt
import json


class joinStatesUFO(MRJob):
    DEFAULT_PROTOCOL = 'json'
    states = ['AL','AK','AZ','AR','CA','CO','CT','DE','DC','FL','GA','HI','ID','IL','IN','IA','KS','KY','LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND','OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY']
    
    def mapper(self, key, line):
        dic = {'sightings':0,'name':'','inhb':0}
        try:
            num = json.loads(line)
            if 'location' in num: # UFO data
                dic['sightings'] = 1
                state = num['location']
                state = state[len(state)-2:].upper()                
                if state not in self.states:
                    state = 'Unknown'
            else: # State data
                inhb = float(num['inhb'])*1000.0
                dic['name'] = num['name']
                dic['inhb'] = inhb
                state = num['state']
            yield state,dic
        except ValueError:
            yield 'Error',dic
  
    def reducer(self, state, vars):
        sightings = 0.0
        dic = {'state':state,'inhb':'','ratio':0,'sightings':0}
        for x in vars:
            sightings += x['sightings']
            if x['name'] != '':
                dic['inhb'] = x['inhb']
        dic['sightings'] = sightings
        if dic['inhb'] != '':
            dic['ratio'] = sightings/float(dic['inhb'])
            yield dic['ratio'],dic

if __name__ == '__main__':
    mrMeanVar.run()
