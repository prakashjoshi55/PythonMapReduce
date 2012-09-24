from mrjob.job import MRJob

class MRWordFreqCount(MRJob):

    def mapper(self, _, line):
        comma_pos = line.find(',')
        id = line[:comma_pos]
        line = line[comma_pos+1:]
        for value in line.split(','):
            yield (id, value)      

    def reducer(self, id, values):
        total = 0.0
        number = 0.0
        for value in values:
            total += float(value)
            number += 1.0
        mean = round(total/number, 2)
        yield (id, mean)

if __name__ == '__main__':
    MRWordFreqCount.run()


