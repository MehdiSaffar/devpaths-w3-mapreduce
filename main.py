from src import MapReduce

class WordCounter(MapReduce):
    def split(self, data):
        return data.splitlines()

    def map(self, k, v):
        for w in v.split():
            yield (w, 1)

    def reduce(self, k, vs):
        return (k, sum(vs))

wc = WordCounter()
with open('text.txt') as file:
    kvs = wc.process(file.read())
    print(kvs)