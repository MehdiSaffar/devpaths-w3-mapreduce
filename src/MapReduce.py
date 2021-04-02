from collections import defaultdict

class MapReduce:
    def split(self, data):
        """
        Splits the data into chunks
        Must be implemented by child class
        """
        return data

    def process(self, data):
        """
        1. splits the data
        2. maps each chunk
        3. combines intermediate kvs from map
        4. shuffles the intermediate kvs
        5. reduces into finals kvs
        """
        kvs = defaultdict(list)

        chunks = self.split(data)
        # map phase
        for chunk_ix, chunk in enumerate(chunks):
            l_kvs = defaultdict(list)
            for k, v in self.map(chunk_ix, chunk):
                l_kvs[k].append(v)

            # combine
            for k, vs in l_kvs.items():
                fl_k, fl_v = self.reduce(k, vs)
                kvs[fl_k].append(fl_v)
        
        # shuffle phase
        kvs = self.sort_kvs(kvs)

        # reduce phase
        f_kvs = {}
        for k, vs in kvs.items():
            f_k, f_v = self.reduce(k, vs)
            f_kvs[f_k] = f_v

        f_kvs = dict(sorted(f_kvs.items(), key=lambda item: item[1]))
        return f_kvs

    def sort_kvs(self, kvs):
        """
        Utility function to sort dict by keys and sorts each value list
        """
        kvs = dict(sorted(kvs.items(), key=lambda item: item[1]))
        for k, vs in kvs.items():
            kvs[k] = list(sorted(vs))
        
        return kvs

    def map(self, k, v):
        """
        Generator function that yields a list of kv from the input kv
        Must be implemented by child class
        """
        pass
            
    def reduce(self, k, vs):
        """
        Function to reduce the value list into a regular value
        Must be implemented by child class
        """
        pass