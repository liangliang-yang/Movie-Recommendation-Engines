# to run this script in command line:
# !python MostPopularMovie_Name.py --items=ml-100k/u.ITEM ml-100k/u.data > MostPopularMovie_Name.txt

from mrjob.job import MRJob
from mrjob.step import MRStep

class MostPopularMovie(MRJob):

    def configure_options(self):
        super(MostPopularMovie, self).configure_options()
        # The resaon here is that, if we just use the u.data, we will only get the moive id in the end
        # In order to map the movie id to the movie name, we need this file u.item
        self.add_file_option('--items', help='Path to u.item')

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer_init=self.reducer_init,
                   reducer=self.reducer_count_ratings),
            MRStep(mapper = self.mapper_passthrough,
                   reducer = self.reducer_find_max)
        ]

    def mapper_get_ratings(self, _, line):
        (userID, movieID, rating, timestamp) = line.split('\t') # split by tab key
        yield movieID, 1

    # just create a dict for later use
    def reducer_init(self):
        self.movieNames = {} # need to create a dict that store pairs of {movie_id, moive_name}

        with open("u.ITEM", encoding='ascii', errors='ignore') as f:
            for line in f:
                # line in u.item is like: 1 | Toy Story(1995) | 01-Jau-1995 | http:.............
                fields = line.split('|') # so after split, fields[0] = moive id, fileds[1] = moive name
                self.movieNames[fields[0]] = fields[1]

    # Steo=ps of work: (we just care about how many ratings, instead of the detailed rating number 1-5 star)
    # 1. Mapper:  row data -> movie_id, 1:   242:1 302:1 242:1 51:1
    # 2. Group and Sort: mapper result -> 242: 1,1  302: 1  51: 1
    # 3. Reducer 1: None:(2, 242)   None:(1, 302)   None:(1, 51)
    # 4. Reducer 2:     max(values) : 2, 242
    # The above is for return moive id, as for this script, there is also one more step as get extract the moive name and map id-> nam


    # to beeter understand this step:
    # https://www.udemy.com/taming-big-data-with-mapreduce-and-hadoop/learn/v4/t/lecture/3278518?start=0
    def reducer_count_ratings(self, key, values):
        yield None, (sum(values), self.movieNames[key]) # return as tuple(sum(values),   movie_name)

    #This mapper does nothing; it's just here to avoid a bug in some
    #versions of mrjob related to "non-script steps." Normally this
    #wouldn't be needed.
    def mapper_passthrough(self, key, value):
        yield key, value

    def reducer_find_max(self, key, values):
        yield max(values)

if __name__ == '__main__':
    MostPopularMovie.run()



# Some other files, put here as references:

# 1. The script to find min temperature
# class MRMinTemperature(MRJob):

#     def MakeFahrenheit(self, tenthsOfCelsius):
#         celsius = float(tenthsOfCelsius) / 10.0
#         fahrenheit = celsius * 1.8 + 32.0
#         return fahrenheit

#     def mapper(self, _, line):
#         (location, date, type, data, x, y, z, w) = line.split(',')
#         if (type == 'TMIN'):
#             temperature = self.MakeFahrenheit(data)
#             yield location, temperature

#     def reducer(self, location, temps):
#         yield location, min(temps)


# 1. The script to find/group average number of friends by age
# class MRFriendsByAge(MRJob):

#     def mapper(self, _, line):
#         (ID, name, age, numFriends) = line.split(',')
#         yield age, float(numFriends)

#     def reducer(self, age, numFriends):
#         total = 0
#         numElements = 0
#         for x in numFriends:
#             total += x
#             numElements += 1

#         yield age, total / numElements
