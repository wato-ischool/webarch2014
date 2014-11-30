"""Find users with > 20 visits.

This program will take a CSV data file and output tab-seperated lines of

    word -> number of visits

To run:

    python top_title_words.py anonymous-msweb.data

To store output:

    python top_title_words.py anonymous-msweb.data > top_title_words.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline

class TopTitleWords(MRJob):

    def mapper_get_words(self, line_no, line):
        """Extracts the user and number of visits"""
        cell = csv_readline(line)
        if cell[0] == 'A':
            for word in cell[3].lower().split():
                yield word, 1 ### FILL IN
                      # What  Key, Value  do we want to output?

    def reducer_count_words(self, word, word_counts):
        """Sumarizes the word counts by adding them together."""
        total = sum(word_counts) ### FILL IN
                # How do we calculate the total words from the word_counts?
        yield None, (total, word) ### FILL IN
                # What  Key, Value  do we want to output?
        
    def reducer_max_words(self, _, word_and_count):
        top_title_words = sorted(word_and_count, reverse=True)
        for i in range(10):
            yield top_title_words[i]

    # from http://pythonhosted.org/mrjob/guides/quickstart.html#writing-your-second-job
    def steps(self):
        return [
            self.mr(mapper=self.mapper_get_words,
                    reducer=self.reducer_count_words),
            self.mr(reducer=self.reducer_max_words)
        ]

if __name__ == '__main__':
    TopTitleWords.run()
