"""Find users with > 20 visits.

This program will take a CSV data file and output tab-seperated lines of

    users -> number of visits

To run:

    python top_users.py user-visits_msweb.data

To store output:

    python top_users.py user-visits_msweb.data > top_users.out
"""

from mrjob.job import MRJob
from combine_user_visits import csv_readline

class TopUsers(MRJob):

    def mapper(self, line_no, line):
        """Extracts the user and number of visits"""
        cell = csv_readline(line)
        if cell[0] == 'V':
            yield cell[3], int(cell[2]) ### FILL IN
                  # What  Key, Value  do we want to output?

    def reducer(self, user, visit_counts):
        """Sumarizes the visit counts by adding them together.  If total visits
        is more than 20, yield the results"""
        total = sum(visit_counts) ### FILL IN
                # How do we calculate the total visits from the visit_counts?
        if total > 20:
            yield user, total ### FILL IN
                  # What  Key, Value  do we want to output?
        
if __name__ == '__main__':
    TopUsers.run()
