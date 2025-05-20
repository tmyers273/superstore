# This file contains differential testing, comparing the results from sqlite to
# the results from our database.
#
# This is intended to serve as a way of surfacing bugs via fuzzing.
#
# The basic idea is that we run our database and sqlite side by side, performing
# the same operations on both. If there is ever any difference, then we flag that
# as a bug.


def test_differential_testing():
    print("diff test")
