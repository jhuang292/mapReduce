import MapReduce
import sys

"""
Matrix Multiply in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

# Implement the MAP function
def mapper(record):
    if record[0] == 'a':
        key = record[1]
        value = record
        mr.emit_intermediate(key, value)
    else:
        mr.emit_intermediate(0, record)
        mr.emit_intermediate(1, record)
        mr.emit_intermediate(2, record)
        mr.emit_intermediate(3, record)
        mr.emit_intermediate(4, record)

# Implement the REDUCE function
def reducer(key, list_of_values):
    a_list = list_of_values[:5]
    b_list = list_of_values[5:]
    total = 0
    for i in range(5):
        for j in range(5):
            total += a_list[j][-1] * b_list[5*j+i][-1]
        mr.emit((key, i, total))
        total = 0


# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
