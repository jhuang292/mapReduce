import MapReduce
import sys

"""
JOIN in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

# Implement the MAP function
def mapper(record):
    key = record[1]
    value = record
    mr.emit_intermediate(key, value)
    

# Implement the REDUCE function
def reducer(key, list_of_values):
    order_of_list, line_item_of_list = [], []
    for v in list_of_values:
        if len(v) == 10:
            order_of_list.append(v)
        else:
            line_item_of_list.append(v)

    for item1 in order_of_list:
        for item2 in line_item_of_list:
            mr.emit((item1 + item2))


    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
