import MapReduce
import sys

"""
Inverted Index Example in the Simple Python MapReduce Framework
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

# Implement the MAP function
def mapper(doc_list):
    document_id = doc_list[0]
    text = doc_list[1]
    tokens = text.split()
    for token in tokens:
        mr.emit_intermediate(token, document_id)

# Implement the REDUCE function
def reducer(key, value):
    mr.emit([key,list(set(value))])
    

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
