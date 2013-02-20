from bitdeli.insight import insight
from bitdeli.widgets import Table
from discodb.query import Q, Literal, Clause
from itertools import islice
import json

def rows(model):
    for uid, values in islice(model.items(), 50):
        rows = {' uid': uid}
        rows.update(tuple(json.loads(value)) for value in values)
        yield rows

@insight
def view(model, params):
    return [Table(label='people',
                  size=(12, "auto"),
                  data=list(rows(model)))]
                  
        
    