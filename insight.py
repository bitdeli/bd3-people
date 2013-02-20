from bitdeli.insight import insight
from bitdeli.widgets import Users
from discodb.query import Q, Literal, Clause
from itertools import islice
import json

def rows(model):
    for uid, values in islice(model.items(), 50):
        rows = {'uid': uid}
        rows.update(tuple(json.loads(value)) for value in values)
        yield rows

@insight
def view(model, params):
    data = list(rows(model))
    return [Users(label='%d users' % len(data),
                  size=(12, "auto"),
                  data=data)]
                  
        
    