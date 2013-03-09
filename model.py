import json
from itertools import chain
from datetime import datetime
from bitdeli.model import model, segment_model

def newest(attr, top=1):
    items = ((iter(hours).next()[0], value)
             for value, hours in attr.iteritems())
    if top == 1:
        return max(items)[1]
    else:
        return [(value, datetime.utcfromtimestamp(hour * 3600).isoformat())
                for hour, value in sorted(items)][-top:]

def attributes(properties):
    for key, prop in properties.iteritems():
        if key[0] == '$' or key[:3] == 'mp_':
            yield key, newest(prop)
        elif 'email' in key:
            maybe_email = newest(prop)
            if '@' in maybe_email:
                yield key, maybe_email
                
@model
def build(profiles):
    for profile in profiles:
        for key, val in chain(attributes(profile['properties']),
                              [('events', newest(profile['events'], top=5))]):
            yield profile.uid, json.dumps((key, val))
            
@segment_model
def segment(model, segments, labels):
    class SegmentModel(object):
        def items(self):
            segs = list(sorted(segments, key=lambda s: len(s)))
            rest = segs[1:]
            for key in segs[0]:
                for seg in rest:
                    if key not in seg:
                        break
                else:
                    yield key, model[key]
    return SegmentModel()