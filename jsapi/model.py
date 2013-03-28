import json
from itertools import islice
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
        first = True
        events = []
        for tstamp, group, ip, event in islice(profile['events'], 5):
            events.append((event.pop('$event_name'), tstamp))
            if first:
                first = False
                for key, val in event.iteritems():
                    yield profile.uid, json.dumps((key, val))
        yield profile.uid, json.dumps(('events', events))
            
@segment_model
def segment(model, segments, labels):
    print 'labels %s' % str(labels)
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