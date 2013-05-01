import json
from itertools import islice, chain
from datetime import datetime
from urlparse import urlparse
from bitdeli.model import model, segment_model

MAX_LEN = 32
MAX_EVENTS = 10

def get_event_name(event):
    name = event.get('$event_name')
    if name == '$dom_event':
        name = event.get('$event_label')
    elif name == '$pageview':
        name = urlparse(event.get('$page', '')).path
    if name:
        return name[:MAX_LEN].encode('utf-8')

@model
def build(profiles):
    for profile in profiles:
        first = True
        source_events = sorted(chain(islice(profile.get('events', []), MAX_EVENTS),
                                     islice(profile.get('$pageview', []), MAX_EVENTS)),
                               key=lambda e: e[0],
                               reverse=True)
        events = []
        for tstamp, group, ip, event in source_events:
            event_name = get_event_name(event)
            if event_name and len(events) < MAX_EVENTS:
                events.append((event_name, tstamp))
            if first:
                first = False
                yield profile.uid, json.dumps(('ip', ip))
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