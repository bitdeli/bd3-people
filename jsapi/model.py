import json
from itertools import islice, chain
from datetime import datetime
from bitdeli.model import model, segment_model

MAX_URL = 64
MAX_EVENTS = 10

# Customize to hide domain from page views
# Example: "bitdeli.com"
URL_DOMAIN = ""

def get_event_name(event):
    name = event.get('$event_name', None)
    if name == '$dom_event':
        name = event.get('$event_label', None)
    elif name == '$pageview':
        if not event.get('$page', ''):
            return
        url = event['$page']
        splitter = URL_DOMAIN if URL_DOMAIN else 'http://'
        if splitter in url:
            url = url.split(splitter, 1)[1]
        name = ('...' + url[-MAX_URL:]) if len(url) > MAX_URL else url
    if name:
        return name

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