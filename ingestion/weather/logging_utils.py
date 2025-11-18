import json
from datetime import datetime

DEF_COMPONENT_KEY = 'component'
DEF_EVENT_KEY = 'event'
DEF_TIMESTAMP_KEY = 'ts_utc'


def json_log(component: str, event: str, **fields):
    record = {DEF_TIMESTAMP_KEY: datetime.utcnow().isoformat(), DEF_COMPONENT_KEY: component, DEF_EVENT_KEY: event}
    if fields:
        record.update(fields)
    print(json.dumps(record, ensure_ascii=False), flush=True)
    return record
