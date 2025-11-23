importjson
fromdatetimeimportdatetime

DEF_COMPONENT_KEY='component'
DEF_EVENT_KEY='event'
DEF_TIMESTAMP_KEY='ts_utc'


defjson_log(component:str,event:str,**fields):
    record={DEF_TIMESTAMP_KEY:datetime.utcnow().isoformat(),DEF_COMPONENT_KEY:component,DEF_EVENT_KEY:event}
iffields:
        record.update(fields)
returnrecord
