#!/usr/bin/env python3
"""Offline audit of the original capture; only Python standard library."""
import gzip,json,sys
with gzip.open(sys.argv[1] if len(sys.argv)>1 else "capture.json.gz","rt") as f:d=json.load(f)
def identity(x): return tuple(x[k].lower() for k in ("blockHash","transactionHash","logIndex"))
def payload(x): return (x["address"].lower(),tuple(t.lower() for t in x["topics"]),x["data"].lower(),x["blockNumber"],x["transactionIndex"])
a={identity(x):payload(x) for x in d["canonicalLogs"]};b={identity(x):payload(x) for x in d["backfillLogs"]}
lo,hi=d["range"]["from"],d["range"]["to"]
live=[x for x in d["liveLogs"] if lo<=int(x["blockNumber"],16)<=hi and not x.get("removed")]
c={identity(x) for x in live}
assert a==b
print(json.dumps({"canonical":len(a),"backfill":len(b),"fullNormalizedPayloadsMatch":a==b,"rawLive":len(d["liveLogs"]),"inRangeLive":len(live),"inRangeUnique":len(c),"absentFromLive":len(set(a)-c),"extraLive":len(c-set(a)),"union":len(c|set(b))},indent=2))
