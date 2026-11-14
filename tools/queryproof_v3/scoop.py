import json, subprocess, urllib.parse, urllib.request, sys

SINCE = "2026-10-15"
JIRA = {
    "N1": ['project=SPARK AND text ~ "dynamicpruning" AND text ~ "UNION"',
           'project=SPARK AND text ~ "has not finished"'],
    "N2": ['project=SPARK AND text ~ "FilterEstimation"', 'project=SPARK AND text ~ "NumberFormatException" AND text ~ "cbo"'],
    "G":  ['project=SPARK AND text ~ "1582" ', 'project=SPARK AND text ~ "calendar gap"'],
    "Z1": ['project=SPARK AND text ~ "TIMESTAMP_MILLIS"', 'project=SPARK AND text ~ "timestampToMillis"',
           'project=SPARK AND text ~ "millisecond" AND text ~ "pushdown"'],
    "S":  ['project=ORC AND text ~ "UTF-16"', 'project=ORC AND text ~ "surrogate"', 'project=ORC AND text ~ "string statistics" '],
    "57956": ['key = SPARK-57956'], "57499": ['key = SPARK-57499'], "59607": ['key = SPARK-59607'],
}
GH = {
    "T3": [("trinodb/trino", "ORC IS NULL nested row"), ("trinodb/trino", "ORC row null struct field is null")],
    "TD": [("trinodb/trino", "byte overflow"), ("trinodb/trino", "DELTA_BINARY_PACKED"), ("trinodb/trino", "short overflow")],
    "C2": [("trinodb/trino", "cannot use empty rangeList"), ("trinodb/trino", "Corrupted column index")],
    "A1": [("apache/arrow", "ORC struct nested"), ("apache/arrow", "ORC writer struct null")],
    "X1": [("apache/datafusion", "evaluate_partition_prefix"), ("apache/datafusion", "partition prefix equality")],
    "X2": [("apache/datafusion", "with_statistics partition"), ("apache/datafusion", "empty file partition max")],
    "W1": [("apache/arrow-rs", "fastparquet"), ("apache/arrow-rs", "Expected list element type")],
    "Y1": [("apache/datafusion-python", "created_by")],
}

def jira(jql):
    q = urllib.parse.urlencode({"jql": f"({jql}) AND (created >= {SINCE} OR key in (SPARK-57956,SPARK-57499,SPARK-59607))",
                                "fields": "summary,created,status,resolution,fixVersions,updated", "maxResults": 20})
    with urllib.request.urlopen("https://issues.apache.org/jira/rest/api/2/search?" + q, timeout=60) as r:
        return json.load(r).get("issues", [])

def gh(repo, q, kind):
    time.sleep(2.6)
    out = subprocess.run(["gh", "search", kind, "--repo", repo, q, "--created", f">={SINCE}", "--limit", "15",
                          "--json", "number,title,state,createdAt,url"], capture_output=True, text=True)
    if out.returncode: print("   GH ERROR", repo, q, out.stderr[:120])
    return json.loads(out.stdout or "[]")

import time
for k, qs in ({} if "gh" in sys.argv else JIRA).items():
    seen = {}
    for q in qs:
        try:
            for i in jira(q):
                f = i["fields"]; seen[i["key"]] = f"{f['created'][:10]} {f['status']['name']} {(f.get('resolution') or {}).get('name','')} | {f['summary']} | fix {[v['name'] for v in f.get('fixVersions') or []]} upd {f['updated'][:10]}"
        except Exception as e:
            seen["ERR " + q] = str(e)
    print(f"== {k}"); [print("  ", a, b) for a, b in seen.items()]
for k, qs in GH.items():
    seen = {}
    for repo, q in qs:
        for kind in ("issues", "prs"):
            for i in gh(repo, q, kind):
                seen[i["url"]] = f"{i['createdAt'][:10]} {i['state']} {i['title']}"
    print(f"== {k}"); [print("  ", a, b) for a, b in seen.items()]
