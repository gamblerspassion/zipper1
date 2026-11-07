#!/usr/bin/env python3
"""Source-specific Honolulu address extractor: NO customer/lead data.

This is Honolulu data only, not all of Hawaii. Use is subject to the
issuing agency's dataset terms; preserve provenance before commercial use.
"""
import csv,gzip,json,os,random,sys,time
from concurrent.futures import ThreadPoolExecutor,as_completed
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request,urlopen

SOURCE='https://services7.arcgis.com/uFAr0LUPy14bDaLg/ArcGIS/rest/services/Hawaii_LE_WFL1/FeatureServer/2'
OUT=Path(sys.argv[1] if len(sys.argv)>1 else 'honolulu-public-addresses')
OUT.mkdir(parents=True,exist_ok=True)
def get(q):
    url=SOURCE+'/query?'+urlencode({**q,'f':'json'})
    for i in range(5):
        try:
            with urlopen(Request(url,headers={'User-Agent':'STANCE-public-data-coverage-audit/1.0'}),timeout=75) as r:
                d=json.load(r)
            if 'error' in d:raise RuntimeError(str(d['error']))
            return d
        except Exception as e:
            err=e
            time.sleep(3+i*5)
    raise RuntimeError(str(err))

ids=get({'where':'1=1','returnIdsOnly':'true','returnGeometry':'false'}).get('objectIds') or []
ids=sorted(set(int(x) for x in ids))
print('Official Honolulu IDs:',len(ids),flush=True)
if not ids:raise SystemExit('No official identifiers; abort')
chunks=[ids[i:i+500] for i in range(0,len(ids),500)]
def fetch(i,chunk):
    d=get({'objectIds':','.join(map(str,chunk)),'outFields':'FID,housenumbe,houseprfx,housesuffx,streetname,suite,city,zip,geocodeadd,globalid',
           'outSR':'4326','returnGeometry':'true'})
    features=d.get('features') or []
    return i,features
results={}; failures={}
with ThreadPoolExecutor(max_workers=6) as p:
    jobs={p.submit(fetch,i,c):i for i,c in enumerate(chunks)}
    for future in as_completed(jobs):
        i=jobs[future]
        try:
            j,features=future.result()
            results[j]=features
            print('chunk',j,'rows',len(features),flush=True)
        except Exception as e:
            failures[i]=str(e)
            print('FAILED',i,str(e),flush=True)
headers=['source_fid','number','street','unit','city','state','zip','longitude','latitude','full_address','globalid','source','source_issued_by','household_serviceable']
seen=set();written=0;skipped=0
with gzip.open(OUT/'HONOLULU_GIS_ADDRESS_POINTS.csv.gz','wt',newline='',encoding='utf8') as f:
    w=csv.writer(f);w.writerow(headers)
    for i in sorted(results):
        for obj in results[i]:
            a=obj.get('attributes') or {};g=obj.get('geometry') or {}
            fid=a.get('FID')
            if fid is None or fid in seen:continue
            seen.add(fid)
            if not a.get('housenumbe') or not a.get('streetname'):
                skipped+=1;continue
            w.writerow([fid,str(a.get('houseprfx') or '')+str(a['housenumbe'])+str(a.get('housesuffx') or ''),
             a['streetname'],a.get('suite') or '',a.get('city') or '','HI',a.get('zip') or '',g.get('x'),g.get('y'),
             a.get('geocodeadd') or '',a.get('globalid') or '',SOURCE,
             'City and County of Honolulu Department of Planning and Permitting','NOT_VERIFIED'])
            written+=1
audit={'source':SOURCE,'source_jurisdiction':'City and County of Honolulu, Hawaii','source_date_checked_utc':datetime.now(timezone.utc).isoformat(),
 'source_object_id_count':len(ids),'retrieved_object_ids':len(seen),'address_records_written':written,
 'records_excluded_missing_address':skipped,'chunks_expected':len(chunks),'chunks_succeeded':len(results),
 'failed_chunks':failures,'statewide_hawaii_complete':False,'commercial_use_rights_review_needed':True,
 'customer_records_used':0,'provider_serviceability_verified':False,
 'status':'COMPLETE' if not failures and len(seen)==len(ids) and written>0 else 'INCOMPLETE'}
(OUT/'HONOLULU_ADDRESS_AUDIT.json').write_text(json.dumps(audit,indent=2))
print(json.dumps(audit,indent=2),flush=True)
if audit['status']!='COMPLETE':raise SystemExit(2)
