#!/usr/bin/env python3
"""Extract real USDOT NAD r24 public address-point records across US states.
This program NEVER uses Stance customers, orders or prospect files.
Data source: https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/Address_Points_from_National_Address_Database_view/FeatureServer/0
"""
import argparse
import csv
import gzip
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from urllib.parse import urlencode
from urllib.request import Request, urlopen

URL = 'https://services.arcgis.com/xOi1kZaI0eWDREZv/ArcGIS/rest/services/Address_Points_from_National_Address_Database_view/FeatureServer/0/query'
STATES = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split()
# The release has additional territories; include them if the service returns records.
STATES += "PR VI AS GU MP".split()
FIELDS = ("OBJECTID","AddNo_Full","StNam_Full","Post_City","State","Zip_Code",
          "Longitude","Latitude","Addr_Type","Lifecycle","UUID","DateUpdate")
OUTPUT_FIELDS = ("source","source_release","source_record_id","address_number",
                 "street","city","state","zip","longitude","latitude",
                 "address_type","lifecycle","source_uuid","date_updated")
BASE = {'f':'json', 'returnGeometry':'false', 'where':'1=1'}

def query(params):
    url = URL+'?'+urlencode(params)
    exc = None
    for attempt in range(4):
        try:
            req = Request(url,headers={'User-Agent':'Stance-NAD-Research/1.0 (public open-data)'})
            with urlopen(req,timeout=75) as resp:
                payload = json.load(resp)
            if 'error' in payload:
                raise RuntimeError(str(payload['error']))
            return payload
        except Exception as e:
            exc=e
            time.sleep(2*(attempt+1))
    raise RuntimeError(str(exc))

def extract(state, maximum):
    where = "State='"+state+"' AND AddNo_Full IS NOT NULL AND StNam_Full IS NOT NULL"
    common = dict(BASE,where=where)
    total = None
    try:
        total = int(query(dict(common,returnCountOnly='true')).get('count',0))
    except Exception as e:
        # Counts may be unavailable on a heavily loaded service; do not misrepresent them as zero.
        count_error = str(e)
    else:
        count_error = None
    try:
        payload = query(dict(common,outFields=','.join(FIELDS),orderByFields='OBJECTID ASC',resultRecordCount=maximum))
        records=[]
        for feature in payload.get('features', []):
            a=feature.get('attributes') or {}
            if not (a.get('AddNo_Full') and a.get('StNam_Full') and a.get('State')):
                continue
            vals=('USDOT_NAD','r24_2026-09-30',str(a.get('OBJECTID') or ''),
                str(a.get('AddNo_Full') or ''),a.get('StNam_Full') or '',
                a.get('Post_City') or '',a.get('State') or '',
                a.get('Zip_Code') or '',a.get('Longitude'),
                a.get('Latitude'),a.get('Addr_Type') or '',
                a.get('Lifecycle') or '',a.get('UUID') or '',a.get('DateUpdate'))
            records.append(vals)
        return state,records,{'state':state,'reported_matching_count':total,'retrieved':len(records),
            'more_exists':bool(payload.get('exceededTransferLimit')) or (total is not None and total>len(records)),
            'error':None,'count_error':count_error}
    except Exception as e:
        return state,[],{'state':state,'reported_matching_count':total,'retrieved':0,
            'more_exists':None,'error':str(e),'count_error':count_error}

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--max-per-state',type=int,default=1200)
    p.add_argument('--workers',type=int,default=4)
    p.add_argument('--output',default='national-address-extract')
    a=p.parse_args()
    if not 1<=a.max_per_state<=2000:
        raise SystemExit('ArcGIS service maxRecordCount is 2000')
    os.makedirs(a.output,exist_ok=True)
    result={}
    with ThreadPoolExecutor(max_workers=a.workers) as pool:
        futs=[pool.submit(extract,state,a.max_per_state) for state in STATES]
        for f in as_completed(futs):
            state, rows, info=f.result()
            result[state]=(rows,info)
            print(state,info,flush=True)
    out=os.path.join(a.output,'NAD_R24_PUBLIC_ADDRESS_RECORDS_US_STATES.csv.gz')
    with gzip.open(out,'wt',newline='',encoding='utf-8') as fp:
        writer=csv.writer(fp)
        writer.writerow(OUTPUT_FIELDS)
        for state in STATES:
            writer.writerows(result[state][0])
    audits=[result[s][1] for s in STATES]
    manifest={'source':URL,'source_release':'r24','release_compiled':'2026-09-30',
        'generated_utc':datetime.now(timezone.utc).isoformat(),
        'record_count':sum(x['retrieved'] for x in audits),'states_with_records':sum(x['retrieved']>0 for x in audits),
        'max_per_state':a.max_per_state,
        'sample_only':True,'not_full_national_archive':True,
        'not_provider_address_qualified':True,'no_customer_records':True,
        'status_by_state':audits}
    with open(os.path.join(a.output,'NAD_R24_EXTRACT_AUDIT.json'),'w') as fp:
        json.dump(manifest,fp,indent=2)
    print('OUTPUT',out,manifest['record_count'],flush=True)
    if manifest['record_count']==0:
        raise SystemExit('No public NAD records returned; refusing to publish empty output')
if __name__=='__main__':
    main()
