#!/usr/bin/env python3
"""Stream official 2026 USDOT NAD national TXT.zip into per-state compressed CSVs.
No Stance customers, contacts, orders, leads, or prospect files are read.
These are government address locations, NOT provider-qualified premises.
"""
import sys,csv,io,gzip,zipfile,json,hashlib,re,os
from pathlib import Path
from collections import Counter
from datetime import datetime,timezone

URL="https://data.transportation.gov/download/fc2s-wawr/application/x-zip-compressed"
STATES=set("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY PR VI AS GU MP".split())
FIELDS=["nad_object_id","house_number","street","city","state","zip","lon","lat","address_type","lifecycle","uuid","release"]
ALIASES={"id":["OID_","OBJECTID"],"house":["AddNo_Full","Add_Number"],"street":["StNam_Full","St_Name"],"city":["Post_City"],"state":["State"],"zip":["Zip_Code"],"lon":["Longitude"],"lat":["Latitude"],"type":["Addr_Type"],"lifecycle":["Lifecycle"],"uuid":["UUID"]}
def key(s): return re.sub(r'[^a-z0-9]','',s.lower().lstrip('\ufeff'))
def main(source,dest):
    source=Path(source);dest=Path(dest);dest.mkdir(parents=True,exist_ok=True)
    if not zipfile.is_zipfile(source):raise ValueError('Not a ZIP archive')
    sha=hashlib.sha256()
    with source.open('rb') as f:
        while chunk:=f.read(8*1024*1024):sha.update(chunk)
    count=Counter(); excluded=Counter(); opened={};total=0
    try:
        with zipfile.ZipFile(source) as archive:
            members=[x for x in archive.infolist() if x.file_size>1000 and x.filename.lower().endswith(('.txt','.csv'))]
            if not members:raise ValueError('No TXT/CSV source found')
            for member in members:
                print('Reading',member.filename,'bytes',member.file_size,flush=True)
                with archive.open(member) as f:
                    stream=io.TextIOWrapper(f,encoding='utf-8-sig',errors='replace',newline='')
                    header=stream.readline()
                    delim='\t' if header.count('\t')>header.count(',') else ','
                    def lines():
                        yield header
                        yield from stream
                    reader=csv.DictReader(lines(),delimiter=delim)
                    schema={key(h):h for h in reader.fieldnames or []}
                    cols={kind:next((schema[key(v)] for v in choices if key(v) in schema),None) for kind,choices in ALIASES.items()}
                    if any(not cols[k] for k in ('id','house','street','state')):raise ValueError('Unexpected NAD columns: '+repr(reader.fieldnames))
                    def get(r,k):return str(r.get(cols[k]) or '').strip() if cols[k] else ''
                    for r in reader:
                        total+=1
                        state=get(r,'state').upper();house=get(r,'house');street=get(r,'street')
                        if state not in STATES or not house or not street:
                            excluded['invalid_state_or_address']+=1;continue
                        zip_code=get(r,'zip')
                        if zip_code and not re.fullmatch(r'\d{5}(-\d{4})?',zip_code):zip_code=''
                        if state not in opened:
                            f=gzip.open(dest/f'NAD_R24_{state}_REAL_ADDRESSES.csv.gz','wt',newline='',encoding='utf8')
                            writer=csv.writer(f);writer.writerow(FIELDS);opened[state]=(f,writer)
                        row=[get(r,'id'),house,street,get(r,'city'),state,zip_code,get(r,'lon'),get(r,'lat'),get(r,'type'),get(r,'lifecycle'),get(r,'uuid'),'2026-09-30']
                        opened[state][1].writerow(row);count[state]+=1
                        if total%1_000_000==0:print('Read',total,'valid',sum(count.values()),flush=True)
    finally:
        for f,_ in opened.values():f.close()
    audit={'official_source':URL,'sha256':sha.hexdigest(),'source_release':'2026-09-30','raw_rows':total,'valid_rows':sum(count.values()),'states':dict(sorted(count.items())),'excluded':dict(excluded),'customer_records':0,'address_serviceability_verified':False,'generated_utc':datetime.now(timezone.utc).isoformat()}
    (dest/'NAD_R24_AUDIT.json').write_text(json.dumps(audit,indent=2))
    print(json.dumps(audit,indent=2),flush=True)
    if not count:raise SystemExit('No official records imported')

if __name__=='__main__':
    if len(sys.argv)!=3:raise SystemExit('Usage: import_nad_full.py official-NAD-r24.zip output-directory')
    main(sys.argv[1],sys.argv[2])
