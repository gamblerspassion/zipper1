#!/usr/bin/env python3
"""Index public NAD and supplementary address records by address and FCC H3 r8 cell.

Produces a queryable state SQLite file; preserves address-unit distinctions.
Missing addresses can be geocoded at lookup time rather than treated as absent.
Overture proprietary licenses and OSM ODbL are separately governed: do not silently
relicense or publish sources with unresolved downstream rights.
"""
from __future__ import annotations
import argparse, csv, gzip, json, re, sqlite3, sys, hashlib, zipfile
from pathlib import Path
from collections import Counter
from datetime import datetime, timezone

COLUMNS='source_key house street unit city state zipcode latitude longitude h3_8 source license_status'.split()

def utc():return datetime.now(timezone.utc).isoformat()
def clean(x):return re.sub(r'\s+',' ',str(x or '').strip())
def norm(x):return re.sub('[^A-Z0-9]','',str(x or '').upper())
def flo(x):
    try:return float(x)
    except (TypeError,ValueError):return None

def h3point(lat,lon):
    import h3
    return h3.latlng_to_cell(lat,lon,8)

def try_row(record,source,state,allowed_proprietary=False,osm_license_accepted=False):
    if source=='NAD':
        house,street,unit=clean(record.get('house_number')),clean(record.get('street')),''
        city=clean(record.get('city'));zipcode=clean(record.get('zip'))[:5]
        rid=clean(record.get('nad_object_id'));license='USDOT_NAD_REVIEW_USE'
    elif source=='OVERTURE':
        house,street,unit=clean(record.get('number')),clean(record.get('street')),clean(record.get('unit'))
        city=clean(record.get('city'));zipcode=clean(record.get('zip'))[:5]
        rid=clean(record.get('id'))
        licenses=clean(record.get('source_licenses')).lower()
        if ('proprietary' in licenses or 'unknown' in licenses) and not allowed_proprietary:
            return 'EXCLUDED_PROPRIETARY_SOURCE', None
        license='OVERTURE_SOURCE_LICENSE_PENDING_OR_REVIEWED'
    elif source=='OSM':
        if not osm_license_accepted:return 'EXCLUDED_ODBL_NOT_APPROVED',None
        house,street,unit=clean(record.get('number')),clean(record.get('street')),clean(record.get('unit'))
        city=clean(record.get('city'));zipcode=clean(record.get('zip'))[:5]
        rid=clean(record.get('source_id'));license='OSM_ODBL_REUSE_REQUIREMENTS'
    else:raise ValueError(source)
    if not house or not street:return 'INVALID_ADDRESS',None
    st=clean(record.get('state') or state).upper()
    if st!=state:return 'STATE_MISMATCH',None
    lat=flo(record.get('latitude'));lon=flo(record.get('longitude'))
    if lat is not None and lon is not None and -90<=lat<=90 and -180<=lon<=180:
        h3=h3point(lat,lon)
    else:
        h3=None
    # City+ZIP retained: an identical street name may exist in multiple cities in a ZIP.
    location=zipcode if zipcode else norm(city)
    address_hash='|'.join([st,location,norm(city),norm(house),norm(street),norm(unit)])
    # Source ID allows multiple official point records per identical textual address,
    # but canonical unique address+unit enforces cross-source deduplication.
    source_key=source+':'+rid if rid else source+':'+hashlib.sha256(address_hash.encode()).hexdigest()[:24]
    return 'GOOD',(address_hash,source_key,house,street,unit,city,st,zipcode,lat,lon,h3,source,license)

def nad_batches(path,batch_size):
    import pyarrow.parquet as pq
    pf=pq.ParquetFile(path)
    fields=['nad_object_id','house_number','street','city','state','zip','latitude','longitude']
    cols=[c for c in fields if c in pf.schema_arrow.names]
    for batch in pf.iter_batches(batch_size=batch_size,columns=cols):
        values=batch.to_pydict();names=tuple(values)
        for a in zip(*(values[n] for n in names)):
            yield dict(zip(names,a))

def read_gzip_csv(path):
    with gzip.open(path,'rt',newline='',encoding='utf8') as f:
        yield from csv.DictReader(f)

def read_part_zip(path):
    """Read the 13 actual Florida Overture ZIP artifact parts without unpacking 12M rows."""
    with zipfile.ZipFile(path) as z:
        candidates=[x for x in z.namelist() if x.endswith('.csv.gz')]
        if len(candidates)!=1:raise ValueError('Expected exactly one compressed CSV in '+str(path))
        with z.open(candidates[0]) as data:
            with gzip.GzipFile(fileobj=data,mode='rb') as decoded:
                import io
                with io.TextIOWrapper(decoded,encoding='utf8',newline='') as f:
                    yield from csv.DictReader(f)

def run(st,nad_root,supp_root,out,accept_odbl,allow_proprietary,batch=10000,limit=0):
    st=st.upper();target=Path(out)/f'state={st}';target.mkdir(parents=True,exist_ok=True)
    dbpath=target/f'PUBLIC_ADDRESS_H3_{st}.sqlite';db=sqlite3.connect(dbpath)
    db.execute('PRAGMA journal_mode=WAL');db.execute('PRAGMA synchronous=NORMAL')
    db.execute('''CREATE TABLE IF NOT EXISTS addresses (
        canonical_key TEXT PRIMARY KEY, source_key TEXT, house TEXT, street TEXT,
        unit TEXT,city TEXT,state TEXT,zip TEXT, latitude REAL,longitude REAL,
        h3_8 TEXT,source TEXT,license_status TEXT) WITHOUT ROWID''')
    db.execute('CREATE INDEX IF NOT EXISTS idx_address_zip ON addresses(state,zip)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_address_h3 ON addresses(h3_8)')
    db.execute('CREATE INDEX IF NOT EXISTS idx_address_source ON addresses(source)')
    counts=Counter();processed=0
    def consume(reader,source):
        nonlocal processed
        staged=[]
        for item in reader:
            processed+=1
            outcome,row=try_row(item,source,st,allow_proprietary,accept_odbl)
            counts[outcome]+=1
            if row:
                staged.append(row)
            if len(staged)>=batch:
                db.executemany('INSERT OR IGNORE INTO addresses VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',staged)
                db.commit();staged.clear()
            if limit and processed>=limit:break
        if staged:db.executemany('INSERT OR IGNORE INTO addresses VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',staged);db.commit()
    source_dir=Path(nad_root)/f'state={st}'
    nad_files=sorted(source_dir.glob('*.parquet'))
    for fn in nad_files:
        consume(nad_batches(fn,batch),'NAD')
        print('NAD source',st,fn.name,'rows_scanned',processed,flush=True)
        if limit and processed>=limit:break
    if not limit or processed<limit:
        supp_dir=Path(supp_root)/f'state={st}' if supp_root else Path('/not-found')
        for fn in sorted(supp_dir.glob('FL_verified_part_*.zip')) if st=='FL' else []:
            consume(read_part_zip(fn),'OVERTURE')
            print('FL PART',fn.name,'rows_scanned',processed,flush=True)
            if limit and processed>=limit:break
        for prefix,source in [('OVERTURE_*_SUPPLEMENT.csv.gz','OVERTURE'),('OSM_*_ADDRESSES.csv.gz','OSM')]:
            for fn in sorted(supp_dir.glob(prefix)):
                consume(read_gzip_csv(fn),source)
                print('SUPPLEMENT',st,fn.name,'rows_scanned',processed,flush=True)
                if limit and processed>=limit:break
            if limit and processed>=limit:break
    n=db.execute('SELECT COUNT(*) FROM addresses').fetchone()[0]
    h3_count=db.execute('SELECT COUNT(*) FROM addresses WHERE h3_8 IS NOT NULL').fetchone()[0]
    states={row[0]:row[1] for row in db.execute('SELECT source,COUNT(*) FROM addresses GROUP BY source')}
    duplicates=counts['GOOD']-n
    audit={'state':st,'status':'PARTIAL_TEST' if limit else 'SOURCE_FILES_INDEXED_COVERAGE_UNVERIFIED',
           'source_records_scanned':processed,'source_files_nad':len(nad_files),
           'unique_textual_addresses':n,'addresses_with_h3_geocodes':h3_count,
           'duplicates_or_collisions':duplicates,'count_by_source':states,
           'rejections':dict(counts),'source_licenses_require_review':True,
           'commercial_reuse_of_proprietary_records_allowed':allow_proprietary,
           'osm_odbl_terms_acknowledged':accept_odbl,
           'fcc_address_serviceability_verified':False,'built_at_utc':utc(),
           'database':str(dbpath)}
    (target/'PUBLIC_ADDRESS_H3_AUDIT.json').write_text(json.dumps(audit,indent=2))
    db.execute('PRAGMA wal_checkpoint(TRUNCATE)');db.close()
    print(json.dumps(audit,indent=2),flush=True)
    return audit

def lookup(db_path,house,street,city,state,zipcode='',unit=''):
    db=sqlite3.connect(db_path)
    loc=zipcode[:5] if zipcode else norm(city)
    key='|'.join([state.upper(),loc,norm(city),norm(house),norm(street),norm(unit)])
    row=db.execute('SELECT * FROM addresses WHERE canonical_key=?',(key,)).fetchone()
    db.close()
    return row

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--state',required=True)
    p.add_argument('--nad-root',required=True)
    p.add_argument('--supp-root',default='')
    p.add_argument('--out',required=True)
    p.add_argument('--accept-odbl',action='store_true')
    p.add_argument('--allow-proprietary',action='store_true')
    p.add_argument('--limit',type=int,default=0)
    a=p.parse_args()
    run(a.state,a.nad_root,a.supp_root,a.out,a.accept_odbl,a.allow_proprietary,limit=a.limit)
if __name__=='__main__': main()