#!/usr/bin/env python3
"""STANCE geographic provider-likelihood lookup, evidence not a qualification.

Inputs: public FCC BDC availability aggregates, NAD/OSM/Overture geography,
optional Census geocoder response. Does NOT infer private Fabric Location IDs.
An arbitrary street address absent from local address files can be queried on demand.
"""
from __future__ import annotations
import argparse, json, sqlite3, os, re, urllib.parse, urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

FCC_SOURCE = 'FCC 2025-12-31, archived 2026-10-19'


def standard_h3(lat: float, lon: float) -> tuple[str, str]:
    import h3
    c = h3.latlng_to_cell(lat, lon, 8)
    return c, str(h3.str_to_int(c))


def coverage_tier(n: int, d: int, min_sample: int = 20) -> str:
    """Descriptive evidence tiers; NOT calibrated probabilities."""
    if not d or d < 0 or not n or n < 0:
        return 'INSUFFICIENT' if not d or d < 0 else 'NO_REPORTED_GIGABIT_EVIDENCE'
    if n > d:
        return 'INCONSISTENT_COUNTS'
    if d < min_sample:
        return 'SMALL_SAMPLE'
    frac = n / d
    if frac >= .85 and n >= 20:
        return 'HIGH'
    if frac >= .60 and n >= 10:
        return 'LIKELY'
    if frac >= .25:
        return 'POSSIBLE'
    return 'LOW'


def combine_evidence(h3row: dict | None, blockrow: dict | None) -> tuple[str, str, float | None]:
    """Use smallest available calibrated *geography*, not a fabricated household probability.

    A block can cross an H3 boundary, so the two are separate overlapping proxies,
    not independent probabilities. Do not multiply/combine as probabilistic evidence.
    """
    b = coverage_tier(int(blockrow['n']), int(blockrow['d'])) if blockrow else 'INSUFFICIENT'
    h = coverage_tier(int(h3row['n']), int(h3row['d'])) if h3row else 'INSUFFICIENT'
    # A provider with zero records in an adequately sampled block is weak local
    # evidence, even if an adjacent H3 cell shows widespread service.
    if b == 'NO_REPORTED_GIGABIT_EVIDENCE' and blockrow and int(blockrow['d'])>=20:b='LOW'
    if h == 'NO_REPORTED_GIGABIT_EVIDENCE' and h3row and int(h3row['d'])>=20:h='LOW'
    valid = {'HIGH', 'LIKELY', 'POSSIBLE', 'LOW'}
    if b in valid and h in valid:
        # A disagreement reduces confidence; a block may be tiny or more specific.
        rank = ['LOW', 'POSSIBLE', 'LIKELY', 'HIGH']
        score = rank[min(rank.index(b), rank.index(h))]
        return score, 'H3_AND_BLOCK_AGREEMENT' if b == h else 'MIXED_H3_BLOCK_EVIDENCE', None
    if b in valid:
        return b, 'FCC_2020_BLOCK_ONLY', None
    if h in valid:
        return h, 'FCC_H3_R8_ONLY', None
    if b == 'SMALL_SAMPLE' or h == 'SMALL_SAMPLE':
        return 'UNCERTAIN', 'SMALL_LOCAL_SAMPLE', None
    if b == 'INCONSISTENT_COUNTS' or h == 'INCONSISTENT_COUNTS':
        return 'UNCERTAIN', 'FCC_DATA_INCONSISTENT', None
    return 'UNKNOWN', 'NO_LOCAL_FCC_EVIDENCE', None


def open_db(path):
    con = sqlite3.connect(str(path))
    con.row_factory = sqlite3.Row
    return con


def get_columns(conn, table):
    return {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}


def load_geo_rows(conn, table: str, state: str, key: str | None, geocol: str):
    if not key:
        return []
    if table not in {'fcc_h3', 'fcc_block'}:raise ValueError(table)
    if not conn:
        return []
    keys=[key]
    if table=='fcc_h3' and re.fullmatch(r'[0-9a-fA-F]{15,16}', key):
        try:keys.append(str(int(key, 16)))
        except ValueError:pass
    sql=f'SELECT * FROM {table} WHERE state=? AND {geocol} IN ({",".join("?"*len(keys))})'
    try:
        return [dict(x) for x in conn.execute(sql,[state]+keys)]
    except sqlite3.OperationalError as e:
        if 'no such table' in str(e):return []
        raise


def denominator(conn, table: str, state: str, key: str | None, geocol: str):
    if not key or not conn:return None
    if table not in {'fcc_h3_totals','fcc_block_totals'}:raise ValueError(table)
    keys=[key]
    if table=='fcc_h3_totals' and re.fullmatch(r'[0-9a-fA-F]{15,16}',key):
        try:keys.append(str(int(key,16)))
        except ValueError:pass
    try:
        row=conn.execute(f'SELECT max(d) FROM {table} WHERE state=? AND {geocol} IN ({",".join("?" for _ in keys)})',[state]+keys).fetchone()
    except sqlite3.OperationalError:return None
    return int(row[0]) if row and row[0] is not None else None


def zip_fallback(conn, state: str, zipcode: str):
    """Public FCC block -> ZCTA approximation, not postal ZIP or address proof."""
    if not conn or not zipcode:return []
    try:
        rows=conn.execute('''SELECT provider_id,brand,loc_1g,loc_2g,loc_fiber_1g AS loc_fiber,loc_cable_1g AS loc_cable,denominator,pct_1g AS percent_1g
          FROM fcc_1g_zcta WHERE state=? AND zcta=?''',(state,zipcode[:5])).fetchall()
    except sqlite3.OperationalError:return []
    return [dict(x) for x in rows]


def prepare_provider(rows, geokey):
    result={}
    for r in rows:
        p=str(r['provider_id'])
        entry=result.get(p)
        if entry is None or int(r['n'])>int(entry['n']):
            result[p]={'n':int(r['n']),'d':int(r['d']),'brand':r['brand'],
              'fiber':int(r['fiber'] or 0),'cable':int(r['cable'] or 0),
              'max_mbps':r['max_mbps'],'geography':geokey}
    return result


def candidates(lat:float|None, lon:float|None, state:str, zipcode:str='', block_geoid:str='',
               geo_db: str|Path|None=None, zip_db: str|Path|None=None, max_results=25) -> dict:
    state=state.upper().strip()
    zipcode=zipcode.strip()[:5]
    if not re.fullmatch(r'[A-Z]{2}',state):raise ValueError('Expected US state USPS code')
    if lat is not None and not -90 <= float(lat) <= 90:raise ValueError('Invalid latitude')
    if lon is not None and not -180 <= float(lon) <= 180:raise ValueError('Invalid longitude')
    geo=open_db(geo_db) if geo_db and Path(geo_db).exists() else None
    zipconn=open_db(zip_db) if zip_db and Path(zip_db).exists() else None
    h3key=None
    if lat is not None and lon is not None:
        h3key, h3decimal=standard_h3(float(lat),float(lon))
    try:
        h3rows=load_geo_rows(geo,'fcc_h3',state,h3key,'h3_8') if h3key else []
        blocks=load_geo_rows(geo,'fcc_block',state,block_geoid,'block_geoid')
        hm=prepare_provider(h3rows,'h3_8')
        bm=prepare_provider(blocks,'2020_census_block')
        hd=denominator(geo,'fcc_h3_totals',state,h3key,'h3_8')
        bd=denominator(geo,'fcc_block_totals',state,block_geoid,'block_geoid')
        z=zip_fallback(zipconn,state,zipcode)
        zm={str(t['provider_id']):t for t in z}
        providers=sorted(set(hm)|set(bm)|set(zm))
        out=[]
        for p in providers:
            h=hm.get(p);b=bm.get(p);zz=zm.get(p)
            # A provider absent from a well-populated block has zero reported
            # gigabit locations there. Do not silently discard this negative evidence.
            base=h or b or {}
            if h is None and hd is not None:
                h={'n':0,'d':hd,'brand':base.get('brand') or (zz or {}).get('brand') or p,
                   'fiber':0,'cable':0,'max_mbps':None,'geography':'h3_8'}
            if b is None and bd is not None:
                b={'n':0,'d':bd,'brand':base.get('brand') or (zz or {}).get('brand') or p,
                   'fiber':0,'cable':0,'max_mbps':None,'geography':'2020_census_block'}
            if h or b:
                tier,method,calibrated=combine_evidence(h,b)
                strongest=b or h
                pct=round(100*strongest['n']/strongest['d'],2) if strongest['d'] else None
                ev={'method':method,'h3_8':{'locations_1g':h['n'],'observed_locations':h['d']} if h else None,
                    'census_block_2020':{'locations_1g':b['n'],'observed_locations':b['d']} if b else None,
                    'local_geographic_share_pct':pct,
                    'source':FCC_SOURCE}
            elif zz:
                # ZIP evidence is substantially weaker. Even 95% can't justify HIGH for a house.
                denom=int(zz['denominator'] or 0); n=int(zz['loc_1g'] or 0)
                raw=coverage_tier(n,denom)
                tier='POSSIBLE' if raw in {'HIGH','LIKELY','POSSIBLE'} else ('LOW' if raw=='LOW' else 'UNKNOWN')
                ev={'method':'APPROXIMATE_CENSUS_ZCTA_FALLBACK','h3_8':None,'census_block_2020':None,
                    'zcta_approx':zipcode,'zip_1g_records':n,'zip_observed_locations':denom,
                    'source':FCC_SOURCE}
            else:continue
            brand=(h or b or {}).get('brand') or (zz or {}).get('brand') or p
            mx=(h or b or {}).get('max_mbps')
            fiber=(h or b or {}).get('fiber') or ((zz or {}).get('loc_fiber') if zz else None)
            cable=(h or b or {}).get('cable') or ((zz or {}).get('loc_cable') if zz else None)
            out.append({'fcc_provider_id':p,'brand':brand,'likelihood':tier,'evidence':ev,
                        'max_reported_mbps':mx,'reported_fiber_1g_locations':fiber,
                        'reported_cable_1g_locations':cable,'address_serviceability_confirmed':False,
                        'sales_authorization_confirmed':False})
        rank={'HIGH':0,'LIKELY':1,'POSSIBLE':2,'LOW':3,'UNCERTAIN':4,'UNKNOWN':5}
        out.sort(key=lambda x:(rank.get(x['likelihood'],6),-(x['evidence'].get('local_geographic_share_pct') or 0),x['brand']))
        return {'state':state,'zip':zipcode or None,'latitude':lat,'longitude':lon,'h3_8':h3key,
                'census_block_2020':block_geoid or None,'fcc_source':FCC_SOURCE,
                'mode':'REPORTED_GEOGRAPHIC_LIKELIHOOD_NOT_HOUSEHOLD_SERVICEABILITY',
                'providers':out[:max_results],'count':len(out),'limitations':['Public FCC Location IDs are not joined to named street addresses','ZCTA is not a USPS ZIP','No data does not mean no service','Contract territory is separate from FCC coverage']}
    finally:
        if geo:geo.close()
        if zipconn:zipconn.close()


def geocode_census(street_address: str, timeout=16) -> dict:
    """Best-effort geocode of any US street address, including ones absent from NAD.

    Address is sent to a public US Census geocoding endpoint only when explicitly used.
    """
    if len(street_address)>300 or not street_address.strip():raise ValueError('Address missing or too long')
    qs=urllib.parse.urlencode({'address':street_address,'benchmark':'Public_AR_Census2020',
                               'vintage':'Census2020_Census2020','format':'json'})
    base='https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress?'
    try:
        with urllib.request.urlopen(urllib.request.Request(base+qs,headers={'User-Agent':'STANCE-geocoding-research/1.0'}),timeout=timeout) as resp:
            obj=json.load(resp)
    except Exception as exc:
        # Caller can query by lat/lon, ZIP, and state even when geocoding is unavailable.
        raise RuntimeError('Public Census geocoder unavailable; use coordinates or ZIP fallback') from exc
    matches=obj.get('result',{}).get('addressMatches') or []
    if not matches:raise ValueError('Address not geocoded by Census; provide coordinates or ZIP for geographic fallback')
    m=matches[0]; coords=m['coordinates'];g=m.get('geographies') or {}
    blocks=g.get('Census Blocks') or g.get('2020 Census Blocks') or []
    block=str(blocks[0].get('GEOID','')) if blocks else ''
    matched=m.get('matchedAddress','')
    # state USPS/ZIP from matchedAddress often comma spaced; fallback caller supplied state.
    state=''; zipcode=''
    mstate=re.search(r'\b([A-Z]{2})\s+(\d{5})(?:-\d{4})?\b',matched)
    if mstate:state,zipcode=mstate.groups()
    return {'latitude':coords['y'],'longitude':coords['x'],'state':state,'zip':zipcode,
            'block_geoid':block,'geocoded_matched_address':matched,'geocode_is_exact_address_qualification':False}


def main():
    a=argparse.ArgumentParser()
    a.add_argument('--geo-db',default=os.getenv('STANCE_H3_BLOCK_DB',''))
    a.add_argument('--zip-db',default=os.getenv('STANCE_ZIP_FALLBACK_DB','data/zip_fallback.sqlite'))
    a.add_argument('--state',default='');a.add_argument('--zip',default='');a.add_argument('--lat',type=float);a.add_argument('--lon',type=float)
    a.add_argument('--block',default='');a.add_argument('--address',default='');a.add_argument('--limit',type=int,default=30)
    a.add_argument('--address-index-root',default=os.getenv('STANCE_ADDRESS_INDEX_ROOT',''))
    a.add_argument('--house',default='');a.add_argument('--street',default='');a.add_argument('--city',default='');a.add_argument('--unit',default='')
    args=a.parse_args()
    lat=args.lat;lon=args.lon;state=args.state;zipcode=args.zip;block=args.block
    match_type='INPUT_COORDINATES' if lat is not None and lon is not None else 'UNRESOLVED'
    if args.address_index_root and args.house and args.street and args.city and args.state:
        from address_index_builder import lookup
        dbpath=Path(args.address_index_root)/f'state={state.upper()}'/f'PUBLIC_ADDRESS_H3_{state.upper()}.sqlite'
        if dbpath.exists():
            row=lookup(dbpath,args.house,args.street,args.city,state,zipcode,args.unit)
            if row and row[8] is not None and row[9] is not None:
                lat,lon=row[8],row[9]
                match_type=f'PUBLIC_ADDRESS_INDEX_SOURCE_{row[11]}'
    addr=args.address
    if not addr and args.house and args.street and args.city and state:
        addr=f'{args.house} {args.street}, {args.city}, {state} {zipcode}'
    if addr and (lat is None or lon is None):
        try:
            geocoded=geocode_census(addr)
            lat,lon=geocoded['latitude'],geocoded['longitude']
            state=state or geocoded['state'];zipcode=zipcode or geocoded['zip'];block=block or geocoded['block_geoid']
            match_type='PUBLIC_CENSUS_GEOCODER'
        except (ValueError,RuntimeError):
            if not (state and zipcode):raise
            match_type='ZIP_FALLBACK_GEOCODING_UNAVAILABLE'
    result=candidates(lat,lon,state,zipcode,block,args.geo_db,args.zip_db,args.limit)
    result['address_match_type']=match_type
    print(json.dumps(result,indent=2))

if __name__=='__main__':main()