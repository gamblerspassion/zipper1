#!/usr/bin/env python3
"""Build nationwide FCC provider-by-H3 and provider-by-2020-block evidence.

Input is the user's verified 973,666,787-row public FCC Parquet. Does not require
CostQuest Fabric; distinct location_id is used only in aggregate counts. Output
contains no FCC Location ID / private Fabric crosswalk / identifiable household.

Run with DuckDB/Colab on the 8.47 GB source. A one-pass state partition reduces
scanning the full file for each of 50+ state aggregates. State jobs are resumable.
"""
import argparse, json, os, re, sqlite3
from datetime import datetime, timezone
from pathlib import Path

STATES='AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY'.split()
TERRITORIES='PR VI AS GU MP'.split()
REQUIRED={'provider_id','location_id','state_usps','block_geoid','h3_8','brand_name',
          'technology','business_residential_code','max_advertised_download_speed'}
SOURCE_BYTES=8472386659
SOURCE_ROWS=973666787

def quote(s):return "'"+str(s).replace("'","''")+"'"
def utc():return datetime.now(timezone.utc).isoformat()

def connection(tmp,threads,mem):
    import duckdb
    tmp=Path(tmp);tmp.mkdir(parents=True,exist_ok=True)
    c=duckdb.connect(':memory:')
    c.execute(f'SET temp_directory={quote(tmp.as_posix())}')
    c.execute(f'SET memory_limit={quote(mem)}')
    c.execute(f'SET threads={int(threads)}')
    c.execute('SET preserve_insertion_order=false')
    return c

def raw_sql(source,con):
    src=quote(str(Path(source)))
    names={x[0] for x in con.execute(f'DESCRIBE SELECT * FROM read_parquet({src})').fetchall()}
    missing=REQUIRED-names
    if missing:raise ValueError('FCC source schema missing: '+repr(missing))
    return f'''SELECT UPPER(TRIM(CAST(state_usps AS VARCHAR))) AS state,
        CAST(location_id AS VARCHAR) AS location_id,
        LPAD(TRIM(CAST(block_geoid AS VARCHAR)),15,'0') AS block_geoid,
        LOWER(TRIM(CAST(h3_8 AS VARCHAR))) AS h3_8,
        CAST(provider_id AS VARCHAR) AS provider_id,
        CAST(brand_name AS VARCHAR) AS brand,
        TRY_CAST(technology AS INTEGER) AS tech,
        TRY_CAST(max_advertised_download_speed AS DOUBLE) AS speed
      FROM read_parquet({src})
      WHERE location_id IS NOT NULL AND UPPER(TRIM(CAST(business_residential_code AS VARCHAR))) IN ('R','X')
         AND state_usps IS NOT NULL'''

def partition(source,cache,tmp,threads,mem,allow_test_source=False):
    """One national FCC scan, materialize lean state Parquet partitions locally."""
    source=Path(source);cache=Path(cache);cache.mkdir(parents=True,exist_ok=True)
    if not source.is_file():raise FileNotFoundError(source)
    if not allow_test_source and source.stat().st_size!=SOURCE_BYTES:
        raise ValueError(f'FCC source wrong size; expected {SOURCE_BYTES}, found {source.stat().st_size}')
    manifest=cache/'partition_source_audit.json'
    if manifest.exists():
        m=json.loads(manifest.read_text())
        if m.get('status')=='COMPLETE' and m.get('source_bytes')==source.stat().st_size:
            print('REUSING VERIFIED FCC STATE PARTITIONS:',cache,flush=True)
            return m
    con=connection(tmp,threads,mem)
    base=raw_sql(source,con)
    out=cache/'parts';out.mkdir(exist_ok=True)
    con.execute(f'''COPY (SELECT * FROM ({base}) r
      WHERE state IN ({','.join(quote(s) for s in STATES+TERRITORIES)})
        AND location_id IS NOT NULL)
      TO {quote(out.as_posix())}
      (FORMAT PARQUET, COMPRESSION ZSTD, PARTITION_BY (state), ROW_GROUP_SIZE 100000)''')
    count=con.execute(f'SELECT COUNT(*) FROM read_parquet({quote(str(out / "state=*" / "*.parquet"))},hive_partitioning=true)').fetchone()[0]
    stat={'status':'COMPLETE','input_file':source.name,'source_bytes':source.stat().st_size,
          'public_availability_records':SOURCE_ROWS if not allow_test_source else None,
          'resident_availability_rows_partitioned':count,'file_path':str(out),
          'contains_street_addresses':False,'contains_individual_location_id_in_intermediate_cache':True,
          'intermediate_cache_should_not_be_published':True,'completed_utc':utc()}
    manifest.write_text(json.dumps(stat,indent=2));con.close()
    print(json.dumps(stat,indent=2),flush=True)
    return stat

def build_state(state,cache,out,tmp,threads,mem):
    state=state.upper()
    if state not in STATES+TERRITORIES:raise ValueError('Invalid state '+state)
    statefiles=Path(cache)/'parts'/f'state={state}'
    if not statefiles.exists():
        print('NO STATE PARTITION',state,flush=True)
        return {'state':state,'status':'NO_FCC_DATA'}
    dest=Path(out)/f'state={state}';dest.mkdir(parents=True,exist_ok=True)
    audit_file=dest/'FCC_H3_BLOCK_AUDIT.json'
    if audit_file.exists():
        old=json.loads(audit_file.read_text())
        if old.get('status')=='COMPLETE' and all((dest/f'FCC_{k.upper()}_{t}_{state}.parquet').exists() for k in ['h3','block'] for t in ['GIGABIT','TOTALS']):
            print('REUSING',state,old['h3_provider_rows'],old['block_provider_rows'],flush=True)
            return old
    con=connection(tmp,threads,mem)
    pattern=quote(str(statefiles/'*.parquet'))
    con.execute(f'CREATE VIEW source AS SELECT * FROM read_parquet({pattern})')
    outputs={}
    for col,key in [('h3_8','h3'),('block_geoid','block')]:
        # Coverage numerator and denominator use DISTINCT location_id at the same
        # granularity; avoid multi-technology duplicate inflation.
        con.execute(f'''CREATE OR REPLACE TEMP TABLE denominator AS
          SELECT {col} AS geographic_key,COUNT(DISTINCT location_id) AS observed_locations
          FROM source WHERE {col} IS NOT NULL AND TRIM({col})<>'' AND LOWER({col}) NOT IN ('none','nan','null')
          GROUP BY 1''')
        con.execute(f'''CREATE OR REPLACE TEMP TABLE available AS
          SELECT {col} AS geographic_key,provider_id,ANY_VALUE(brand) AS brand,
             COUNT(DISTINCT location_id) AS n,
             COUNT(DISTINCT CASE WHEN tech=50 THEN location_id END) AS fiber,
             COUNT(DISTINCT CASE WHEN tech=40 THEN location_id END) AS cable,
             MAX(speed) AS max_mbps
          FROM source
          WHERE {col} IS NOT NULL AND TRIM({col})<>'' AND provider_id IS NOT NULL
            AND tech IN (40,50) AND speed>=1000
          GROUP BY 1,2''')
        totals_out=dest/f'FCC_{key.upper()}_TOTALS_{state}.parquet'
        totals_pending=totals_out.with_suffix('.pending.parquet')
        con.execute(f'COPY (SELECT {quote(state)} AS state,geographic_key AS {col},observed_locations AS d FROM denominator) TO {quote(str(totals_pending))} (FORMAT PARQUET,COMPRESSION ZSTD)')
        totals_pending.replace(totals_out)
        output=dest/f'FCC_{key.upper()}_GIGABIT_{state}.parquet'
        tmpoutput=output.with_suffix('.pending.parquet')
        # Each index is keyed by exact FCC h3_8 or 2020 Census block; neither
        # geometry is a direct FCC Location ID -> public address assignment.
        query=f'''SELECT {quote(state)} AS state, geographic_key AS {col},
            a.provider_id,a.brand,a.n,d.observed_locations AS d,a.fiber,a.cable,a.max_mbps
          FROM available a INNER JOIN denominator d USING(geographic_key)
          WHERE a.n<=d.observed_locations AND a.n>0'''
        con.execute(f'COPY ({query}) TO {quote(str(tmpoutput))} (FORMAT PARQUET,COMPRESSION ZSTD)')
        tmpoutput.replace(output)
        outputs[key]={'provider_geo_rows':con.execute('SELECT COUNT(*) FROM available').fetchone()[0],
                      'location_geo_rows':con.execute('SELECT COUNT(*) FROM denominator').fetchone()[0],
                      'output_file':output.name,'source':'FCC'}
    audit={'status':'COMPLETE','state':state,
        'h3_provider_rows':outputs['h3']['provider_geo_rows'],'block_provider_rows':outputs['block']['provider_geo_rows'],
        'h3_cells_observed':outputs['h3']['location_geo_rows'],
        'blocks_observed':outputs['block']['location_geo_rows'],
        'technology_filter':'cable_40_or_fiber_50_at_least_1000_down; R/X residential/both',
        'denominator':'distinct observed residential FCC Location IDs with a reported technology in geographic key',
        'does_not_contain_location_ids':True,'does_not_contain_addresses':True,
        'commercial_sales_territory_verified':False,'serviceability_at_address_verified':False,
        'completed_utc':utc()}
    audit_file.write_text(json.dumps(audit,indent=2));con.close()
    print(json.dumps(audit,indent=2),flush=True)
    return audit

def index_sqlite(out,sqlite_path,replace=False):
    import duckdb
    path=Path(sqlite_path);path.parent.mkdir(parents=True,exist_ok=True)
    if path.exists() and replace:path.unlink()
    db=sqlite3.connect(path)
    db.execute('PRAGMA journal_mode=WAL')
    db.execute('PRAGMA synchronous=NORMAL')
    db.execute('CREATE TABLE IF NOT EXISTS fcc_h3(state TEXT,h3_8 TEXT,provider_id TEXT,brand TEXT,n INTEGER,d INTEGER,fiber INTEGER,cable INTEGER,max_mbps REAL,PRIMARY KEY(state,h3_8,provider_id)) WITHOUT ROWID')
    db.execute('CREATE TABLE IF NOT EXISTS fcc_block(state TEXT,block_geoid TEXT,provider_id TEXT,brand TEXT,n INTEGER,d INTEGER,fiber INTEGER,cable INTEGER,max_mbps REAL,PRIMARY KEY(state,block_geoid,provider_id)) WITHOUT ROWID')
    db.execute('CREATE TABLE IF NOT EXISTS fcc_h3_totals(state TEXT,h3_8 TEXT,d INTEGER,PRIMARY KEY(state,h3_8)) WITHOUT ROWID')
    db.execute('CREATE TABLE IF NOT EXISTS fcc_block_totals(state TEXT,block_geoid TEXT,d INTEGER,PRIMARY KEY(state,block_geoid)) WITHOUT ROWID')
    con=duckdb.connect(':memory:')
    state_total={};grand=0
    for state in STATES+TERRITORIES:
        folder=Path(out)/f'state={state}'
        audit=folder/'FCC_H3_BLOCK_AUDIT.json'
        if not audit.is_file():continue
        info=json.loads(audit.read_text())
        if info['status']!='COMPLETE':continue
        total=0
        for key,table in [('H3','fcc_h3'),('BLOCK','fcc_block'),('H3_TOTALS','fcc_h3_totals'),('BLOCK_TOTALS','fcc_block_totals')]:
            f=folder/(f'FCC_{key[:-7]}_TOTALS_{state}.parquet' if key.endswith('_TOTALS') else f'FCC_{key}_GIGABIT_{state}.parquet')
            if not f.exists():raise FileNotFoundError(f)
            view=con.execute(f'SELECT * FROM read_parquet({quote(f.as_posix())})')
            # fetchmany avoids keeping millions of rows in Python memory
            batch=10000
            while rows:=view.fetchmany(batch):
                db.executemany(f'INSERT OR REPLACE INTO {table} VALUES ({",".join("?" for _ in range(len(rows[0])))})',rows)
                total+=len(rows)
            db.commit()
        state_total[state]=total;grand+=total
        print('SQLITE INDEX',state,total,flush=True)
    db.execute('CREATE TABLE IF NOT EXISTS build_meta(name TEXT PRIMARY KEY,value TEXT)')
    for k,v in [('status','INDEXED_FCC_GEOGRAPHIC_EVIDENCE'),('fcc_source','2025-12-31 / 2026-10-19'),
                ('h3_block_index_rows',str(grand)),('states_indexed',json.dumps(state_total)),('created_utc',utc())]:
        db.execute('INSERT OR REPLACE INTO build_meta VALUES (?,?)',(k,v))
    db.commit();db.execute('PRAGMA wal_checkpoint(TRUNCATE)');db.close()
    (path.parent/(path.stem+'_AUDIT.json')).write_text(json.dumps({'status':'COMPLETE','indexed_rows':grand,'state_counts':state_total,'not_location_qualified':True},indent=2))
    return grand

def main():
    parser=argparse.ArgumentParser()
    sub=parser.add_subparsers(dest='command',required=True)
    p=sub.add_parser('partition');p.add_argument('--source',required=True);p.add_argument('--cache',required=True)
    p=sub.add_parser('build');p.add_argument('--cache',required=True);p.add_argument('--out',required=True);p.add_argument('--states',default='ALL')
    p=sub.add_parser('index');p.add_argument('--out',required=True);p.add_argument('--sqlite',required=True);p.add_argument('--replace',action='store_true')
    for cmd in ('partition','build'):
        x=[z for z in sub.choices[cmd]._actions if z.dest!='help']
        sub.choices[cmd].add_argument('--temp',default='/content/fcc_geo_temp')
        sub.choices[cmd].add_argument('--threads',type=int,default=2)
        sub.choices[cmd].add_argument('--memory',default='5GB')
    args=parser.parse_args()
    if args.command=='partition':partition(args.source,args.cache,args.temp,args.threads,args.memory)
    elif args.command=='build':
        states=STATES+TERRITORIES if args.states.upper()=='ALL' else [x.strip().upper() for x in args.states.split(',')]
        for state in states:build_state(state,args.cache,args.out,args.temp,args.threads,args.memory)
    elif args.command=='index':index_sqlite(args.out,args.sqlite,args.replace)
if __name__=='__main__':main()