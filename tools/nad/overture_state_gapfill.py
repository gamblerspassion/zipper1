#!/usr/bin/env python3
"""State-by-state supplemental public address extraction (never customer data).

Overture 2026-10-21 address records, clipped with Census 2025 state boundaries.
Separate from NAD-r24; NOT exact FCC serviceability and NOT proof of completeness.
Usage: python tools/nad/overture_state_gapfill.py --state NH --out output
"""
import argparse,csv,gzip,hashlib,json,math,re,shutil,sqlite3,subprocess,tempfile,time
from datetime import datetime,timezone
from pathlib import Path
from urllib.request import urlopen,Request
import geopandas as gpd
from shapely.geometry import Point,box
from shapely.prepared import prep

CENSUS="https://www2.census.gov/geo/tiger/GENZ2025/shp/cb_2025_us_state_500k.zip"
RELEASE="2026-10-21.0"
FIELDS="id number street unit city state zip longitude latitude source_datasets source_licenses source_ids release serviceability".split()
STATES="AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VA VT WA WI WV WY DC".split()

def utc(): return datetime.now(timezone.utc).isoformat()
def get_geom(state,cache):
    cache.mkdir(parents=True,exist_ok=True)
    f=cache/"census_states_2025.zip"
    if not f.exists() or f.stat().st_size<10000:
        with urlopen(Request(CENSUS,headers={"User-Agent":"STANCE-geo-research/1.0"}),timeout=90) as x, f.open("wb") as out: shutil.copyfileobj(x,out)
    g=gpd.read_file(f)
    if g.crs and g.crs.to_epsg()!=4326:g=g.to_crs(4326)
    selected=g.loc[g.STUSPS.eq(state)]
    if selected.empty:raise RuntimeError("Census boundary unavailable: "+state)
    return selected.geometry.union_all()

def tiles(geom,step):
    w,s,e,n=geom.bounds
    w=math.floor(w/step)*step;s=math.floor(s/step)*step
    out=[]
    for i in range(math.ceil((e-w)/step)):
        for j in range(math.ceil((n-s)/step)):
            a,b=w+i*step,s+j*step
            if geom.intersects(box(a,b,a+step,b+step)):
                out.append((f"{i}_{j}",(a-.00001,b-.00001,a+step+.00001,b+step+.00001)))
    return out

def parse(line,state,border):
    try:f=json.loads(line.lstrip("\x1e").strip())
    except ValueError:return None
    if f.get("type")!="Feature":return None
    p=f.get("properties") or {};g=f.get("geometry") or {}
    if p.get("country")!="US" or not p.get("number") or not p.get("street"):return None
    if g.get("type")!="Point":return None
    xy=g.get("coordinates") or []
    if len(xy)<2:return None
    try:x,y=float(xy[0]),float(xy[1])
    except (TypeError,ValueError):return None
    if not border.covers(Point(x,y)):return None
    zipv=str(p.get("postcode") or "").strip()
    vals=[state,zipv,str(p.get("postal_city") or ""),str(p["number"]),str(p["street"]),str(p.get("unit") or "")]
    if not zipv:vals.extend([str(round(x,5)),str(round(y,5))])
    key=hashlib.sha256("|".join(re.sub(r"\s+"," ",v.strip().upper()) for v in vals).encode()).hexdigest()
    src=p.get("sources") or []
    data=[str(f.get("id") or ""),str(p["number"]),str(p["street"]),str(p.get("unit") or ""),str(p.get("postal_city") or ""),state,zipv,x,y,
        json.dumps(sorted({str(z.get("dataset") or "") for z in src if isinstance(z,dict)})),
        json.dumps(sorted({str(z.get("license") or "") for z in src if isinstance(z,dict)})),
        json.dumps([str(z.get("record_id") or "") for z in src if isinstance(z,dict)]),RELEASE,"NOT_VERIFIED"]
    return key,data

def main():
    a=argparse.ArgumentParser()
    a.add_argument("--state",choices=STATES,required=True)
    a.add_argument("--out",default="address_gapfill")
    a.add_argument("--tile-size",type=float,default=1.5)
    a.add_argument("--max-tiles",type=int,default=0)
    args=a.parse_args()
    if args.tile_size<=0 or args.tile_size>5:raise ValueError("Invalid tile-size")
    target=Path(args.out)/f"state={args.state}";target.mkdir(parents=True,exist_ok=True)
    geom=get_geom(args.state,Path(args.out)/"census_boundary_cache")
    border=prep(geom)
    alltiles=tiles(geom,args.tile_size)
    active=alltiles[:args.max_tiles] if args.max_tiles else alltiles
    db=sqlite3.connect(target/"staging.sqlite")
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.execute("CREATE TABLE IF NOT EXISTS address(k TEXT PRIMARY KEY,"+ ",".join(x+" TEXT" for x in FIELDS)+") WITHOUT ROWID")
    status_file=target/"PROGRESS.json"; status={"state":args.state,"source":"Overture Maps","release":RELEASE,"total_tiles":len(alltiles),"tiles":{},"started":utc()}
    if status_file.exists():status=json.loads(status_file.read_text())
    for pos,(tile_id,bbox) in enumerate(active):
        if status.get("tiles",{}).get(tile_id,{}).get("status")=="ok":continue
        succeeded=False;errors=[]
        for attempt in range(3):
            with tempfile.TemporaryDirectory() as d:
                tmp=Path(d)/"tile.geojsonseq"
                cmd=["overturemaps","download","--bbox="+",".join(str(round(x,6)) for x in bbox),"-f","geojsonseq","--type=address","-o",str(tmp)]
                try:
                    ret=subprocess.run(cmd,capture_output=True,text=True,timeout=1800)
                    if ret.returncode!=0:raise RuntimeError((ret.stderr or ret.stdout)[-800:])
                    if not tmp.exists():raise RuntimeError("No output returned")
                    scanned=0;accepted=0
                    with tmp.open(encoding="utf8",errors="replace") as fh:
                        for line in fh:
                            parsed=parse(line,args.state,border)
                            scanned+=1
                            if not parsed:continue
                            k,row=parsed
                            inserted=db.execute("INSERT OR IGNORE INTO address VALUES ("+",".join("?" for _ in range(1+len(FIELDS)))+")",[k]+row)
                            accepted+=inserted.rowcount
                    db.commit()
                    status["tiles"][tile_id]={"status":"ok","source_rows":scanned,"new_unique_rows":accepted}
                    print(args.state,tile_id,scanned,accepted,flush=True)
                    succeeded=True;break
                except Exception as exc:errors.append(str(exc));time.sleep((attempt+1)*5)
        if not succeeded:
            status["tiles"][tile_id]={"status":"error","errors":errors}
            print("ERROR",args.state,tile_id,errors,flush=True)
        status["last_updated_utc"]=utc()
        status_file.write_text(json.dumps(status,indent=2))
    count=db.execute("SELECT COUNT(*) FROM address").fetchone()[0]
    complete=len(active)==len(alltiles) and len(status["tiles"])==len(alltiles) and all(x["status"]=="ok" for x in status["tiles"].values()) and count>0
    audit={"state":args.state,"status":"COMPLETE" if complete else "INCOMPLETE","unique_public_address_records":count,"tiles_expected":len(alltiles),"tiles_finished":sum(x["status"]=="ok" for x in status["tiles"].values()),"source":"Overture Maps addresses","release":RELEASE,
        "census_boundary_source":CENSUS,"no_customer_data":True,"address_level_fcc_serviceability_verified":False,"completed_utc":utc()}
    (target/"STATE_AUDIT.json").write_text(json.dumps(audit,indent=2))
    if complete:
        with gzip.open(target/f"OVERTURE_{args.state}_SUPPLEMENT.csv.gz","wt",newline="",encoding="utf8") as f:
            writer=csv.writer(f);writer.writerow(FIELDS)
            for r in db.execute("SELECT "+",".join(FIELDS)+" FROM address"):writer.writerow(r)
        (target/"COMPLETE.ok").write_text(utc())
    print(json.dumps(audit,indent=2),flush=True)
    db.close()
    if not complete:raise SystemExit(2)

if __name__=="__main__": main()
