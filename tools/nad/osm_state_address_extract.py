#!/usr/bin/env python3
"""Public OpenStreetMap address supplementation with explicit ODbL provenance.

Extracts addr:housenumber + addr:street from Geofabrik state PBFs.
NO private/customer files, NO names/emails/phones, NO provider qualification.
Retain separate state datasets: ODbL obligations may apply to derivatives.
"""
import argparse, csv, gzip, hashlib, json, shutil, sqlite3, tempfile, time
from pathlib import Path
from datetime import datetime, timezone
from urllib.request import Request, urlopen
import osmium

STATES={"HI":"hawaii", "NH":"new-hampshire", "NV":"nevada",
        "MI":"michigan","MS":"mississippi","FL":"florida",
        "GA":"georgia","SC":"south-carolina"}
COLS="source_id number street unit city state zip longitude latitude location_type release source license serviceability".split()

class Addresses(osmium.SimpleHandler):
    def __init__(self,db,state):
        super().__init__();self.db=db;self.state=state
        self.scanned=0;self.accepted=0;self.duplicates=0
    def emit(self,kind,obj,coords,loc_type):
        self.scanned+=1
        tags=obj.tags
        number=tags.get("addr:housenumber","").strip()
        street=tags.get("addr:street","").strip()
        if not number or not street:return
        try: lon,lat=map(float,coords)
        except (ValueError,TypeError):return
        if not (-180<=lon<=180 and -90<=lat<=90):return
        source_id=f"{kind}{obj.id}"
        vals=(source_id,number,street,tags.get("addr:unit","") or "",tags.get("addr:city","") or "",
              self.state,tags.get("addr:postcode","") or "",lon,lat,loc_type,
              "geofabrik-current","OpenStreetMap via Geofabrik","ODbL 1.0","NOT_VERIFIED")
        cursor=self.db.execute("INSERT OR IGNORE INTO address VALUES ("+",".join("?" for _ in COLS)+")",vals)
        if cursor.rowcount:self.accepted+=1
        else:self.duplicates+=1
        if self.scanned%500000==0:
            self.db.commit()
            print(self.state,"examined",self.scanned,"addresses",self.accepted,flush=True)
    def node(self,node):
        if not node.location.valid():return
        self.emit("n",node,(node.location.lon,node.location.lat),"OSM_node")
    def way(self,way):
        # A building way address has an approximate building center, not a serviceable door point.
        coords=[(n.location.lon,n.location.lat) for n in way.nodes if n.location.valid()]
        if not coords:return
        self.emit("w",way,(sum(x for x,y in coords)/len(coords),sum(y for x,y in coords)/len(coords)),"OSM_way_mean_node")

def main():
    p=argparse.ArgumentParser();p.add_argument("--state",choices=list(STATES),required=True)
    p.add_argument("--output",default="osm_address_sources");p.add_argument("--pbf")
    args=p.parse_args();target=Path(args.output)/f"state={args.state}";target.mkdir(parents=True,exist_ok=True)
    slug=STATES[args.state];url=f"https://download.geofabrik.de/north-america/us/{slug}-latest.osm.pbf"
    source=Path(args.pbf) if args.pbf else target/f"{slug}-latest.osm.pbf"
    if not args.pbf and not source.exists():
        print("Downloading",url,flush=True)
        for attempt in range(4):
            try:
                with urlopen(Request(url,headers={"User-Agent":"STANCE-public-address-quality-audit/1.0"}),timeout=100) as r, source.open("wb") as f:
                    shutil.copyfileobj(r,f,4*1024*1024)
                break
            except Exception as exc:
                print("retry",attempt,str(exc),flush=True)
                if source.exists():source.unlink()
                time.sleep(5*(attempt+1))
        if not source.exists():raise SystemExit("Cannot download official state OSM extract")
    sha=hashlib.sha256()
    with source.open("rb") as f:
        while chunk:=f.read(1024*1024*8):sha.update(chunk)
    db=sqlite3.connect(target/"addresses.sqlite")
    db.execute("CREATE TABLE IF NOT EXISTS address("+",".join(c+" TEXT" for c in COLS)+", PRIMARY KEY(source_id)) WITHOUT ROWID")
    h=Addresses(db,args.state)
    h.apply_file(str(source),locations=True)
    db.commit()
    count=db.execute("SELECT COUNT(*) FROM address").fetchone()[0]
    export=target/f"OSM_{args.state}_ADDRESSES.csv.gz"
    with gzip.open(export,"wt",newline="",encoding="utf8") as f:
        w=csv.writer(f);w.writerow(COLS)
        for row in db.execute("SELECT "+",".join(COLS)+" FROM address ORDER BY source_id"):
            w.writerow(row)
    audit={"state":args.state,"status":"EXTRACTED_NOT_COMPLETE" if count else "NO_ADDRESS_RECORDS",
       "real_source_address_records":count,"feature_objects_scanned":h.scanned,
       "source_url":url,"source_sha256":sha.hexdigest(),"file_bytes":source.stat().st_size,
       "source":"OpenStreetMap/Geofabrik","license":"Open Database License (ODbL) 1.0",
       "commercial_derivative_database_licensing_review_required":True,
       "coordinates_for_ways_are_approximate_building_centers":True,
       "customer_records_used":0,"provider_serviceability_verified":False,
       "colab_or_github_finished_utc":datetime.now(timezone.utc).isoformat()}
    (target/"OSM_ADDRESS_AUDIT.json").write_text(json.dumps(audit,indent=2))
    print(json.dumps(audit,indent=2),flush=True)
    db.close()
    if not args.pbf:source.unlink()
    if count<1:raise SystemExit("No public address records in extract")
if __name__=="__main__":main()
