#!/usr/bin/env python3
"""Minimal HTTP API for geographic likelihood. Default loopback only.

Local prototype, NOT an internet-exposed production API. Add authentication,
rate limiting, and database hosting before deployment. Does not store addresses.
"""
import argparse,json,os,urllib.parse
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from pathlib import Path
from likelihood_engine import candidates,geocode_census

class Handler(BaseHTTPRequestHandler):
    GEO_DB='';ZIP_DB='';ORIGIN=''
    def log_message(self,format,*args):
        # Deliberately don't log requests containing potentially sensitive address strings.
        return
    def reply(self,code,data):
        raw=json.dumps(data,ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header('Content-Type','application/json; charset=utf-8')
        self.send_header('Cache-Control','no-store')
        self.send_header('Content-Length',str(len(raw)))
        if self.ORIGIN:self.send_header('Access-Control-Allow-Origin',self.ORIGIN)
        self.end_headers();self.wfile.write(raw)
    def do_GET(self):
        if len(self.path)>1000:return self.reply(414,{'error':'Query too long'})
        url=urllib.parse.urlsplit(self.path)
        if url.path=='/health':
            return self.reply(200,{'status':'available','h3_block_index_exists':bool(self.GEO_DB and Path(self.GEO_DB).exists()),
                                'zip_fallback_exists':bool(self.ZIP_DB and Path(self.ZIP_DB).exists()),
                                'address_qualifications_confirmed':False})
        if url.path!='/api/likely-providers':return self.reply(404,{'error':'Not found'})
        d=urllib.parse.parse_qs(url.query,max_num_fields=15)
        def get(k):return str(d.get(k,[''])[0])
        try:
            address=get('address');state=get('state');zipcode=get('zip');block=get('block_geoid')
            if address:
                try:
                    match=geocode_census(address)
                    lat,lon=match['latitude'],match['longitude']
                    state=state or match['state'];zipcode=zipcode or match['zip'];block=block or match['block_geoid']
                    method='PUBLIC_CENSUS_GEOCODER'
                except (RuntimeError,ValueError):
                    if not zipcode or not state:raise
                    lat=lon=None;method='GEOCODING_FAILED_ZIP_FALLBACK'
            else:
                lat=float(get('lat')) if get('lat') else None
                lon=float(get('lon')) if get('lon') else None
                method='SUPPLIED_COORDINATES' if lat is not None and lon is not None else 'ZIP_FALLBACK'
            if not state or not (zipcode or (lat is not None and lon is not None)):
                raise ValueError('Must provide state and (lat + lon or ZIP or a geocodable address)')
            r=candidates(lat,lon,state,zipcode,block,self.GEO_DB,self.ZIP_DB)
            r['geocode_method']=method
            return self.reply(200,r)
        except ValueError as e:return self.reply(400,{'error':str(e)})
        except Exception:return self.reply(503,{'error':'Lookup unavailable; try coordinates or ZIP and state'})

def main():
    p=argparse.ArgumentParser()
    p.add_argument('--host',default='127.0.0.1');p.add_argument('--port',type=int,default=8765)
    p.add_argument('--geo-db',default=os.getenv('STANCE_H3_BLOCK_DB',''))
    p.add_argument('--zip-db',default=os.getenv('STANCE_ZIP_FALLBACK_DB','data/zip_fallback.sqlite'))
    p.add_argument('--origin',default='')
    a=p.parse_args()
    Handler.GEO_DB=a.geo_db;Handler.ZIP_DB=a.zip_db;Handler.ORIGIN=a.origin
    print(f'Local API: http://{a.host}:{a.port}/health',flush=True)
    ThreadingHTTPServer((a.host,a.port),Handler).serve_forever()
if __name__=='__main__':main()