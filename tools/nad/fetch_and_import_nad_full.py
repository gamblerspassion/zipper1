#!/usr/bin/env python3
"""Download official NAD r24 public address archive, resume if supported, then import."""
import argparse,subprocess,sys,time,urllib.request,shutil
from pathlib import Path
URL="https://data.transportation.gov/download/fc2s-wawr/application/x-zip-compressed"
def fetch(path):
    path.parent.mkdir(parents=True,exist_ok=True)
    for attempt in range(5):
        pos=path.stat().st_size if path.exists() else 0
        headers={"User-Agent":"STANCE-NAD-Source-Archive/1.0"}
        if pos:headers['Range']=f'bytes={pos}-'
        try:
            request=urllib.request.Request(URL,headers=headers)
            with urllib.request.urlopen(request,timeout=120) as r:
                if pos and r.status==200:path.unlink();pos=0
                with path.open('ab' if pos and r.status==206 else 'wb') as f:
                    shutil.copyfileobj(r,f,1024*1024)
            return
        except Exception as e:
            print("download retry",attempt+1,repr(e),flush=True)
            time.sleep(min(25,3*(attempt+1)))
    raise RuntimeError("Official public archive not downloadable; no empty results published.")
def main():
    p=argparse.ArgumentParser()
    p.add_argument('--archive',default='nad-r24-download/NAD_r24_TXT.zip')
    p.add_argument('--out',default='nad-r24-state-addresses')
    a=p.parse_args()
    archive=Path(a.archive)
    fetch(archive)
    subprocess.run([sys.executable,str(Path(__file__).parent/'import_nad_full.py'),str(archive),a.out],check=True)
if __name__=='__main__':main()
