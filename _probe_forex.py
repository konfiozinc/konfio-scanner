import time, json
from concurrent.futures import ThreadPoolExecutor
from iqoptionapi.stable_api import IQ_Option
api = IQ_Option("***REDACTED***", "***REDACTED***")
print("CONNECT:", api.connect())
api.change_balance("PRACTICE")
time.sleep(2)
try:
    with ThreadPoolExecutor(max_workers=1) as ex:
        data = ex.submit(api.get_instruments, "forex").result(timeout=40)
    if not isinstance(data, dict):
        print("NO_DICT:", type(data)); raise SystemExit
    ins = data.get("instruments")
    print("forex instrumentos:", len(ins) if isinstance(ins, list) else type(ins))
    now = int(api.get_server_timestamp())
    print("SERVER_TS:", now)
    for d in (ins if isinstance(ins, list) else [])[:6]:
        nm = d.get("name"); sched = d.get("schedule")
        abierto = False
        for sc in (sched or []):
            if int(sc.get("open",0)) < now < int(sc.get("close",0)):
                abierto = True
        print(f"{nm:10} open={abierto} schedule={sched[:1] if sched else None}")
    eur = [d for d in ins if d.get('name')=='EURUSD']
    print("EURUSD:", json.dumps(eur[0]) if eur else "no está en forex")
except Exception as e:
    print("ERROR:", type(e).__name__, str(e)[:200])
