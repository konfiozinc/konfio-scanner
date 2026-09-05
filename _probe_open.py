import time
from iqoptionapi.stable_api import IQ_Option
api = IQ_Option("***REDACTED***", "***REDACTED***")
ok, reason = api.connect()
print("CONNECT:", ok, reason)
if not ok:
    raise SystemExit("no conecto")
api.change_balance("PRACTICE")
time.sleep(4)
op = api.get_all_open_time()
print("TIPOS:", list(op.keys()))
for t in list(op.keys()):
    sub = op[t]
    print("TIPO", t, "->", list(sub.keys())[:5] if isinstance(sub, dict) else sub)
# Buscar activos concretos en cualquier tipo
for nombre in ["EURUSD", "EURUSD-OTC", "USDCAD", "USDCAD-OTC", "AUDUSD", "AUDUSD-OTC", "GBPUSD-OTC"]:
    estado = {}
    for t, sub in op.items():
        if nombre in sub:
            estado[t] = dict(sub[nombre])
    print("ACTIVO", nombre, "->", estado if estado else "no aparece")
api.__del__()
