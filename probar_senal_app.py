# -*- coding: utf-8 -*-
"""
==========================================================
AppALÍ GSR Scanner - PRUEBA DE PUBLICACIÓN EN LA APP
==========================================================
Envía una señal de PRUEBA a Alí Binary Options para comprobar que la integración
funciona de punta a punta, sin esperar a que el mercado dé una señal real.

Dos modos:

  1) Contra la configuración real del .env (Cloud Function o Firestore):
         venv311\\Scripts\\python.exe probar_senal_app.py
     Requiere APP_SIGNAL_URL + APP_SIGNAL_SECRET (opción A) o FIREBASE_SA_PATH
     (opción B). Si funciona, verás la señal en la app.

  2) --local : levanta un servidor local que hace de Cloud Function, captura el
     payload y lo muestra. Sirve para validar el formato exacto SIN desplegar
     nada y sin escribir en Firestore.

Uso:
    venv311\\Scripts\\python.exe probar_senal_app.py --local
    venv311\\Scripts\\python.exe probar_senal_app.py --asset EURUSD-OTC --dir CALL
"""
import argparse
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, BASE_DIR)

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

os.environ["APPALI_NO_FILE_LOG"] = "1"      # no ensuciar scanner.log
import app_ali as A  # noqa: E402

CAPTURADO = {}


def _servidor_local(puerto=8799):
    """Servidor de un solo uso que imprime el payload recibido."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            n = int(self.headers.get("Content-Length", 0))
            cuerpo = self.rfile.read(n).decode("utf-8", "replace")
            CAPTURADO["ruta"] = self.path
            CAPTURADO["payload"] = cuerpo
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"ok":true}')

        def log_message(self, *a):
            pass

    srv = HTTPServer(("127.0.0.1", puerto), Handler)
    threading.Thread(target=srv.handle_request, daemon=True).start()
    return srv


def main():
    ap = argparse.ArgumentParser(description="Prueba de publicación en Alí Binary Options")
    ap.add_argument("--local", action="store_true",
                    help="usa un servidor local como Cloud Function (no toca Firestore)")
    ap.add_argument("--asset", default="EURUSD-OP", help="activo (default EURUSD-OP)")
    ap.add_argument("--dir", default="CALL", choices=["CALL", "PUT"], help="dirección")
    ap.add_argument("--rsi", type=float, default=84.2)
    ap.add_argument("--damoa", type=float, default=12.4)
    args = ap.parse_args()

    print("=" * 70)
    print("  PRUEBA DE PUBLICACIÓN EN ALÍ BINARY OPTIONS")
    print("=" * 70)

    srv = None
    if args.local:
        srv = _servidor_local()
        A.APP_SIGNAL_URL = "http://127.0.0.1:8799/postSignal"
        A.APP_SIGNAL_SECRET = A.APP_SIGNAL_SECRET or "secreto-de-prueba"
        print("  Modo LOCAL: se usará un servidor de prueba en 127.0.0.1:8799")
    else:
        print(f"  APP_SIGNAL_URL       : {A.APP_SIGNAL_URL or '(vacío)'}")
        print(f"  FIREBASE_SA_PATH     : {A.FIREBASE_SA_PATH or '(vacío)'}")
        if not A.APP_SIGNAL_URL and not A.FIREBASE_SA_PATH:
            print("\n  ERROR: no hay ninguna vía configurada en .env.")
            print("         Configura APP_SIGNAL_URL + APP_SIGNAL_SECRET (opción A)")
            print("         o FIREBASE_SA_PATH (opción B), o usa --local para probar")
            print("         sólo el formato del payload.")
            return 2

    print(f"  Estado según configuración: {A._estado_app()['state']} "
          f"({A._estado_app()['detail']})")

    hora_cot = A.broker_clock.now_colombia().strftime("%H:%M")
    print(f"\n  Enviando señal de PRUEBA: {args.asset} {args.dir} "
          f"(RSI {args.rsi}, DAMOA {args.damoa}, hora {hora_cot})...")

    ok_a = A.enviar_alerta_app(args.asset, args.dir, args.rsi, args.damoa, 75.0, hora_cot)
    ok_b = A.enviar_alerta_firestore(args.asset, args.dir, args.rsi, args.damoa, 75.0, hora_cot)

    if args.local:
        time.sleep(0.5)
        print("\n  --- payload recibido por el servidor de prueba ---")
        if "payload" in CAPTURADO:
            try:
                print("  " + json.dumps(json.loads(CAPTURADO["payload"]),
                                        indent=2, ensure_ascii=False).replace("\n", "\n  "))
            except Exception:
                print("  " + CAPTURADO["payload"])
            print("\n  RESULTADO: el payload se envía correctamente (formato válido).")
            print("  Para la prueba REAL, despliega la Cloud Function, pon su URL en")
            print("  APP_SIGNAL_URL y ejecuta este script sin --local.")
        else:
            print("  NO llegó ningún payload -> revisa la función enviar_alerta_app().")
            return 1
    else:
        if ok_a:
            print("\n  RESULTADO: la Cloud Function aceptó la señal (opción A).")
            print("  Comprueba en la app: debería aparecer en la colección 'signals'.")
        if ok_b:
            print("\n  RESULTADO: la señal se escribió en Firestore (opción B).")
        if not ok_a and not ok_b:
            print("\n  RESULTADO: no se pudo publicar. Revisa los avisos de arriba.")
            return 1

    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
