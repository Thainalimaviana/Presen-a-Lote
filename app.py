from flask import Flask, request, jsonify, send_file
import time, threading, sqlite3, json, os
from queue import Queue
import pandas as pd

WEBHOOK_ENDPOINT = "/webhook-presenca"
RESULT_FOLDER = "resultados"
DB_FILE = "presenca_lotes.db"

os.makedirs(RESULT_FOLDER, exist_ok=True)

app = Flask(__name__)

fila_cpfs = Queue()
lotes = {}  # lote_id: {total, concluidos, pendentes:{cpf:{status,duracao,ini}}, resultados:[]}

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS resultados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lote_id TEXT,
        cpf TEXT,
        resultado_json TEXT
    )
    """)

    conn.commit()
    conn.close()

def salvar_resultado(lote_id, cpf, resultado):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        INSERT INTO resultados (lote_id, cpf, resultado_json)
        VALUES (?, ?, ?)
    """, (lote_id, cpf, json.dumps(resultado, ensure_ascii=False)))

    conn.commit()
    conn.close()


# ==========================
# RECEBIMENTO DO WEBHOOK
# ==========================
@app.post(WEBHOOK_ENDPOINT)
def webhook_presenca():
    dados = request.json
    print("\n=== WEBHOOK RECEBIDO ===")
    print(json.dumps(dados, indent=4, ensure_ascii=False))

    key = dados.get("keyRequest")
    consulta = dados.get("consultaMargem")
    vinculo = dados.get("vinculos")

    # NÃO TEM COMO SABER O CPF SEM O CLIENTE INFORMAR VIA API!
    # ENTÃO O CLIENTE DEVE INFORMAR "cpf" E "lote_id" NA CONSULTA
    cpf = dados.get("cpf")
    lote_id = dados.get("lote_id")

    if not lote_id or not cpf:
        return jsonify({"erro": "Webhook sem lote_id ou cpf"}), 400

    resultado = consulta if consulta else vinculo

    salvar_resultado(lote_id, cpf, resultado)

    # Atualiza lote
    if lote_id in lotes:
        lotes[lote_id]["concluidos"] += 1
        lotes[lote_id]["resultados"].append({
            "cpf": cpf,
            "resultado": resultado
        })

        if cpf in lotes[lote_id]["pendentes"]:
            lotes[lote_id]["pendentes"][cpf]["status"] = "finalizado"
            lotes[lote_id]["pendentes"][cpf]["fim"] = time.time()

    return jsonify({"ok": True})


# ==========================
# INICIAR LOTE
# ==========================
@app.post("/iniciar-lote")
def iniciar_lote():
    dados = request.json
    cpfs = dados.get("cpfs", [])

    lote_id = str(int(time.time()))

    lotes[lote_id] = {
        "total": len(cpfs),
        "concluidos": 0,
        "pendentes": {},
        "resultados": []
    }

    ts = time.time()

    for cpf in cpfs:
        lotes[lote_id]["pendentes"][cpf] = {
            "inicio": ts,
            "status": "aguardando"
        }

    return jsonify({"lote_id": lote_id})


# ==========================
# STATUS DO LOTE (FRONT USA)
# ==========================
@app.get("/api/lote-atualizado/<lote_id>")
def lote_atualizado(lote_id):
    if lote_id not in lotes:
        return jsonify({"erro": "lote não existe"})

    lote = lotes[lote_id]

    # calcular duração de cada pendente
    pendentes_out = {}

    for cpf, info in lote["pendentes"].items():
        inicio = info["inicio"]
        fim = info.get("fim")
        agora = time.time()

        duracao = (fim or agora) - inicio

        pendentes_out[cpf] = {
            "status": info["status"],
            "duracao": round(duracao, 1)
        }

    return jsonify({
        "total": lote["total"],
        "concluidos": lote["concluidos"],
        "pendentes": pendentes_out,
        "resultados": lote["resultados"]
    })


# ==========================
# PÁGINAS HTML
# ==========================
@app.get("/")
def index():
    return send_file("templates/index.html")

@app.get("/progresso")
def progresso_page():
    return send_file("templates/progresso.html")


init_db()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
