import os
import time
import json
import sqlite3
import threading
from queue import Queue
from flask import Flask, request, jsonify, send_file

WEBHOOK_ENDPOINT = "/webhook-presenca"
RESULT_FOLDER = "resultados"
DB_FILE = "presenca_lotes.db"

os.makedirs(RESULT_FOLDER, exist_ok=True)

app = Flask(__name__)

fila_cpfs = Queue()
lotes = {} 

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

@app.post(WEBHOOK_ENDPOINT)
def webhook_presenca():
    try:
        dados = request.json
        cpf = dados.get("cpf")
        lote_id = dados.get("lote_id")
        resultado = dados.get("resultado")

        if lote_id in lotes:
            lotes[lote_id]["concluidos"] += 1

            lotes[lote_id]["pendentes"][cpf]["status"] = "RECEBIDO_WEBHOOK"
            lotes[lote_id]["pendentes"][cpf]["fim"] = time.time()
            lotes[lote_id]["pendentes"][cpf]["duracao"] = round(
                lotes[lote_id]["pendentes"][cpf]["fim"] - lotes[lote_id]["pendentes"][cpf]["inicio"], 2
            )

            lotes[lote_id]["resultados"].append({
                "cpf": cpf,
                "resultado": resultado
            })

        salvar_resultado(lote_id, cpf, resultado)
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        return jsonify({"erro": str(e)}), 500

@app.get("/")
def index():
    return send_file("templates/index.html")


@app.get("/progresso")
def progresso_page():
    return send_file("templates/progresso.html")

@app.get("/status-lote/<lote_id>")
def status_lote(lote_id):
    return jsonify(lotes.get(lote_id, {}))

@app.get("/download-excel/<lote_id>")
def download_excel(lote_id):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("""
        SELECT cpf, resultado_json
        FROM resultados
        WHERE lote_id = ?
    """, conn, params=(lote_id,))
    conn.close()

    df["resultado"] = df["resultado_json"].apply(json.loads)
    df.drop(columns=["resultado_json"], inplace=True)

    caminho = f"{RESULT_FOLDER}/lote_{lote_id}.xlsx"
    df.to_excel(caminho, index=False)

    return send_file(caminho, as_attachment=True)


@app.get("/download-csv/<lote_id>")
def download_csv(lote_id):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query("""
        SELECT cpf, resultado_json
        FROM resultados
        WHERE lote_id = ?
    """, conn, params=(lote_id,))
    conn.close()

    df["resultado"] = df["resultado_json"].apply(json.loads)
    df.drop(columns=["resultado_json"], inplace=True)

    caminho = f"{RESULT_FOLDER}/lote_{lote_id}.csv"
    df.to_csv(caminho, index=False)

    return send_file(caminho, as_attachment=True)

@app.post("/iniciar-lote")
def iniciar_lote():
    dados = request.json
    cpfs = dados.get("cpfs", [])

    lote_id = str(int(time.time()))

    lotes[lote_id] = {
        "total": len(cpfs),
        "concluidos": 0,
        "resultados": [],
        "pendentes": {cpf: {
            "status": "AGUARDANDO_WEBHOOK",
            "inicio": time.time()
        } for cpf in cpfs}
    }

    return jsonify({
        "lote_id": lote_id
    })

@app.get("/api/lote-atualizado/<lote_id>")
def lote_atualizado(lote_id):
    dados = lotes.get(lote_id)
    if not dados:
        return jsonify([])

    return jsonify({
        "total": dados["total"],
        "concluidos": dados["concluidos"],
        "resultados": dados["resultados"],
        "atualizado_em": time.time()
    })

if __name__ == "__main__":
    init_db()
    app.run(host="0.0.0.0", port=5000, debug=True)
