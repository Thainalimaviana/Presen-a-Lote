import os
import threading
import time
import json
import sqlite3
from queue import Queue
from flask import Flask, request, jsonify, send_file
import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

API_BASE_URL = "https://homolog-presencabank.xptoconsig.com.br"
LOGIN_ENDPOINT = "/login"

LOGIN_USUARIO = os.getenv("PB_LOGIN", "70657909246_nCNQ")
LOGIN_SENHA = os.getenv("PB_SENHA", "Clik@70657909246")

RESULT_FOLDER = "resultados"
os.makedirs(RESULT_FOLDER, exist_ok=True)

DB_FILE = "margem_presenca.db"

fila_cpfs = Queue()
lotes = {}
token_atual = None

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS webhook_resultados (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cpf TEXT,
        tipo_evento TEXT,
        payload_json TEXT,
        data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS lote_cpfs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lote_id TEXT,
        cpf TEXT,
        status TEXT DEFAULT 'aguardando',
        resultado_json TEXT
    )
    """)

    conn.commit()
    conn.close()


def salvar_webhook(cpf, tipo, payload):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        INSERT INTO webhook_resultados (cpf, tipo_evento, payload_json)
        VALUES (?, ?, ?)
    """, (cpf, tipo, json.dumps(payload, ensure_ascii=False)))

    conn.commit()
    conn.close()

def atualizar_lote(cpf, payload):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        UPDATE lote_cpfs
        SET status = 'concluido',
            resultado_json = ?
        WHERE cpf = ?
    """, (json.dumps(payload, ensure_ascii=False), cpf))

    conn.commit()
    conn.close()

def obter_token():
    global token_atual

    url = API_BASE_URL + LOGIN_ENDPOINT
    payload = {
        "login": LOGIN_USUARIO,
        "senha": LOGIN_SENHA
    }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    print("→ Obtendo token...")

    r = requests.post(url, json=payload, headers=headers, verify=False)

    print("STATUS:", r.status_code)
    print("RESPOSTA:", r.text)

    if r.status_code != 200:
        print("Erro no login:", r.text)
        return None

    token_atual = r.json().get("token")
    print("→ Token obtido com sucesso!")
    return token_atual

app = Flask(__name__)


@app.route("/")
def home():
    return jsonify({"status": "online", "msg": "Presenca Bank Webhook ativo!"})

@app.post("/webhook-presenca")
def webhook_presenca():
    try:
        payload = request.json
        print("\n=== WEBHOOK RECEBIDO ===")
        print(json.dumps(payload, indent=4, ensure_ascii=False))

        tipo_evento = request.headers.get("webhook-type", "desconhecido")

        cpf = None

        if "operacaoId" in payload:
            cpf = "desconhecido"

        if "consultaMargem" in payload:
            cpf = payload["consultaMargem"]["document_number"]

        salvar_webhook(cpf, tipo_evento, payload)

        if cpf:
            atualizar_lote(cpf, payload)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("ERRO WEBHOOK:", e)
        return jsonify({"erro": str(e)}), 400

@app.post("/iniciar-lote")
def iniciar_lote():
    dados = request.json
    cpfs = dados["cpfs"]

    lote_id = str(int(time.time()))

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    for cpf in cpfs:
        c.execute("""
            INSERT INTO lote_cpfs (lote_id, cpf)
            VALUES (?, ?)
        """, (lote_id, cpf))

    conn.commit()
    conn.close()

    return jsonify({"lote_id": lote_id})


@app.get("/progresso/<lote_id>")
def progresso(lote_id):

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    c.execute("""
        SELECT cpf, status, resultado_json
        FROM lote_cpfs
        WHERE lote_id = ?
    """, (lote_id,))

    dados = c.fetchall()
    conn.close()

    total = len(dados)
    concluidos = sum(1 for row in dados if row[1] == "concluido")

    resultados = []
    for cpf, status, res in dados:
        resultados.append({
            "cpf": cpf,
            "status": status,
            "resultado": json.loads(res) if res else None
        })

    return jsonify({
        "total": total,
        "concluidos": concluidos,
        "resultados": resultados
    })

@app.get("/download-excel/<lote_id>")
def baixar_excel(lote_id):
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql_query(
        "SELECT cpf, status, resultado_json FROM lote_cpfs WHERE lote_id = ?",
        conn, params=(lote_id,)
    )
    conn.close()

    df["resultado"] = df["resultado_json"].apply(lambda v: json.loads(v) if v else None)
    df.drop(columns=["resultado_json"], inplace=True)

    caminho = f"{RESULT_FOLDER}/lote_{lote_id}.xlsx"
    df.to_excel(caminho, index=False)

    return send_file(caminho, as_attachment=True)


if __name__ == "__main__":
    init_db()
    obter_token()
    app.run(host="0.0.0.0", port=5000)
