let loteId = null;
let monitor = null;

document.getElementById("iniciarBtn").addEventListener("click", async () => {
    let lista = document.getElementById("cpfLista").value
        .split("\n")
        .map(x => x.trim())
        .filter(x => x.length >= 11);

    const arquivo = document.getElementById("csvFile").files[0];

    if (arquivo) {
        const text = await arquivo.text();
        const linhas = text.split("\n").map(x => x.trim());
        lista = lista.concat(linhas);
    }

    if (lista.length === 0) {
        alert("Insira CPFs ou carregue um CSV!");
        return;
    }

    const resp = await fetch("/iniciar-lote", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ cpfs: lista })
    });

    const data = await resp.json();
    loteId = data.lote_id;

    document.getElementById("progressoArea").classList.remove("hidden");

    monitor = setInterval(checarProgresso, 1500);
});


async function checarProgresso() {
    const resp = await fetch(`/progresso/${loteId}`);
    const data = await resp.json();

    const total = data.total || 0;
    const concluidos = data.concluidos || 0;

    document.getElementById("progressoTexto").innerText =
        `Concluídos: ${concluidos} / ${total}`;

    const pct = total > 0 ? (concluidos / total) * 100 : 0;
    document.getElementById("progressFill").style.width = pct + "%";

    const tbody = document.querySelector("#resultadoTable tbody");
    tbody.innerHTML = "";

    (data.resultados || []).forEach(item => {
        const tr = document.createElement("tr");

        const cpfTd = document.createElement("td");
        cpfTd.innerText = item.cpf;

        const resTd = document.createElement("td");
        resTd.innerText = JSON.stringify(item.resultado);

        tr.appendChild(cpfTd);
        tr.appendChild(resTd);

        tbody.appendChild(tr);
    });

    if (total > 0 && concluidos === total) {
        clearInterval(monitor);
    }
}


document.getElementById("baixarExcel").addEventListener("click", () => {
    if (!loteId) return;
    window.location.href = `/download-excel/${loteId}`;
});

document.getElementById("baixarCsv").addEventListener("click", () => {
    if (!loteId) return;
    window.location.href = `/download-csv/${loteId}`;
});
