const askBtn = document.getElementById("ask");
const answerDiv = document.getElementById("answer");
const citationsDiv = document.getElementById("citations");
const apiKeyInput = document.getElementById("apiKey");

let sessionId = null;

askBtn.addEventListener("click", async () => {
  const question = document.getElementById("question").value.trim();
  if (!question) return;

  answerDiv.textContent = "思考中...";
  citationsDiv.innerHTML = "";

  const resp = await fetch("/chat", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-API-Key": apiKeyInput.value.trim(),
    },
    body: JSON.stringify({ session_id: sessionId, message: question }),
  });

  if (!resp.ok) {
    answerDiv.textContent = `请求失败: ${resp.status}`;
    return;
  }

  const data = await resp.json();
  sessionId = data.session_id;
  answerDiv.textContent = data.answer;

  (data.citations || []).forEach((c) => {
    const div = document.createElement("div");
    div.className = "citation";
    div.textContent = `[${c.index}] ${c.source_path} (page ${c.page})\n${c.snippet}`;
    citationsDiv.appendChild(div);
  });
});
