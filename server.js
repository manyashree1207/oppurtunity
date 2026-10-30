const express = require("express");
const path = require("path");
require("dotenv").config();

const app = express();
const PORT = process.env.PORT || 3000;
app.use(express.json({ limit: "1mb" }));
app.use(express.static(__dirname));

app.get("/", (req, res) => res.sendFile(path.join(__dirname, "index.html")));

app.post("/api/opportunities", async (req, res) => {
  const apiKey = process.env.TINYFISH_API_KEY;
  if (!apiKey) return res.status(500).json({ error: "TINYFISH_API_KEY is missing from .env" });

  const { course = "Any field", year = "Any year", types = ["scholarship", "internship"], keyword = "" } = req.body || {};
  const typeText = Array.isArray(types) && types.length ? types.join(" and ") : "scholarships and internships";
  const goal = `
Find real, current scholarships and internships in India matching:
Course: ${course}; Study year: ${year}; Types: ${typeText}; Keyword: ${keyword || "any"}.
Search efficiently: use targeted searches based on the preferences, prioritize official government, university, and provider pages, and stop once you find 4 good matches. Do not search exhaustively or inspect unrelated pages.
Return ONLY verified opportunities found on pages. Do not invent names, dates, amounts, eligibility or URLs. Keep descriptions concise.
Return JSON only in this shape:
{"opportunities":[{"name":"","type":"scholarship or internship","provider":"","deadline":"Not specified","benefit":"Not specified","eligibility":"Not specified","description":"","source_url":"","application_url":""}]}
Include at most 4 results. Every result must have a real source_url. If a detail is missing, use "Not specified". If current availability cannot be verified, say so in description. Return an empty array if none can be verified.
`;
  const controller = new AbortController();
  let clientDisconnected = false;
  const onClientClose = () => {
    if (!res.writableEnded) {
      clientDisconnected = true;
      controller.abort();
    }
  };
  res.on("close", onClientClose);
  let upstream;
  try {
    upstream = await fetch("https://agent.tinyfish.ai/v1/automation/run-sse", {
      method: "POST",
      headers: { "X-API-Key": apiKey, "Content-Type": "application/json", "Accept": "text/event-stream" },
      body: JSON.stringify({
        url: "https://www.google.com/",
        goal,
        browser_profile: "lite"
      }),
      signal: controller.signal
    });
  } catch (e) {
    res.off("close", onClientClose);
    if (clientDisconnected) return;
    console.error("TinyFish connection error:", e.message);
    return res.status(502).json({ error: "Could not connect to TinyFish. Please try again." });
  }
  if (!upstream.ok) {
    res.off("close", onClientClose);
    const details = await upstream.text().catch(() => "");
    console.error("TinyFish HTTP error:", upstream.status, details);
    return res.status(upstream.status).json({ error: `TinyFish returned HTTP ${upstream.status}.`, details: details.slice(0, 1000) });
  }
  if (!upstream.body) {
    res.off("close", onClientClose);
    return res.status(502).json({ error: "TinyFish returned an empty stream." });
  }

  res.status(200);
  res.setHeader("Content-Type", "text/event-stream; charset=utf-8");
  res.setHeader("Cache-Control", "no-cache, no-transform");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders?.();

  const reader = upstream.body.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      res.write(Buffer.from(value));
    }
    res.end();
  } catch (e) {
    console.error("TinyFish stream error:", e.message);
    if (!res.writableEnded && !res.destroyed) {
      res.write(`data: ${JSON.stringify({ type: "ERROR", message: "TinyFish stream was interrupted." })}\n\n`);
      res.end();
    }
  } finally {
    res.off("close", onClientClose);
  }
});

app.listen(PORT, () => console.log(`Opportunity Hunter running at http://localhost:${PORT}`));
