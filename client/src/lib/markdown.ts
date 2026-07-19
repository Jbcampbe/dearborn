// Minimal markdown renderer for agent-generated chat/context text. Safety
// model: the source is HTML-escaped FIRST, then a small subset of markdown
// (code fences, inline code, bold, italic, headings, lists) is expanded — so
// no raw HTML from the model can ever reach the DOM. Output is meant for
// `v-html` inside a `.md` container (styles live in ui.css).

function escapeHtml(s: string): string {
  return s
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

/** Inline formatting on already-escaped text. */
function inline(s: string): string {
  return s
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<strong>$1</strong>")
    .replace(/(^|[^*])\*([^*\n]+)\*(?!\*)/g, "$1<em>$2</em>");
}

/** Block-level rendering for a non-code-fence segment. */
function renderBlocks(s: string): string {
  const lines = s.split("\n");
  const out: string[] = [];
  let list: "ul" | "ol" | null = null;
  let para: string[] = [];

  const flushPara = () => {
    if (para.length > 0) {
      out.push(`<p>${para.map(inline).join("<br>")}</p>`);
      para = [];
    }
  };
  const flushList = () => {
    if (list !== null) {
      out.push(`</${list}>`);
      list = null;
    }
  };

  for (const line of lines) {
    const trimmed = line.trim();
    const heading = /^(#{1,4})\s+(.*)$/.exec(trimmed);
    const ul = /^[-*]\s+(.*)$/.exec(trimmed);
    const ol = /^\d+[.)]\s+(.*)$/.exec(trimmed);

    if (heading) {
      flushPara();
      flushList();
      out.push(`<span class="md-h">${inline(heading[2])}</span>`);
    } else if (ul) {
      flushPara();
      if (list !== "ul") {
        flushList();
        out.push("<ul>");
        list = "ul";
      }
      out.push(`<li>${inline(ul[1])}</li>`);
    } else if (ol) {
      flushPara();
      if (list !== "ol") {
        flushList();
        out.push("<ol>");
        list = "ol";
      }
      out.push(`<li>${inline(ol[1])}</li>`);
    } else if (trimmed === "") {
      flushPara();
      flushList();
    } else {
      flushList();
      para.push(trimmed);
    }
  }
  flushPara();
  flushList();
  return out.join("");
}

/** Render a markdown string to a safe HTML string. */
export function renderMarkdown(src: string): string {
  const escaped = escapeHtml(src);
  // Split on code fences; odd indices are fenced code.
  const parts = escaped.split(/```/);
  let html = "";
  for (let i = 0; i < parts.length; i++) {
    if (i % 2 === 1) {
      // The first line of a fence may be a language tag — drop it.
      const chunk = parts[i].replace(/^[a-zA-Z0-9_-]*\n/, "").replace(/\n$/, "");
      html += `<pre class="md-pre"><code>${chunk}</code></pre>`;
    } else {
      html += renderBlocks(parts[i]);
    }
  }
  return html;
}
