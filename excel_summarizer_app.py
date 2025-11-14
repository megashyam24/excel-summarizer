# app_simple_ui.py
"""
Flask app — simple, clean UI (no DataTables).
Upload .xls/.xlsx/.xlsb/.csv/.txt — app shortens Party names, inserts subtotal rows,
shows preview on the same page (scrollable) and lets you download the modified Excel.
"""

import io, os, re, tempfile, atexit
from pathlib import Path
from flask import Flask, request, render_template_string, redirect, url_for, flash, send_file
from werkzeug.utils import secure_filename
import pandas as pd

app = Flask(__name__)
app.secret_key = "change-this-secret-to-something-random"

# ---------- Mapping (same as before) ----------
MAPPING = [
    ("AKZO NOBEL", "AKZO NOBEL"),
    ("AKZO NOBEL INDIA LTD", "AKZO NOBEL"),
    ("ASIAN PAINTS", "ASIAN"),
    ("ESDEE PAINTS", "ESDEE"),
    ("INDIGO PAINTS", "INDIGO"),
    ("SIMPSON & CO", "SIMPSON"),
    ("APC-DIVISION", "SIMPSON"),
    ("VEENA PAINTS", "VEENA"),
    ("T.A.L.C.ANNAMALAI NADAR", "T.A.L.C"),
    ("UTTAM ELECTRONICS", "UTTAM"),
    ("GEETHA PAINTS", "GEETHA PAINTS"),
    ("BALAJI INDUSTRIES", "BALAJI IND"),
    ("T.A.L.C.A.SATCHITHANANTHAM", "T.A.L.C.A.SA"),
    ("SPECTRUM SURFACE SOLUTIONS", "SPECTRUM"),
    ("GLOBAL PAINTS", "GLOBAL PAINTS"),
    ("JPJ AGENCIES", "JPJ AGENCIES"),
    ("SRI VELAVAN TRADERS", "SRI VELAVAN"),
    ("ASCKANIA CHEMICALS", "ASCKANIA"),
    ("SRI MARUTI EXPORTS", "SRI MARUTI"),
    ("SREE VALAMPURI AGENCIES", "SREE VALAMPURI"),
    ("SENTHIL CORPORATION", "SENTHIL CORP"),
    ("SENTHI AGENCY", "SENTHI AGENCY"),
    ("SRI ANDAL SALES CORPORATION", "SRI ANDAL"),
    ("JOTHI TRADERS", "JOTHI TRADERS"),
    ("GANESH ENTERPRISES", "GANESH EP"),
    ("NIVIN BRUSH", "NIVIN BRUSH"),
]

def normalize_for_match(s: str) -> str:
    if s is None:
        return ""
    return re.sub(r'[^A-Z0-9]+', '', str(s).upper())

MAPPING_NORMALIZED = [(normalize_for_match(k), v) for k, v in MAPPING]

SUFFIX_WORDS = [
    "INDIA", "LTD", "LIMITED", "PVT", "PRIVATE", "COMPANY", "CO",
    "PVT.", "LTD.", "PVT LTD", "DIVISION", "APC", "APC-DIVISION"
]
SUFFIX_RE = re.compile(r'\b(?:' + '|'.join([re.escape(s) for s in SUFFIX_WORDS]) + r')\b', flags=re.IGNORECASE)

def fallback_shorten(name: str) -> str:
    if pd.isna(name):
        return ""
    s = str(name).strip()
    s = re.sub(r'\(.*?\)', '', s)
    if '-' in s:
        s = s.split('-', 1)[0].strip()
    if '/' in s:
        s = s.split('/', 1)[0].strip()
    s = SUFFIX_RE.sub("", s).strip()
    s = re.sub(r'\s{2,}', ' ', s).strip()
    return s.upper() if s else str(name).strip().upper()

def determine_party_short(original_name: str) -> str:
    norm = normalize_for_match(original_name)
    for key_norm, short in MAPPING_NORMALIZED:
        if key_norm and key_norm in norm:
            return short.upper()
    return fallback_shorten(original_name)

# ---------- .xls converter (bytes) ----------
def convert_xls_bytes_to_xlsx_bytes(xls_bytes: bytes) -> bytes:
    import xlrd
    from openpyxl import Workbook
    from io import BytesIO
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xls")
    try:
        tmp.write(xls_bytes)
        tmp.flush()
        tmp.close()
        book = xlrd.open_workbook(tmp.name, formatting_info=False)
        wb = Workbook()
        try:
            wb.remove(wb.active)
        except Exception:
            pass
        for i in range(book.nsheets):
            sh = book.sheet_by_index(i)
            ws = wb.create_sheet(title=(sh.name[:31] if sh.name else f"Sheet{i+1}"))
            for r in range(sh.nrows):
                ws.append(sh.row_values(r))
        bio = BytesIO()
        wb.save(bio)
        return bio.getvalue()
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass

# ---------- Smart reader ----------
def smart_read_file(file_name: str, content: bytes) -> pd.DataFrame:
    suf = Path(file_name).suffix.lower()
    if suf == ".xls":
        xlsx_bytes = convert_xls_bytes_to_xlsx_bytes(content)
        return pd.read_excel(io.BytesIO(xlsx_bytes), engine="openpyxl")
    elif suf in (".xlsx", ".xlsm", ".xltx", ".xltm"):
        return pd.read_excel(io.BytesIO(content), engine="openpyxl")
    elif suf == ".xlsb":
        return pd.read_excel(io.BytesIO(content), engine="pyxlsb")
    elif suf in (".csv", ".txt"):
        return pd.read_csv(io.BytesIO(content))
    else:
        raise ValueError("Unsupported extension. Use .xls/.xlsx/.xlsb/.csv/.txt")

# ---------- Summarize ----------
def summarize_df(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.strip() for c in df.columns]
    if 'Party' not in df.columns or 'GSTIN' not in df.columns:
        raise ValueError("Input must contain 'Party' and 'GSTIN' columns.")
    num_cols = ['TAXABLE', 'IGST', 'CGST', 'SGST', 'NETAMOUNT']
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c].astype(str).str.replace(',', ''), errors='coerce').fillna(0)
        else:
            df[c] = 0
    df['Party_Short'] = df['Party'].apply(determine_party_short)
    df['GSTIN'] = df['GSTIN'].astype(str).replace('nan','')
    ordered = []
    seen = set()
    for ps, gst in zip(df['Party_Short'], df['GSTIN']):
        key = (ps, gst)
        if key not in seen:
            ordered.append(key)
            seen.add(key)
    out_rows = []
    final_cols = [c for c in df.columns if c != 'Party_Short']
    for party_short, gstin_key in ordered:
        mask = (df['Party_Short'] == party_short) & (df['GSTIN'] == gstin_key)
        grp = df[mask]
        for _, r in grp.iterrows():
            row = {}
            for c in final_cols:
                row[c] = party_short if c == 'Party' else r[c]
            out_rows.append(row)
        sums = {c: round(float(grp[c].sum()), 2) for c in ['TAXABLE','IGST','CGST','SGST','NETAMOUNT']}
        summary = {c: '' for c in final_cols}
        summary['GSTIN'] = gstin_key
        summary['Party'] = party_short
        summary['TAXABLE'] = sums['TAXABLE']
        if 'TAXPER' in final_cols:
            summary['TAXPER'] = ''
        summary['IGST'] = '' if sums['IGST'] == 0 else sums['IGST']
        summary['CGST'] = '' if sums['CGST'] == 0 else sums['CGST']
        summary['SGST'] = '' if sums['SGST'] == 0 else sums['SGST']
        summary['NETAMOUNT'] = sums['NETAMOUNT']
        out_rows.append(summary)
    res = pd.DataFrame(out_rows, columns=final_cols)
    for c in ['TAXABLE','IGST','CGST','SGST','NETAMOUNT']:
        if c in res.columns:
            def fmt(x):
                if x == '':
                    return ''
                try:
                    return round(float(x), 2)
                except Exception:
                    return x
            res[c] = res[c].apply(fmt)
    return res

# ---------- Temp file registry ----------
TMP_INDEX = {}
def save_tempfile_and_register(df: pd.DataFrame) -> str:
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx")
    tmp.close()
    df.to_excel(tmp.name, index=False, engine="openpyxl")
    token = Path(tmp.name).name
    TMP_INDEX[token] = tmp.name
    return token

def cleanup_tempfiles():
    for token, path in list(TMP_INDEX.items()):
        try:
            os.remove(path)
        except Exception:
            pass
    TMP_INDEX.clear()

atexit.register(cleanup_tempfiles)

# ---------- Simple, nicer template ----------
HTML = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Excel Summarizer — Clean UI</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- Bootstrap 5 -->
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    body{background:#f4f6f9; padding:30px;}
    .card { box-shadow: 0 10px 30px rgba(18,38,63,0.06); border:0; }
    .preview-wrap { max-height: 68vh; overflow:auto; padding:12px; background:#fff; border-radius:6px; border:1px solid #eef0f4; }
    .summary-row { background:#f1f5f9; font-weight:600; }
    .small-muted { color:#6c757d; font-size:0.9rem; }
    .brand { color:#0d6efd; font-weight:700; }
    table { font-size:0.92rem; }
    th { background:#fafbfc; position: sticky; top: 0; z-index:2; }
  </style>
</head>
<body>
<div class="container">
  <div class="row justify-content-center mb-4">
    <div class="col-lg-10">
      <div class="card p-4">
        <div class="d-flex justify-content-between align-items-start mb-3">
          <div>
            <h4 class="mb-0 brand">Excel Summarizer</h4>
            <div class="small-muted">Upload .xls/.xlsx/.xlsb/.csv/.txt — app shortens Party names, inserts subtotal rows and provides a downloadable modified file.</div>
          </div>
          <div class="text-end">
            <small class="text-muted">Local only • No files are uploaded to internet</small>
          </div>
        </div>

        {% with messages = get_flashed_messages() %}
          {% if messages %}
            {% for msg in messages %}
              <div class="alert alert-warning small mb-3">{{msg}}</div>
            {% endfor %}
          {% endif %}
        {% endwith %}

        <form method="post" enctype="multipart/form-data" class="row g-2 mb-3">
          <div class="col-md-9">
            <input class="form-control" type="file" name="file" accept=".xls,.xlsx,.xlsb,.csv,.txt" required>
          </div>
          <div class="col-md-3 d-grid">
            <button class="btn btn-primary" type="submit">Upload & Process</button>
          </div>
        </form>

        {% if preview_html %}
        <div class="mb-3">
          <div class="d-flex justify-content-between align-items-center mb-2">
            <div>
              <strong>Preview</strong>
              <span class="small-muted"> — showing first {{nrows}} rows (scroll inside the box)</span>
            </div>
            <div>
              <a class="btn btn-success btn-sm" href="{{download_url}}" role="button">Download modified Excel</a>
            </div>
          </div>

          <div class="preview-wrap">
            {{preview_html|safe}}
          </div>

          <div class="mt-2 small-muted">
            Rows: {{rows_count}} &nbsp; • &nbsp; Groups: {{groups_count}}
          </div>
        </div>
        {% endif %}

        <div class="mt-2 small-muted">Tip: Use browser zoom out if the table is wide. Large files may take a few seconds to process.</div>
      </div>
    </div>
  </div>
</div>

<!-- JS: Bootstrap -->
<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>

<script>
// highlight summary rows (where bl_invno cell is empty)
(function(){
  try {
    const tbl = document.querySelector('.preview-wrap table');
    if (!tbl) return;
    const headers = Array.from(tbl.querySelectorAll('thead th')).map(th => th.innerText.trim().toLowerCase());
    const invIndex = headers.indexOf('bl_invno') >= 0 ? headers.indexOf('bl_invno') : headers.indexOf('bl_invno'.toLowerCase());
    // loop tbody rows and add class if the bl_invno cell text is empty
    tbl.querySelectorAll('tbody tr').forEach(tr => {
      const tds = tr.querySelectorAll('td');
      if (tds.length > invIndex && invIndex >= 0) {
        const v = tds[invIndex].innerText.trim();
        if (!v) tr.classList.add('summary-row');
      }
    });
  } catch (e) {
    // ignore
  }
})();
</script>
</body>
</html>
"""

# ---------- Routes ----------
@app.route("/", methods=["GET", "POST"])
def index():
    if request.method == "POST":
        uploaded = request.files.get("file")
        if not uploaded:
            flash("No file uploaded")
            return redirect(request.url)
        filename = secure_filename(uploaded.filename)
        try:
            content = uploaded.read()
            df = smart_read_file(filename, content)
            df_out = summarize_df(df)
            token = save_tempfile_and_register(df_out)
            download_url = url_for("download", token=token)
            # show first N rows in preview but still single page
            N_PREVIEW = min(500, len(df_out))
            preview_html = df_out.head(N_PREVIEW).to_html(index=False, classes="table table-sm table-bordered", na_rep="")
            rows_count = len(df_out)
            # groups count: count summary rows we inserted (approx by checking blank bl_invno)
            groups_count = df_out[df_out['bl_invno'].astype(str).str.strip() == ''].shape[0] if 'bl_invno' in df_out.columns else 0
            return render_template_string(HTML,
                                          preview_html=preview_html,
                                          download_url=download_url,
                                          nrows=N_PREVIEW,
                                          rows_count=rows_count,
                                          groups_count=groups_count)
        except Exception as e:
            flash(f"Error processing file: {e}")
            return redirect(request.url)
    return render_template_string(HTML, preview_html=None)

@app.route("/download/<token>")
def download(token):
    path = TMP_INDEX.get(token)
    if not path or not os.path.exists(path):
        return "File not found or expired", 404
    return send_file(path, as_attachment=True, download_name=f"{Path(path).stem}_modified.xlsx")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
