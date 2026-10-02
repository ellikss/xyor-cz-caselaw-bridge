import re
from datetime import date as date_cls
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

REPO = "legalize-dev/legalize-cz"
RAW_BASE = "https://raw.githubusercontent.com"
API_BASE = "https://api.github.com"

app = FastAPI(title="XYOR CZ Legislation Bridge", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)


def act_path(year: int, number: int) -> str:
    return f"cz/SB-{year}-{number}.md"


async def get_text(url: str, *, timeout: float = 30.0) -> str:
    async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
        response = await client.get(url, headers={"User-Agent": "XYOR-CZ-Legislation-Bridge/0.1"})
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail={"source_not_found": url})
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={"upstream_status": response.status_code, "upstream_url": url, "body": response.text[:1500]},
        )
    return response.text


async def get_json(url: str, params: dict[str, Any] | None = None) -> Any:
    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.get(
            url,
            params=params or {},
            headers={"Accept": "application/vnd.github+json", "User-Agent": "XYOR-CZ-Legislation-Bridge/0.1"},
        )
    if response.status_code == 404:
        raise HTTPException(status_code=404, detail={"source_not_found": str(response.request.url)})
    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail={
                "upstream_status": response.status_code,
                "upstream_url": str(response.request.url),
                "body": response.text[:1500],
                "note": "Public GitHub API is rate-limited; retry later if the limit is exhausted.",
            },
        )
    return response.json()


def parse_frontmatter(text: str) -> dict[str, str]:
    if not text.startswith("---\n"):
        return {}
    end = text.find("\n---\n", 4)
    if end < 0:
        return {}
    block = text[4:end]
    out: dict[str, str] = {}
    for line in block.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        value = value.strip().strip('"')
        out[key.strip()] = value
    return out


def normalize_label(label: str) -> str:
    s = re.sub(r"\s+", " ", label.strip())
    s = s.replace("§§", "§")
    return s


def extract_provision(text: str, label: str) -> dict[str, Any]:
    wanted = normalize_label(label)
    lines = text.splitlines()
    start = None
    matched = None
    provision_heading_re = re.compile(r"^#####\s+(§\s*[^\s]+|Čl\.\s*[^\s]+)")

    def comparable(s: str) -> str:
        return re.sub(r"\s+", "", s).lower().replace("čl.", "čl")

    target = comparable(wanted)
    for i, line in enumerate(lines):
        m = provision_heading_re.match(line.strip())
        if not m:
            continue
        current = normalize_label(m.group(1))
        if comparable(current) == target:
            start = i
            matched = current
            break
    if start is None:
        raise HTTPException(status_code=404, detail={"provision_not_found": wanted})

    end = len(lines)
    for j in range(start + 1, len(lines)):
        if provision_heading_re.match(lines[j].strip()):
            end = j
            break

    body = "\n".join(lines[start:end]).strip()
    return {
        "requested_label": wanted,
        "matched_label": matched,
        "start_line_1based": start + 1,
        "end_line_1based": end,
        "text": body,
    }


async def current_act(year: int, number: int) -> dict[str, Any]:
    path = act_path(year, number)
    raw_url = f"{RAW_BASE}/{REPO}/main/{path}"
    text = await get_text(raw_url)
    return {
        "repo": REPO,
        "ref": "main",
        "path": path,
        "raw_url": raw_url,
        "metadata": parse_frontmatter(text),
        "text": text,
        "temporal_status": "CURRENT_REPOSITORY_HEAD",
        "warning": "Verify the source metadata/effective date before applying the current consolidated wording to a legal question.",
    }


async def historical_act(year: int, number: int, on_date: str) -> dict[str, Any]:
    try:
        parsed = date_cls.fromisoformat(on_date)
    except ValueError:
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")

    path = act_path(year, number)
    commits_url = f"{API_BASE}/repos/{REPO}/commits"
    commits = await get_json(
        commits_url,
        params={"path": path, "until": f"{parsed.isoformat()}T23:59:59Z", "per_page": 1},
    )
    if not commits:
        raise HTTPException(
            status_code=404,
            detail={"historical_snapshot_not_found": {"path": path, "date": on_date}},
        )
    commit = commits[0]
    sha = commit.get("sha")
    commit_meta = commit.get("commit") or {}
    raw_url = f"{RAW_BASE}/{REPO}/{sha}/{path}"
    text = await get_text(raw_url)
    return {
        "repo": REPO,
        "ref": sha,
        "path": path,
        "requested_date": on_date,
        "commit_sha": sha,
        "commit_date": ((commit_meta.get("committer") or {}).get("date")),
        "commit_message": commit_meta.get("message"),
        "html_url": commit.get("html_url"),
        "raw_url": raw_url,
        "metadata": parse_frontmatter(text),
        "text": text,
        "temporal_status": "REPOSITORY_SNAPSHOT_AT_OR_BEFORE_REQUESTED_DATE",
        "warning": (
            "Repository snapshot date is provenance, not by itself a legal conclusion about applicability/effectiveness. "
            "Historical snapshots may contain frontmatter generated from later/current e-Sbírka metadata. "
            "Anchor historical conclusions to commit SHA/date plus exact historical body/diff and verify the legal effective date."
        ),
    }


@app.get("/")
async def root():
    return {
        "service": "XYOR CZ Legislation Bridge",
        "version": "0.1.0",
        "mode": "public-read-only",
        "upstream_repo": REPO,
        "endpoints": ["/health", "/act/{year}/{number}", "/provision/{year}/{number}", "/act-at/{year}/{number}", "/provision-at/{year}/{number}"],
    }


@app.get("/health")
async def health():
    return {"ok": True, "service": "XYOR CZ Legislation Bridge", "version": "0.1.0", "upstream_repo": REPO}


@app.get("/act/{year}/{number}")
async def get_current_act(year: int, number: int):
    return await current_act(year, number)


@app.get("/provision/{year}/{number}")
async def get_current_provision(year: int, number: int, label: str = Query(..., min_length=1)):
    act = await current_act(year, number)
    provision = extract_provision(act["text"], label)
    act.pop("text", None)
    return {"act": act, "provision": provision}


@app.get("/act-at/{year}/{number}")
async def get_historical_act(year: int, number: int, date: str = Query(...)):
    return await historical_act(year, number, date)


@app.get("/provision-at/{year}/{number}")
async def get_historical_provision(
    year: int,
    number: int,
    date: str = Query(...),
    label: str = Query(..., min_length=1),
):
    act = await historical_act(year, number, date)
    provision = extract_provision(act["text"], label)
    act.pop("text", None)
    return {"act": act, "provision": provision}
