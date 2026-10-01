# XYOR CZ Case-Law Bridge

Temporary, read-only cloud bridge for the XYOR | LEX Czech investor demo.

## Purpose

Expose a narrow HTTP layer over the Hugging Face Dataset Viewer API for:

- `/health`
- `/splits`
- `/rows`
- `/search`
- `/filter`

The service does not store the Czech court corpus and does not become an authority layer.
It only retrieves rows from `overthelex/cz-court-decisions`.

## Render

Build command:

    pip install -r requirements.txt

Start command:

    uvicorn app:app --host 0.0.0.0 --port $PORT

## First checks

    GET /health
    GET /splits

Then use the config names returned by `/splits`, likely including `justice` and `czcdc`,
for `/rows`, `/search`, and `/filter`.

## Security / disposal

- read-only
- no local XYOR code
- no secrets required for the public HF dataset
- delete the Render service after the investor demo
