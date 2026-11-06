# Request Path Limits (freeunit issue #923, 1.37.2)

The server work tracked in [freeunit issue #923](https://github.com/freeunitorg/freeunit/issues/923)
(string compare and header matching in the router) and the 1.37.2 bug fixes
next to it (PRs #920 to #929) touch the request path. This page records where
the model stands against each change, as of 2026-11-06. Every pull request
was still open on that date; check the issue for the current state.

---

## Simulated

| server change | model |
|---|---|
| A header name is at most 255 bytes (`uint8_t name_length`); the modules refuse a longer response name instead of cutting it to `len % 256` (PR #926) | `UnitRequest.serialize()` and `UnitResponse.serialize()` raise `FieldNameTooLong` over `NXT_UNIT_FIELD_NAME_MAX` (255). The worker answers 503 for such a response, as the Python, Perl and Node modules do (PHP answers 500). |
| A pass to a target the application does not have is declined with 404 (PR #925) | `Router.set_app_targets(app_id, count)` (default 1). A `req.app_target` out of range gets 404, never reaches the worker, and the stream completes. |

Before this change the model had the same defect as the modules: ctypes
wraps a `c_uint8` set to 300 to 44 without a word. The router also never
read `req.app_target`.

`tests/test_request_path_limits.py` covers 255, 256 and 300-byte names on
both sides, a missing target next to a known one, the default single target,
and that INV-3 (no SHM leak) and INV-4 (progress) hold on the new paths.

```sh
uv run pytest -q tests/test_request_path_limits.py
```

## Not modelled

| server change | why |
|---|---|
| `Connection` as a token list, `close` wins; `Transfer-Encoding` without case (PR #920) | The listener has no header handling; keep-alive is a flag on `send_http_response()`. |
| Field value scan, field dispatch table, compare helpers (PRs #921, #922, #929) | No HTTP parser and no per-field cost. The callgrind census in #923 is the reference. |
| Route regexes in UTF mode, Unicode host patterns matched as punycode (PR #928) | The router takes `app_id` from the caller; there is no route or host match. |
| `response_headers` and Python `Content-Length` matched by name (PR #927) | The router reads only the response status and passes fields through. |
