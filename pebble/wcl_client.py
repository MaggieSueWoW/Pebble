from __future__ import annotations

import logging
import time
from typing import Optional

import requests
from requests.auth import HTTPBasicAuth
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

_RETRY_STATUS_CODES = {429, 500, 502, 503, 504}


def _is_retryable(exc: Exception) -> bool:
    if isinstance(exc, requests.HTTPError):
        return (
            exc.response is not None and exc.response.status_code in _RETRY_STATUS_CODES
        )
    return isinstance(exc, (requests.ConnectionError, requests.Timeout))


class WCLClient:
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        *,
        base_url: str = "https://www.warcraftlogs.com/api/v2/client",
        token_url: str = "https://www.warcraftlogs.com/oauth/token",
    ):
        self._session = requests.Session()
        self._client_id = client_id
        self._client_secret = client_secret
        self._base_url = base_url
        self._token_url = token_url
        self._token: Optional[str] = None
        self._token_exp: float = 0.0

    @retry(
        reraise=True,
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _ensure_token(self) -> None:
        now = time.time()
        if self._token and now < (self._token_exp - 60):
            return
        r = self._session.post(
            self._token_url,
            data={"grant_type": "client_credentials"},
            auth=HTTPBasicAuth(self._client_id, self._client_secret),
            timeout=30,
        )
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        if not token:
            raise RuntimeError(f"WCL token response missing access_token: {data}")
        expires_in = int(data.get("expires_in", 3600))
        self._token = token
        self._token_exp = now + max(60, expires_in)
        self._session.headers.update({"Authorization": f"Bearer {self._token}"})

    @retry(
        reraise=True,
        retry=retry_if_exception(_is_retryable),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        stop=stop_after_attempt(5),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _post(self, query: str, variables: Optional[dict] = None) -> dict:
        self._ensure_token()
        payload = {"query": query, "variables": variables or {}}
        r = self._session.post(self._base_url, json=payload, timeout=60)
        r.raise_for_status()
        data = r.json()
        if "errors" in data:
            raise RuntimeError(data["errors"])  # surface graph errors
        return data

    def fetch_report_bundle(self, code: str, translate: bool = True) -> dict:
        """Report meta + fights + masterData actors in one call.
        NOTE: fight start/end are relative ms to report.startTime; we normalize in ingest.
        """
        q = """
        query ReportFightsAndActors($code: String!, $translate: Boolean = true) {
          reportData {
            report(code: $code) {
              code
              title
              startTime
              endTime
              zone { name }
              region { id name compactName }
              guild { id name server { name region { id name compactName } } }
              fights { id encounterID name difficulty startTime endTime friendlyPlayers kill }
              masterData(translate: $translate) { actors(type: "Player") { id name server subType type } }
            }
          }
        }
        """
        return self._post(q, {"code": code, "translate": translate})["data"][
            "reportData"
        ]["report"]
