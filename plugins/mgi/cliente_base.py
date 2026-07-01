import logging
import time
from http import HTTPStatus
from typing import Any, Optional, Tuple

import httpx


class ClienteBase:
    DEFAULT_TIMEOUT = httpx.Timeout(connect=10, read=300, write=10, pool=10)
    DEFAULT_MAX_RETRIES = 3
    DEFAULT_SLEEP_SECONDS = 2

    def __init__(self, base_url: str, headers: Optional[dict] = None) -> None:
        self.base_url = base_url
        self.client = httpx.Client(base_url=base_url, headers=headers)
        logging.info(
            f"[cliente_base.py] Initialized ClienteBase with base_url: {base_url}"
        )

    def request(
        self, method: str, path: str, **kwargs: Any
    ) -> Tuple[HTTPStatus, Optional[dict | list]]:
        """
        Faz uma requisição HTTP em até DEFAULT_MAX_RETRIES+1 tentativas.

        Args:
            method (str): HTTP Method.
            path (str): URL path.

        Returns:
            Tuple[HTTPStatus, dict]: status e resposta da requisição HTTP.
        """
        kwargs["timeout"] = kwargs.get("timeout", self.DEFAULT_TIMEOUT)
        response = None

        for attempt in range(self.DEFAULT_MAX_RETRIES):
            try:
                logging.info(
                    f"[cliente_base.py] Attempt {attempt + 1} for {method} "
                    f"{self.base_url}{path} with kwargs: {kwargs}"
                )
                response = self.client.request(method, path, **kwargs)
                response.raise_for_status()
                logging.info(
                    f"[cliente_base.py] Request successful with status "
                    f"{response.status_code}"
                )
                return HTTPStatus(response.status_code), response.json()
            except httpx.HTTPStatusError as e:
                status = e.response.status_code
                is_last_attempt = attempt == self.DEFAULT_MAX_RETRIES - 1
                logging.warning(
                    f"[cliente_base.py] HTTP {status} on attempt {attempt + 1} "
                    f"for {method} {path}"
                )
                if is_last_attempt:
                    raise Exception(
                        f"API failed with HTTP {status} after "
                        f"{self.DEFAULT_MAX_RETRIES} attempts"
                    ) from e
                # 429 Too Many Requests: espera mais tempo antes de tentar novamente
                if status == 429:
                    wait = 45 * (attempt + 1)
                    logging.warning(f"[cliente_base.py] Rate limit atingido — aguardando {wait}s")
                    time.sleep(wait)
                else:
                    time.sleep(attempt**2 * self.DEFAULT_SLEEP_SECONDS)
            except httpx.HTTPError as e:
                is_last_attempt = attempt == self.DEFAULT_MAX_RETRIES - 1
                logging.warning(
                    f"[cliente_base.py] Request error on attempt {attempt + 1}: {e}"
                )
                if is_last_attempt:
                    raise Exception(
                        f"API failed after {self.DEFAULT_MAX_RETRIES} attempts"
                    ) from e
                time.sleep(attempt**2 * self.DEFAULT_SLEEP_SECONDS)

        return HTTPStatus.INTERNAL_SERVER_ERROR, None
