import logging
import threading
import time
from typing import Any

from mgi.cliente_base import ClienteBase


class RateLimiter:
    """Garante intervalo mínimo entre requisições. Thread-safe."""

    def __init__(self, interval: float) -> None:
        self._interval = interval
        self._lock = threading.Lock()
        self._last = 0.0

    def wait(self) -> None:
        with self._lock:
            elapsed = time.time() - self._last
            if elapsed < self._interval:
                time.sleep(self._interval - elapsed)
            self._last = time.time()


class ClienteComprasGov(ClienteBase):
    BASE_URL = "https://dadosabertos.compras.gov.br"
    PAGE_SIZE = 500
    PAGE_DELAY = 1.0

    def __init__(self) -> None:
        super().__init__(base_url=self.BASE_URL)

    def iter_pages(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        rate_limiter: "RateLimiter | None" = None,
    ):
        """Gerador que yielda cada página individualmente, mantendo apenas
        uma página na memória por vez. Ideal para endpoints com muitos registros."""
        params = params or {}
        pagina = 1
        total_paginas: int | None = None

        while True:
            if rate_limiter is not None:
                rate_limiter.wait()
            else:
                time.sleep(self.PAGE_DELAY)
            page_params = {**params, "pagina": pagina, "tamanhoPagina": self.PAGE_SIZE}
            try:
                _, resp = self.request("GET", path, params=page_params)
            except Exception as exc:
                logging.error(
                    "[compras_gov] Falha na página %s de %s: %s", pagina, path, exc
                )
                if total_paginas is None or pagina >= total_paginas:
                    return
                pagina += 1
                continue

            if not isinstance(resp, dict):
                logging.warning(
                    "[compras_gov] Resposta inválida em %s p.%s: %r", path, pagina, resp
                )
                return

            data = [r if r is not None else {} for r in resp.get("resultado", [])]
            total_registros = resp.get("totalRegistros", 0)
            total_paginas = resp.get("totalPaginas", pagina)

            logging.info(
                "[compras_gov] %s | p.%s/%s | +%s | api_total=%s",
                path, pagina, total_paginas, len(data), total_registros,
            )

            yield data, total_registros

            if resp.get("paginasRestantes", 0) == 0:
                return
            pagina += 1

    def fetch_all_pages(
        self, path: str, params: dict[str, Any] | None = None
    ) -> tuple[list[dict], int]:
        """Pagina automaticamente até paginasRestantes == 0.

        Em caso de falha em uma página individual, registra o erro e
        tenta continuar com a próxima (conforme req. 4.3).

        Returns:
            (registros, total_registros_api)
        """
        params = params or {}
        pagina = 1
        all_data: list[dict] = []
        total_registros = 0
        total_paginas: int | None = None

        while True:
            page_params = {**params, "pagina": pagina, "tamanhoPagina": self.PAGE_SIZE}
            try:
                _, resp = self.request("GET", path, params=page_params)
            except Exception as exc:
                logging.error(
                    "[compras_gov] Falha na página %s de %s: %s", pagina, path, exc
                )
                if total_paginas is None or pagina >= total_paginas:
                    break
                pagina += 1
                continue

            if not isinstance(resp, dict):
                logging.warning(
                    "[compras_gov] Resposta inválida em %s p.%s: %r", path, pagina, resp
                )
                break

            data = [r if r is not None else {} for r in resp.get("resultado", [])]
            all_data.extend(data)
            total_registros = resp.get("totalRegistros", len(all_data))
            total_paginas = resp.get("totalPaginas", pagina)

            logging.info(
                "[compras_gov] %s | p.%s/%s | +%s | acum=%s | api_total=%s",
                path,
                pagina,
                total_paginas,
                len(data),
                len(all_data),
                total_registros,
            )

            if resp.get("paginasRestantes", 0) == 0:
                break
            time.sleep(self.PAGE_DELAY)
            pagina += 1

        if len(all_data) != total_registros:
            logging.warning(
                "[compras_gov] Divergência em %s: ingeridos=%s api_total=%s",
                path,
                len(all_data),
                total_registros,
            )

        return all_data, total_registros

    # ── Módulo Material ──────────────────────────────────────────────────────

    def consultar_grupo_material(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-material/1_consultarGrupoMaterial",
        )

    def consultar_classe_material(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-material/2_consultarClasseMaterial",
        )

    def consultar_pdm_material(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-material/3_consultarPdmMaterial",
        )

    def consultar_item_material(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-material/4_consultarItemMaterial",
        )

    def consultar_natureza_despesa_material(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-material/5_consultarMaterialNaturezaDespesa",
        )

    def consultar_unidade_fornecimento_material(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-material/6_consultarMaterialUnidadeFornecimento",
        )

    def consultar_caracteristicas_material(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-material/7_consultarMaterialCaracteristicas",
            {},
        )

    # ── Módulo Serviço ───────────────────────────────────────────────────────

    def consultar_secao_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/1_consultarSecaoServico",
        )

    def consultar_divisao_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/2_consultarDivisaoServico",
        )

    def consultar_grupo_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/3_consultarGrupoServico",
        )

    def consultar_classe_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/4_consultarClasseServico",
            {},
        )

    def consultar_subclasse_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/5_consultarSubClasseServico",
        )

    def consultar_item_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/6_consultarItemServico",
        )

    def consultar_und_medida_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/7_consultarUndMedidaServico",
        )

    def consultar_natureza_despesa_servico(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-servico/8_consultarNaturezaDespesaServico",
        )

    # ── Módulo UASG ─────────────────────────────────────────────────────────

    def consultar_uasg(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-uasg/1_consultarUasg",
            {"statusUasg": "true"},
        )

    def consultar_orgao(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-uasg/2_consultarOrgao",
            {"statusOrgao": "true"},
        )

    # ── Módulo Contratações ──────────────────────────────────────────────────

    def consultar_contratacoes(
        self,
        data_inicial: str,
        data_final: str,
        codigo_modalidade: int,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-contratacoes/1_consultarContratacoes_PNCP_14133",
            {
                "dataPublicacaoPncpInicial": data_inicial,
                "dataPublicacaoPncpFinal": data_final,
                "codigoModalidade": codigo_modalidade,
            },
        )

    def consultar_itens_contratacoes(
        self,
        data_inicial: str,
        data_final: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-contratacoes/2_consultarItensContratacoes_PNCP_14133",
            {
                "dataInclusaoPncpInicial": data_inicial,
                "dataInclusaoPncpFinal": data_final,
            },
        )

    def consultar_resultado_itens_contratacoes(
        self,
        data_inicial: str,
        data_final: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-contratacoes/3_consultarResultadoItensContratacoes_PNCP_14133",
            {
                "dataResultadoPncpInicial": data_inicial,
                "dataResultadoPncpFinal": data_final,
                "bps": "false",
            },
        )

    # ── Módulo Contratos ─────────────────────────────────────────────────────

    def consultar_contratos(
        self,
        codigo_orgao: str,
        data_inicial: str,
        data_final: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-contratos/1_consultarContratos",
            {
                "codigoOrgao": codigo_orgao,
                "dataVigenciaInicialMin": data_inicial,
                "dataVigenciaInicialMax": data_final,
            },
        )

    def consultar_contratos_item(
        self,
        codigo_orgao: str,
        data_inicial: str,
        data_final: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-contratos/2_consultarContratosItem",
            {
                "codigoOrgao": codigo_orgao,
                "dataVigenciaInicialMin": data_inicial,
                "dataVigenciaInicialMax": data_final,
            },
        )

    # ── Módulo ARP ───────────────────────────────────────────────────────────

    def consultar_arp(
        self,
        data_inicial: str,
        data_final: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-arp/1_consultarARP",
            {
                "dataVigenciaInicialMin": data_inicial,
                "dataVigenciaInicialMax": data_final,
            },
        )

    def consultar_arp_item(
        self,
        data_inicial: str,
        data_final: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-arp/2_consultarARPItem",
            {
                "dataVigenciaInicialMin": data_inicial,
                "dataVigenciaInicialMax": data_final,
            },
        )

    def consultar_arp_unidades_item(
        self,
        numero_ata: str,
        unidade_gerenciadora: str,
        numero_item: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-arp/3_consultarUnidadesItem",
            {
                "numeroAta": numero_ata,
                "unidadeGerenciadora": unidade_gerenciadora,
                "numeroItem": numero_item,
            },
        )

    def consultar_arp_empenhos_saldo(
        self,
        numero_ata: str,
        unidade_gerenciadora: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-arp/4_consultarEmpenhosSaldoItem",
            {
                "numeroAta": numero_ata,
                "unidadeGerenciadora": unidade_gerenciadora,
            },
        )

    def consultar_arp_adesoes_item(
        self,
        numero_ata: str,
        unidade_gerenciadora: str,
        numero_item: str,
    ) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-arp/5_consultarAdesoesItem",
            {
                "numeroAta": numero_ata,
                "unidadeGerenciadora": unidade_gerenciadora,
                "numeroItem": numero_item,
            },
        )

    # ── Módulo Fornecedor ────────────────────────────────────────────────────

    def consultar_fornecedor(self) -> tuple[list[dict], int]:
        return self.fetch_all_pages(
            "/modulo-fornecedor/1_consultarFornecedor",
            {"ativo": "true"},
        )
