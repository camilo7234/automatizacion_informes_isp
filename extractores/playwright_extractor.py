# extractores/playwright_extractor.py
"""
Módulo para extraer datos de Wispro mediante Playwright.
Responsabilidades: login, seriales CPE, órdenes, tickets, facturas.
NOTA: Clientes y contratos se obtienen desde CSV → procesadores/csv_merger.py
"""

import asyncio
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path

import yaml
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class WisproPlaywrightExtractor:
    """
    Extracción automatizada desde Wispro vía Playwright.
    Clientes y contratos → csv_merger.py (no responsabilidad de este módulo).
    """

    def __init__(self, config_path="config/entorno.yaml"):
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
        self.wispro_config = config["wispro"]
        self.extraccion_config = config["extraccion"]
        self.output_path = Path(self.extraccion_config["archivo_salida"])
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self.timeout = self.extraccion_config.get("tiempo_espera_segundos", 30) * 1000

    # ------------------------------------------------------------------
    # BLOQUE 1: INICIO DE SESIÓN (COMPLETO Y PROBADO)
    # ------------------------------------------------------------------
    async def _login(self, page):
        """
        Navega a la página de login, completa credenciales y espera
        redirección al dashboard. Lanza excepción si falla el login.
        """
        logger.info("Navegando a la página de login...")
        try:
            await page.goto(
                self.wispro_config["login_url"],
                wait_until="networkidle",
                timeout=self.timeout
            )
        except PlaywrightTimeoutError:
            raise Exception(
                f"Tiempo de espera agotado al cargar {self.wispro_config['login_url']}"
            )

        await page.fill('input[name="user[email]"]', self.wispro_config["usuario"])
        await page.fill('input[name="user[password]"]', self.wispro_config["password"])
        await page.click('input[type="submit"][value="Ingresar"]')

        dashboard_url = "https://www.cloud.wispro.co/stats/dashboard?locale=es"
        try:
            await page.wait_for_url(dashboard_url, timeout=self.timeout)
            logger.info(f"Login exitoso, redirigido a {dashboard_url}")
        except PlaywrightTimeoutError:
            error = await page.text_content('.flash-error, .alert-danger')
            if error:
                raise Exception(f"Error de login: {error}")
            else:
                raise Exception("No se pudo verificar el login: tiempo de espera agotado.")

    # ------------------------------------------------------------------
    # BLOQUE 2: EXTRACCIÓN DE TABLAS GENÉRICA (REUTILIZABLE)
    # ------------------------------------------------------------------
    async def _extraer_tabla(
        self,
        page,
        url,
        selector_filas="table tbody tr",
        mapeo_columnas=None,
        esperar_tabla=True
    ):
        """
        Navega a una URL, espera que cargue la tabla y extrae los datos.
        - url: URL de la página.
        - selector_filas: selector CSS para las filas de la tabla.
        - mapeo_columnas: dict {campo: índice_columna} (empezando en 0).
        - esperar_tabla: si True, espera a que aparezca el selector.
        Retorna lista de diccionarios con los datos extraídos.
        """
        logger.info(f"Navegando a {url}")
        await page.goto(url, wait_until="networkidle", timeout=self.timeout)

        if esperar_tabla:
            try:
                await page.wait_for_selector(selector_filas, timeout=self.timeout)
            except PlaywrightTimeoutError:
                logger.warning(
                    f"No se encontraron filas con selector '{selector_filas}' en {url}"
                )
                return []

        filas = await page.query_selector_all(selector_filas)
        datos = []

        if mapeo_columnas is None:
            for fila in filas:
                celdas = await fila.query_selector_all('td')
                fila_texto = [await celda.inner_text() for celda in celdas]
                datos.append(fila_texto)
        else:
            for fila in filas:
                celdas = await fila.query_selector_all('td')
                if len(celdas) < max(mapeo_columnas.values()) + 1:
                    continue
                item = {}
                for campo, idx in mapeo_columnas.items():
                    texto = await celdas[idx].inner_text()
                    item[campo] = texto.strip()
                datos.append(item)

        logger.info(f"Extraídas {len(datos)} filas desde {url}")
        return datos

    # ------------------------------------------------------------------
    # BLOQUE 3: RESERVADO — DETALLE DE CLIENTE (ESTRATO)
    # Suspendido: Wispro confirmó que añadirá ESTRATO SOCIAL al CSV.
    # Reactivar solo si el CSV sigue sin incluir ese campo.
    # ------------------------------------------------------------------
    # async def extraer_detalle_cliente(self, page, client_uuid):
    #     """
    #     Navega a /clients/{uuid} y extrae estrato u otra info adicional.
    #     Con 3.000 clientes implica 3.000 requests — solo activar si es necesario.
    #     """
    #     pass

    # ------------------------------------------------------------------
    # BLOQUE 4: DETALLE DE CONTRATO (SUSPENSIONES)
    # ------------------------------------------------------------------
    async def extraer_detalle_contrato(self, page, uuid):
        """
        Extrae información detallada desde /contracts/{uuid}.
        Útil para obtener motivo de suspensión y fecha de finalización,
        datos que no están disponibles en el CSV exportado.
        """
        url = f"{self.wispro_config['url_base']}/contracts/{uuid}?locale=es"
        logger.info(f"Detalle contrato: {url}")

        await page.goto(url, wait_until="networkidle", timeout=self.timeout)

        detalle = {}

        try:
            fecha_fin_elem = await page.query_selector(
                'li:has-text("Fecha de finalización") span.pull-right'
            )
            detalle["fecha_finalizacion"] = (
                (await fecha_fin_elem.inner_text()).strip()
                if fecha_fin_elem else ""
            )
        except Exception:
            detalle["fecha_finalizacion"] = ""

        try:
            motivo_elem = await page.query_selector(
                'li:has-text("Motivo") span.pull-right'
            )
            detalle["motivo_suspension"] = (
                (await motivo_elem.inner_text()).strip()
                if motivo_elem else ""
            )
        except Exception:
            detalle["motivo_suspension"] = ""

        return detalle

    # ------------------------------------------------------------------
    # BLOQUE 5: EXTRACCIÓN DE ÓRDENES DE INSTALACIÓN
    # ------------------------------------------------------------------
    async def extraer_ordenes_instalacion(self, page, mes=None, anio=None):
        """
        Extrae órdenes de instalación completadas.
        Si se proporcionan mes y año, filtra por ese periodo.
        """
        if mes and anio:
            url = (
                f"{self.wispro_config['url_base']}/order/orders"
                f"?kind=installation&status=completed"
                f"&created_at_from={anio}-{mes:02d}-01"
                f"&created_at_to={anio}-{mes:02d}-31&locale=es"
            )
        else:
            url = (
                f"{self.wispro_config['url_base']}/order/orders"
                f"?kind=installation&status=completed&locale=es"
            )

        mapeo = {
            "id": 0,
            "cliente": 1,
            "fecha_creacion": 2,
            "fecha_completada": 3,
            "tecnico": 4,
            "serial_cpe": 5
        }
        return await self._extraer_tabla(
            page, url, selector_filas="table tbody tr", mapeo_columnas=mapeo
        )

    # ------------------------------------------------------------------
    # BLOQUE 6: EXTRACCIÓN DE TICKETS (PQRS)
    # ------------------------------------------------------------------
    async def extraer_tickets(self, page, mes=None, anio=None):
        """
        Extrae tickets de la mesa de ayuda (PQRS).
        """
        if mes and anio:
            url = (
                f"{self.wispro_config['url_base']}/help_desk/issues"
                f"?created_at_from={anio}-{mes:02d}-01"
                f"&created_at_to={anio}-{mes:02d}-31&locale=es"
            )
        else:
            url = f"{self.wispro_config['url_base']}/help_desk/issues?locale=es"

        mapeo = {
            "id": 0,
            "cliente": 1,
            "tipo": 2,
            "asunto": 3,
            "fecha_creacion": 4,
            "estado": 5,
            "prioridad": 6
        }
        return await self._extraer_tabla(
            page, url, selector_filas="table tbody tr", mapeo_columnas=mapeo
        )

    # ------------------------------------------------------------------
    # BLOQUE 7: EXTRACCIÓN DE FACTURAS EMITIDAS
    # ------------------------------------------------------------------
    async def extraer_facturas(self, page, mes=None, anio=None):
        """
        Extrae facturas emitidas del periodo.
        """
        if mes and anio:
            url = (
                f"{self.wispro_config['url_base']}/invoicing/invoices/issued"
                f"?from_date={anio}-{mes:02d}-01"
                f"&to_date={anio}-{mes:02d}-31&locale=es"
            )
        else:
            url = (
                f"{self.wispro_config['url_base']}/invoicing/invoices/issued?locale=es"
            )

        mapeo = {
            "id": 0,
            "cliente": 1,
            "fecha_emision": 2,
            "valor": 3,
            "estado_pago": 4
        }
        return await self._extraer_tabla(
            page, url, selector_filas="table tbody tr", mapeo_columnas=mapeo
        )

    # ------------------------------------------------------------------
    # BLOQUE 8: EXTRACCIÓN DE SERIALES CPE — INVENTARIO OCUPADOS (DEBUG)
    # URL: /inventory/articles?q[state_eq_any][]=engaged
    # Columnas reales:
    #   0 = checkbox
    #   1 = #
    #   2 = Código de producto
    #   3 = Número de serie
    #   4 = MAC address
    #   5 = Marca
    #   6 = Modelo
    #   7 = Ubicación
    #   8 = Estado
    # Retorna dict {id_contrato: serial} listo para informe_semanal.py
    # ------------------------------------------------------------------
    async def extraer_seriales_cpe(self, page) -> dict:
        """
        Extrae números de serie de todos los CPE con estado OCUPADO.
        Parsea la columna Ubicación para obtener el ID de contrato.
        Retorna dict {id_contrato (str): serial (str)}.
        Maneja paginación completa automáticamente.
        """
        import re

        url_base = (
            f"{self.wispro_config['url_base']}/inventory/articles"
            f"?locale=es&per=50&q%5Bstate_eq_any%5D%5B%5D=engaged"
        )

        mapa_seriales = {}
        pagina = 1

        while True:
            url = f"{url_base}&page={pagina}"
            logger.info(f"Extrayendo seriales CPE — página {pagina}: {url}")

            await page.goto(url, wait_until="networkidle", timeout=self.timeout)

            # DEBUG: URL real cargada por Playwright
            print("\n================ URL DEBUG ================")
            print("URL ACTUAL:", page.url)
            print("===========================================\n")

            # Esperar tabla y dar margen extra al renderizado
            try:
                await page.wait_for_selector("table tbody tr", timeout=self.timeout)
                await page.wait_for_timeout(2000)
            except PlaywrightTimeoutError:
                logger.info("No hay más filas — paginación completada.")
                break

            filas = await page.query_selector_all("table tbody tr")

            if not filas:
                print("⚠️ DEBUG: No se encontraron filas en la tabla")
                break

            # DEBUG: mostrar primeras filas para validar índices
            print("\n=========== DEBUG TABLA ===========")
            print(f"Total filas detectadas: {len(filas)}")
            for fila in filas[:3]:
                celdas = await fila.query_selector_all("td")
                valores = [await c.inner_text() for c in celdas]
                print("Fila:", valores)
                print("Cantidad columnas:", len(celdas))
                print("----------------------------------")
            print("==================================\n")

            seriales_pagina = 0

            for fila in filas:
                try:
                    celdas = await fila.query_selector_all("td")

                    # Mínimo 9 columnas para ser fila válida
                    if len(celdas) < 9:
                        print(f"⚠️ Fila descartada por columnas insuficientes: {len(celdas)}")
                        continue

                    # Columnas reales según el HTML actual
                    serial = (await celdas[3].inner_text()).strip()
                    ubicacion = (await celdas[7].inner_text()).strip()

                    # DEBUG: valores clave
                    print(f"DEBUG serial bruto: '{serial}'")
                    print(f"DEBUG ubicación: '{ubicacion}'")

                    if not serial or serial in ("", "nan"):
                        print("⚠️ Serial vacío → descartado")
                        continue

                    match = re.search(r"Contrato:\s*(\d+)", ubicacion, re.IGNORECASE)
                    if not match:
                        logger.debug(
                            f"Serial '{serial}' sin contrato en ubicación: '{ubicacion}'"
                        )
                        continue

                    id_contrato = match.group(1).strip()
                    mapa_seriales[id_contrato] = serial
                    seriales_pagina += 1

                except Exception as e:
                    logger.warning(f"Error procesando fila de inventario: {e}")
                    continue

            logger.info(f"Página {pagina}: {seriales_pagina} seriales mapeados")

            # PAGINACIÓN: terminar si la página trajo menos de 50
            # o si no hay enlace Siguiente
            if len(filas) < 50:
                logger.info("Última página alcanzada (menos de 50 filas).")
                break

            siguiente = await page.query_selector(
                'a[rel="next"], .pagination a:has-text("Siguiente")'
            )
            if siguiente:
                clase = await siguiente.get_attribute("class") or ""
                if "disabled" in clase:
                    break
                pagina += 1
            else:
                break

        logger.info(
            f"Mapa seriales CPE construido: {len(mapa_seriales)} entradas "
            f"({pagina} página(s) procesada(s))"
        )
        return mapa_seriales
    
    
    # ------------------------------------------------------------------
    # BLOQUE 9: ORQUESTADOR PRINCIPAL
    # ------------------------------------------------------------------
    async def ejecutar(self, mes=None, anio=None):
        """
        Orquesta la extracción completa vía Playwright:
          - Login
          - Seriales CPE (inventario ocupados) → guarda en registro_procesados.json
          - Órdenes de instalación
          - Tickets (PQRS)
          - Facturas
        Guarda resultado en datos/entrada/wispro/wispro_datos.json
        NOTA: Clientes y contratos NO se extraen aquí → csv_merger.py
        """
        if mes is None or anio is None:
            hoy = datetime.now()
            if hoy.day <= 5:
                fecha = hoy.replace(day=1) - timedelta(days=1)
                mes   = fecha.month
                anio  = fecha.year
            else:
                mes  = hoy.month
                anio = hoy.year

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=False)
            context = await browser.new_context(
                viewport={"width": 1280, "height": 800},
                locale="es-CO"
            )
            page = await context.new_page()

            try:
                await self._login(page)

                # --------------------------------------------------
                # SERIALES CPE → dict {id_contrato: serial}
                # --------------------------------------------------
                seriales_cpe = await self.extraer_seriales_cpe(page)
                logger.info(f"Seriales CPE extraídos: {len(seriales_cpe)} entradas")

                # --------------------------------------------------
                # PERSISTIR MAPA EN registro_procesados.json
                # para que informe_semanal.py lo consuma via BLOQUE 3
                # --------------------------------------------------
                ruta_registro = Path(
                    "datos/procesados/modelo_contrato/registro_procesados.json"
                )
                if ruta_registro.exists():
                    with open(ruta_registro, "r", encoding="utf-8") as f:
                        data_reg = json.load(f)
                else:
                    data_reg = {}

                data_reg["seriales_cpe"]          = seriales_cpe
                data_reg["ultima_actualizacion"]  = datetime.now().isoformat()

                with open(ruta_registro, "w", encoding="utf-8") as f:
                    json.dump(data_reg, f, indent=2, ensure_ascii=False)

                logger.info(
                    f"Mapa seriales persistido en {ruta_registro} "
                    f"({len(seriales_cpe)} entradas)"
                )

                # --------------------------------------------------
                # ÓRDENES DE INSTALACIÓN
                # --------------------------------------------------
                ordenes = await self.extraer_ordenes_instalacion(page, mes, anio)
                logger.info(f"Órdenes de instalación extraídas: {len(ordenes)}")

                # --------------------------------------------------
                # TICKETS (PQRS)
                # --------------------------------------------------
                tickets = await self.extraer_tickets(page, mes, anio)
                logger.info(f"Tickets extraídos: {len(tickets)}")

                # --------------------------------------------------
                # FACTURAS
                # --------------------------------------------------
                facturas = await self.extraer_facturas(page, mes, anio)
                logger.info(f"Facturas extraídas: {len(facturas)}")

                # --------------------------------------------------
                # CONSTRUIR Y GUARDAR OBJETO FINAL
                # --------------------------------------------------
                datos_wispro = {
                    "seriales_cpe":         seriales_cpe,
                    "ordenes_instalacion":  ordenes,
                    "tickets":              tickets,
                    "facturas":             facturas,
                    "fecha_extraccion":     datetime.now().isoformat(),
                    "periodo": {
                        "mes":  mes,
                        "anio": anio
                    }
                }

                with open(self.output_path, "w", encoding="utf-8") as f:
                    json.dump(datos_wispro, f, indent=2, ensure_ascii=False)

                logger.info(
                    f"Extracción completada. Archivo guardado en {self.output_path}"
                )

            except Exception as e:
                logger.error(f"Error durante la extracción: {e}")
                screenshot_path = self.output_path.with_suffix(".png")
                await page.screenshot(path=screenshot_path, full_page=True)
                logger.info(f"Screenshot guardado en {screenshot_path}")
                raise

            finally:
                await browser.close()


# ------------------------------------------------------------------
# FUNCIÓN DE ENTRADA — llamar desde main.py
# ------------------------------------------------------------------
def extraer_datos(mes=None, anio=None):
    """
    Función síncrona para ejecutar la extracción Playwright.
    Puede llamarse desde main.py o directamente desde terminal.
    """
    extractor = WisproPlaywrightExtractor()
    asyncio.run(extractor.ejecutar(mes, anio))


if __name__ == "__main__":
    extraer_datos()
