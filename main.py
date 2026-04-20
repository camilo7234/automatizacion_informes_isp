# main.py
"""
Orquestador principal del pipeline semanal de informes ISP.

Flujo de ejecución:
  1. Playwright     → extrae seriales CPE desde inventario Wispro
                      persiste dict {id_contrato: serial} en registro_procesados.json
  2. CsvMerger      → fusiona orders + contratos + clientes
                      detecta solo los registros nuevos de la semana
  3. InformeSemanal → genera Excel oficial con seriales ya disponibles
  4. TicketsMerger  → genera Excel PQRS (solo si hay CSV de tickets)

Frecuencia: cada viernes (día de corte semanal)
"""


# ------------------------------------------------------------------
# ------------------------------------------------------------------
# BLOQUE 1: IMPORTS Y CONFIGURACIÓN
# ------------------------------------------------------------------
import sys
import json
import asyncio
import logging
from pathlib import Path

from playwright.async_api               import async_playwright
from extractores.playwright_extractor   import WisproPlaywrightExtractor
from procesadores.csv_merger            import procesar_csvs
from generadores.informe_semanal        import generar_informe_semanal
from procesadores.tickets_merger        import TicketsMerger

# 🔴 NUEVO IMPORT (FACTURACIÓN)
from generadores.reporte_facturacion_clientes import generar_reporte_facturacion

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent

# ------------------------------------------------------------------
# BLOQUE 2: PASO 1 — EXTRACCIÓN DE SERIALES CPE (PLAYWRIGHT)
# ------------------------------------------------------------------
async def extraer_seriales(extractor: WisproPlaywrightExtractor):
    """
    Ejecuta el scraping del inventario de artículos en Wispro.
    Persiste el mapa {id_contrato: serial} en registro_procesados.json
    ANTES de que CsvMerger cree o modifique ese archivo.
    """
    logger.info("PASO 1 — Iniciando extracción de seriales CPE...")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(
            viewport={"width": 1280, "height": 800},
            locale="es-CO"
        )
        page = await context.new_page()

        try:
            await extractor._login(page)
            mapa = await extractor.extraer_seriales_cpe(page)

            # --------------------------------------------------
            # GUARDAR MAPA EN REGISTRO ANTES DE QUE CSV_MERGER
            # cree el archivo — así la clave seriales_cpe existe
            # cuando informe_semanal.py la busque en PASO 3
            # --------------------------------------------------
            ruta_registro = BASE_DIR / "datos/procesados/modelo_contrato/registro_procesados.json"
            ruta_registro.parent.mkdir(parents=True, exist_ok=True)

            data = {}
            if ruta_registro.exists():
                with open(ruta_registro, "r", encoding="utf-8") as f:
                    data = json.load(f)

            data["seriales_cpe"] = mapa

            with open(ruta_registro, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)

            logger.info(
                f"PASO 1 completado — {len(mapa)} seriales guardados "
                f"en {ruta_registro}"
            )
            return mapa

        except Exception as e:
            logger.error(f"PASO 1 FALLIDO — Error en scraping: {e}")
            screenshot = BASE_DIR / "salidas" / "error_scraping.png"
            await page.screenshot(path=screenshot, full_page=True)
            logger.info(f"Screenshot guardado en: {screenshot}")
            raise

        finally:
            await browser.close()


# ------------------------------------------------------------------
# BLOQUE 3: PASO 2 — FUSIÓN CSV Y DETECCIÓN DE NUEVOS
# ------------------------------------------------------------------
def fusionar_csvs():
    """
    Carga los 3 CSVs exportados de Wispro:
      - orders     (fuente de verdad → instalaciones exitosas)
      - contratos
      - clientes
    Detecta solo los registros nuevos de la semana.
    Retorna lista de dicts lista para el generador del informe.
    """
    logger.info("PASO 2 — Fusionando CSVs y detectando registros nuevos...")

    registros = procesar_csvs()

    if not registros:
        logger.warning("PASO 2 — No hay registros nuevos esta semana.")
    else:
        logger.info(f"PASO 2 completado — {len(registros)} registros nuevos detectados")

    return registros


# ------------------------------------------------------------------
# BLOQUE 4: PASO 3 — GENERACIÓN DEL EXCEL SEMANAL
# ------------------------------------------------------------------
def generar_excel(registros: list):
    """
    Genera el Excel semanal oficial con los registros nuevos.
    Los seriales CPE ya están disponibles en registro_procesados.json
    gracias al PASO 1 (Playwright).
    Retorna None si no hay registros nuevos.
    """
    logger.info("PASO 3 — Generando Excel semanal...")

    if not registros:
        logger.warning("PASO 3 — Sin registros nuevos, no se genera Excel.")
        return None

    ruta = generar_informe_semanal(registros)

    logger.info(f"PASO 3 completado — Excel generado en: {ruta}")
    return ruta


# ------------------------------------------------------------------
# BLOQUE 5: PASO 4 — GENERACIÓN DEL EXCEL PQRS (CONDICIONAL)
# ------------------------------------------------------------------
def generar_pqrs():
    """
    Genera el Excel contractual de PQRS solo si hay CSV de tickets.
    Si no existe el CSV o está vacío, retorna None sin error.
    """
    logger.info("PASO 4 — Verificando tickets PQRS...")

    ruta = TicketsMerger().generar()

    if ruta:
        logger.info(f"PASO 4 completado — PQRS generado en: {ruta}")
    else:
        logger.info("PASO 4 — Sin tickets esta semana, no se genera PQRS.")

    return ruta


# ------------------------------------------------------------------
# BLOQUE 6: ORQUESTADOR PRINCIPAL
# ------------------------------------------------------------------
def main():
    """
    Ejecuta el pipeline completo en el orden correcto:
      1. Playwright  → seriales CPE
      2. CsvMerger   → registros nuevos
      3. Excel       → informe semanal
      4. PQRS        → Excel contractual
      5. Facturación → nuevo reporte financiero por cliente
    """
    print("=" * 50)
    print("  PIPELINE SEMANAL ISP — INICIO")
    print("=" * 50)

    try:
        # --------------------------------------------------
        # PASO 1: SERIALES CPE (PLAYWRIGHT)
        # --------------------------------------------------
        extractor = WisproPlaywrightExtractor()
        mapa = asyncio.run(extraer_seriales(extractor))

        if not mapa:
            logger.warning(
                "No se obtuvieron seriales CPE. "
                "El Excel se generará con PENDIENTE en esa columna."
            )

        # --------------------------------------------------
        # PASO 2: FUSIÓN CSV
        # --------------------------------------------------
        registros = fusionar_csvs()

        # --------------------------------------------------
        # PASO 3: EXCEL SEMANAL
        # --------------------------------------------------
        ruta_excel = generar_excel(registros)

        # --------------------------------------------------
        # PASO 4: PQRS
        # --------------------------------------------------
        ruta_pqrs = generar_pqrs()

        # --------------------------------------------------
        # 🔴 PASO 5: REPORTE DE FACTURACIÓN (NUEVO)
        # --------------------------------------------------
        logger.info("PASO 5 — Generando reporte de facturación por clientes...")
        generar_reporte_facturacion()
        logger.info("PASO 5 completado — Reporte de facturación generado")

        # --------------------------------------------------
        # RESUMEN FINAL
        # --------------------------------------------------
        print("=" * 50)
        print("  PIPELINE SEMANAL ISP — COMPLETADO")
        print(f"  Instalaciones nuevas: {len(registros) if registros else 0}")
        print(f"  Informe semanal:      {ruta_excel or 'No generado'}")
        print(f"  PQRS:                 {ruta_pqrs  or 'No generado'}")
        print(f"  Facturación:          salidas/informes_facturacion/")
        print("=" * 50)

    except Exception as e:
        logger.error(f"ERROR CRÍTICO EN EL PIPELINE: {e}")
        print("=" * 50)
        print("  PIPELINE FALLIDO — revisa el log y el screenshot")
        print(f"  Error: {e}")
        print("=" * 50)
        sys.exit(1)


if __name__ == "__main__":
    main()
