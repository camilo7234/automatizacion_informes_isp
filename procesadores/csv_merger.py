# procesadores/csv_merger.py
"""
Fusiona clientes + contratos desde CSVs exportados de Wispro.
Detecta registros NUEVOS comparando contra el registro histórico.
Genera el modelo de datos listo para el adaptador y el informe.
"""

import pandas as pd
import json
import logging
from pathlib import Path
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CsvMerger:
    """
    Responsabilidades:
    1. Leer y fusionar clientes + contratos por ID CLIENTE
    2. Detectar registros nuevos (no procesados en semanas anteriores)
    3. Actualizar el registro histórico de procesados
    4. Devolver lista limpia lista para el pipeline
    """

    # ------------------------------------------------------------------
    # BLOQUE 1: INICIALIZACIÓN Y RUTAS
    # ------------------------------------------------------------------
    def __init__(self, config_path="config/entorno.yaml"):
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        self.ruta_entrada = Path("datos/entrada/wispro")
        self.ruta_procesados = Path("datos/procesados/modelo_contrato")
        self.ruta_registro = self.ruta_procesados / "registro_procesados.json"

        # Crear carpetas si no existen
        self.ruta_procesados.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # BLOQUE 2: CARGA DE ARCHIVOS CSV
    # ------------------------------------------------------------------
    def _cargar_csvs(self, archivo_clientes: str, archivo_contratos: str, archivo_orders: str):
        """
        Carga los tres CSVs exportados desde Wispro.
        Soporta separador ; o , detectándolo automáticamente.
        """
        ruta_cli = self.ruta_entrada / archivo_clientes
        ruta_con = self.ruta_entrada / archivo_contratos
        ruta_ord = self.ruta_entrada / archivo_orders

        if not ruta_cli.exists():
            raise FileNotFoundError(f"No encontrado: {ruta_cli}")
        if not ruta_con.exists():
            raise FileNotFoundError(f"No encontrado: {ruta_con}")
        if not ruta_ord.exists():
            raise FileNotFoundError(f"No encontrado: {ruta_ord}")

        df_clientes  = pd.read_csv(ruta_cli, sep=None, engine="python", encoding="utf-8")
        df_contratos = pd.read_csv(ruta_con, sep=None, engine="python", encoding="utf-8")
        df_orders    = pd.read_csv(ruta_ord, sep=None, engine="python", encoding="utf-8")

        logger.info(f"Clientes cargados:  {len(df_clientes)} registros")
        logger.info(f"Contratos cargados: {len(df_contratos)} registros")
        logger.info(f"Orders cargadas:    {len(df_orders)} registros")

        return df_clientes, df_contratos, df_orders


    # ------------------------------------------------------------------
    # BLOQUE 3: NORMALIZACIÓN Y LIMPIEZA
    # ------------------------------------------------------------------
    def _normalizar(self, df_clientes: pd.DataFrame, df_contratos: pd.DataFrame, df_orders: pd.DataFrame):
        """
        Normaliza columnas, limpia espacios y unifica tipos
        para garantizar los JOINs correctos entre los tres DataFrames.
        """
        for df in [df_clientes, df_contratos, df_orders]:
            df.columns = df.columns.str.strip()

        # --- CONTRATOS ---
        df_contratos = df_contratos.rename(columns={
            "IDENTIFICADOR NACIONAL": "DOCUMENTO/CÉDULA"
        })
        df_contratos["ID CLIENTE"]       = df_contratos["ID CLIENTE"].astype(str).str.strip()
        df_contratos["ID CONTRATO"]      = df_contratos["ID CONTRATO"].astype(str).str.strip()
        df_contratos["DOCUMENTO/CÉDULA"] = df_contratos["DOCUMENTO/CÉDULA"].astype(str).str.strip()

        # --- CLIENTES ---
        df_clientes["ID CLIENTE"]        = df_clientes["ID CLIENTE"].astype(str).str.strip()
        df_clientes["DOCUMENTO/CÉDULA"]  = df_clientes["DOCUMENTO/CÉDULA"].astype(str).str.strip()

        # --- ORDERS ---
        df_orders["ID CLIENTE"]   = df_orders["ID CLIENTE"].astype(str).str.strip()
        df_orders["ID CONTRATO"]  = df_orders["ID CONTRATO"].astype(str).str.strip()
        df_orders["DOCUMENTO O CÉDULA DE IDENTIDAD CLIENTE"] = (
            df_orders["DOCUMENTO O CÉDULA DE IDENTIDAD CLIENTE"].astype(str).str.strip()
        )

        return df_clientes, df_contratos, df_orders

    # ------------------------------------------------------------------
    # BLOQUE 3.5: FILTRO DE ÓRDENES EXITOSAS
    # ------------------------------------------------------------------
    def _filtrar_exitosas(self, df_orders: pd.DataFrame) -> pd.DataFrame:
        """
        Fuente de verdad: solo instalaciones con
        TIPO=Instalación, ESTADO=Cerrado, RESULTADO=Exitosa.
        Estas son las 23 instalaciones reales del período.
        """
        mask = (
            (df_orders["TIPO"].str.strip().str.lower()      == "instalación") &
            (df_orders["ESTADO"].str.strip().str.lower()    == "cerrado")     &
            (df_orders["RESULTADO"].str.strip().str.lower() == "exitosa")
        )
        df_exitosas = df_orders[mask].copy()
        logger.info(
            f"Instalaciones exitosas: {len(df_exitosas)} de {len(df_orders)} órdenes totales"
        )
        return df_exitosas


    # ------------------------------------------------------------------
    # BLOQUE 4: JOIN TRIPLE — ORDERS ↔ CONTRATOS ↔ CLIENTES
    # ------------------------------------------------------------------
    def _fusionar(self, df_orders: pd.DataFrame, df_clientes: pd.DataFrame, df_contratos: pd.DataFrame):
        """
        JOIN 1: orders (fuente de verdad) ← contratos  por ID CONTRATO
                Aporta: MAC, TELÉFONOS, EMAIL, ESTADO servicio, ESTRATO SOCIAL
        JOIN 2: resultado ← clientes       por ID CLIENTE
                Aporta: BARRIO, ZONA, TELÉFONO, TELÉFONO CELULAR, EMAIL cliente
        """
        # JOIN 1: orders + contratos
        df_step1 = pd.merge(
            df_orders,
            df_contratos,
            on="ID CONTRATO",
            how="left",
            suffixes=("_order", "_contrato")
        )

        # Después del JOIN 1, ID CLIENTE queda como ID CLIENTE_order e
        # ID CLIENTE_contrato. Creamos columna auxiliar para el JOIN 2.
        df_step1["_id_cliente_join"] = df_step1["ID CLIENTE_order"]

        # JOIN 2: step1 + clientes
        df_merged = pd.merge(
            df_step1,
            df_clientes,
            left_on="_id_cliente_join",
            right_on="ID CLIENTE",
            how="left",
            suffixes=("", "_cliente")
        )

        df_merged = df_merged.drop(columns=["_id_cliente_join"])

        logger.info(f"Columnas disponibles post-merge: {list(df_merged.columns)}")
        logger.info(f"Registros fusionados: {len(df_merged)}")
        return df_merged




    # ------------------------------------------------------------------
    # BLOQUE 5: DETECCIÓN DE REGISTROS NUEVOS
    # ------------------------------------------------------------------
    def _filtrar_nuevos(self, df_merged: pd.DataFrame):
        """
        Compara contra registro_procesados.json.
        Llave única: cédula del CSV de órdenes.
        """
        cedulas_procesadas = self._cargar_registro()

        col_cedula = "DOCUMENTO O CÉDULA DE IDENTIDAD CLIENTE"
        mascara_nuevos = ~df_merged[col_cedula].isin(cedulas_procesadas)
        df_nuevos = df_merged[mascara_nuevos].copy()

        logger.info(
            f"Registros totales: {len(df_merged)} | "
            f"Ya procesados: {len(df_merged) - len(df_nuevos)} | "
            f"Nuevos esta semana: {len(df_nuevos)}"
        )
        return df_nuevos


    # ------------------------------------------------------------------
    # BLOQUE 6: REGISTRO HISTÓRICO (MEMORIA DEL SISTEMA)
    # ------------------------------------------------------------------
    def _cargar_registro(self) -> set:
        """
        Carga el set de cédulas ya procesadas.
        Si no existe el archivo, retorna set vacío (primera ejecución).
        """
        if not self.ruta_registro.exists():
            logger.info("Primer ejecución — registro_procesados.json no existe, se creará.")
            return set()

        with open(self.ruta_registro, "r", encoding="utf-8") as f:
            data = json.load(f)

        return set(data.get("cedulas_procesadas", []))

    def _actualizar_registro(self, df_nuevos: pd.DataFrame):
        """
        Agrega las cédulas de los nuevos registros al historial.
        Nunca elimina registros anteriores — solo acumula.
        Preserva todas las claves existentes del JSON (ej: seriales_cpe).
        """
        cedulas_actuales = self._cargar_registro()

        col_cedula      = "DOCUMENTO O CÉDULA DE IDENTIDAD CLIENTE"
        cedulas_nuevas  = set(df_nuevos[col_cedula].astype(str).tolist())
        cedulas_totales = cedulas_actuales | cedulas_nuevas

        # Leer el JSON completo para no perder claves como seriales_cpe
        data = {}
        if self.ruta_registro.exists():
            with open(self.ruta_registro, "r", encoding="utf-8") as f:
                data = json.load(f)

        # Actualizar solo las claves propias del merger
        data["cedulas_procesadas"]   = sorted(list(cedulas_totales))
        data["ultima_actualizacion"] = datetime.now().isoformat()
        data["total_procesados"]     = len(cedulas_totales)

        with open(self.ruta_registro, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(
            f"Registro actualizado: {len(cedulas_totales)} cédulas totales "
            f"({len(cedulas_nuevas)} nuevas agregadas)"
        )


    # ------------------------------------------------------------------
    # BLOQUE 6.5: HELPER — LIMPIEZA DE TELÉFONOS
    # ------------------------------------------------------------------
    def _limpiar_telefono(self, valor) -> str:
        """
        Normaliza teléfonos colombianos:
        573152158424.0  →  3152158424
        57 315 215 8424 →  3152158424
        3152158424.0    →  3152158424
        """
        if not valor or str(valor).strip() in ("", "nan", "None"):
            return ""

        tel = str(valor).strip()

        # Quitar .0 si viene como float de pandas
        if tel.endswith(".0"):
            tel = tel[:-2]

        # Quitar espacios, guiones y paréntesis
        tel = tel.replace(" ", "").replace("-", "").replace("(", "").replace(")", "")

        # Quitar prefijo 57 solo si el resultado tiene 12 dígitos (57 + 10)
        if tel.startswith("57") and len(tel) == 12 and tel.isdigit():
            tel = tel[2:]

        return tel


    # ------------------------------------------------------------------
    # BLOQUE 7: CONVERSIÓN AL MODELO DEL PIPELINE
    # ------------------------------------------------------------------
    def _convertir_a_modelo(self, df_nuevos: pd.DataFrame) -> list:
        """
        Convierte el DataFrame fusionado al formato de lista de dicts
        que consume generadores/informe_semanal.py.

        Fuentes por campo:
        - fecha_instalacion  : FINALIZADA EL              (orders)
        - estado_servicio    : ESTADO_contrato             (contratos)
        - latitud/longitud   : LATITUD/LONGITUD (CONTRATO) (orders)
        - direccion          : DIRECCIÓN (CONTRATO)        (orders)
        - barrio             : BARRIO                      (clientes)
        - telefono/celular   : TELÉFONO/TELÉFONO CELULAR   (clientes)
                               TELÉFONOS como fallback     (contratos)
        """
        logger.info(f"Columnas disponibles post-merge: {list(df_nuevos.columns)}")

        registros = []

        for _, row in df_nuevos.iterrows():

            def get_val(*cols):
                for col in cols:
                    val = row.get(col, "")
                    if val and str(val).strip() not in ("", "nan", "None"):
                        return str(val).strip()
                return ""

            # --------------------------------------------------
            # FECHA DE INSTALACIÓN — desde FINALIZADA EL (orders)
            # Limpiar timezone: "2026-02-24 17:40:43 -0500" → "2026-02-24 17:40:43"
            # --------------------------------------------------
            fecha_raw = get_val("FINALIZADA EL")
            if " -0500" in fecha_raw:
                fecha_raw = fecha_raw.replace(" -0500", "").strip()

            # --------------------------------------------------
            # TELÉFONOS — limpiar prefijo 57 y sufijo .0
            # Prioridad: campos individuales de clientes,
            # fallback TELÉFONOS de contratos
            # --------------------------------------------------
            telefono = self._limpiar_telefono(
                get_val("TELÉFONO", "TELÉFONOS")
            )
            celular = self._limpiar_telefono(
                get_val("TELÉFONO CELULAR", "TELÉFONOS")
            )
            if not telefono and celular:
                telefono = celular
            if not celular and telefono:
                celular = telefono

            registro = {
                # ------ IDENTIFICACIÓN ------
                "id_cliente_wispro":     get_val("ID CLIENTE_order"),
                "id_contrato_wispro":    get_val("ID CONTRATO"),
                "id_personalizable":     get_val("ID PERSONALIZABLE_order",
                                                  "USUARIO PPPOE"),
                "documento":             get_val("DOCUMENTO O CÉDULA DE IDENTIDAD CLIENTE"),

                # ------ DATOS PERSONALES ------
                "nombre_completo":       get_val("NOMBRE CLIENTE_order",
                                                  "NOMBRE CLIENTE_contrato",
                                                  "NOMBRE"),
                "email":                 get_val("EMAIL_cliente", "EMAIL"),
                "telefono":              telefono,
                "celular":               celular,

                # ------ UBICACIÓN ------
                "direccion":             get_val("DIRECCIÓN (CONTRATO)",
                                                  "DIRRECIÓN DEL CONTRATO"),
                "complemento_direccion": get_val("DATO ADICIONAL"),
                "barrio":                get_val("BARRIO"),
                "zona":                  get_val("ZONA"),
                "municipio":             "Ipiales",
                "departamento":          "Nariño",
                "latitud":               get_val("LATITUD (CONTRATO)",
                                                  "LATITUD_contrato"),
                "longitud":              get_val("LONGITUD (CONTRATO)",
                                                  "LONGITUD_contrato"),

                # ------ SERVICIO ------
                "fecha_instalacion":     fecha_raw,
                "estado_servicio":       get_val("ESTADO_contrato"),
                "mac_address":           get_val("MAC-ADDRESS"),
                "plan":                  get_val("NOMBRE PLAN_order",
                                                  "NOMBRE PLAN_contrato"),

                # ------ PENDIENTES ------
                "estrato":               get_val("ESTRATO SOCIAL") or "PENDIENTE",
                "serial_cpe":            "PENDIENTE_SCRAPING",

                # ------ PARA EL INFORME MENSUAL ------
                "fecha_finalizacion":    "",
                "causa_suspension":      "",
            }

            registros.append(registro)

        logger.info(f"Modelo generado con {len(registros)} registros nuevos")
        return registros



    # ------------------------------------------------------------------
    # BLOQUE 8: MÉTODO PRINCIPAL
    # ------------------------------------------------------------------
    def procesar(
        self,
        archivo_clientes:  str  = None,
        archivo_contratos: str  = None,
        archivo_orders:    str  = None,
        actualizar_registro: bool = True
    ) -> list:
        """
        Orquesta el proceso completo:
        1. Carga los 3 CSVs (auto-detecta el más reciente si no se especifica)
        2. Filtra solo instalaciones exitosas desde orders
        3. Normaliza los 3 DataFrames
        4. JOIN triple: orders → contratos → clientes
        5. Filtra solo nuevos (no procesados antes)
        6. Actualiza registro histórico
        7. Retorna lista lista para el generador del informe semanal
        """
        def detectar(patron):
            archivos = sorted(self.ruta_entrada.glob(patron))
            if not archivos:
                raise FileNotFoundError(
                    f"No se encontró ningún archivo con patrón: {patron}"
                )
            nombre = archivos[-1].name
            logger.info(f"Archivo detectado automáticamente: {nombre}")
            return nombre

        if archivo_clientes  is None:
            archivo_clientes  = detectar("wispro_clientes_*.csv")
        if archivo_contratos is None:
            archivo_contratos = detectar("wispro_contratos_*.csv")
        if archivo_orders    is None:
            archivo_orders    = detectar("orders_*.csv")

        # Pipeline
        df_clientes, df_contratos, df_orders = self._cargar_csvs(
            archivo_clientes, archivo_contratos, archivo_orders
        )
        df_orders                            = self._filtrar_exitosas(df_orders)
        df_clientes, df_contratos, df_orders = self._normalizar(
            df_clientes, df_contratos, df_orders
        )
        df_merged = self._fusionar(df_orders, df_clientes, df_contratos)
        df_nuevos = self._filtrar_nuevos(df_merged)

        if df_nuevos.empty:
            logger.warning("No hay registros nuevos esta semana.")
            return []

        if actualizar_registro:
            self._actualizar_registro(df_nuevos)

        return self._convertir_a_modelo(df_nuevos)


# ------------------------------------------------------------------
# FUNCIÓN DE ENTRADA (para llamar desde main.py)
# ------------------------------------------------------------------
def procesar_csvs(
    archivo_clientes:  str = None,
    archivo_contratos: str = None,
    archivo_orders:    str = None
) -> list:
    """
    Función síncrona para usar desde main.py o cualquier módulo del pipeline.
    """
    merger = CsvMerger()
    return merger.procesar(archivo_clientes, archivo_contratos, archivo_orders)

