# generadores/informe_semanal.py
"""
Genera el informe semanal en formato Excel (.xlsx).
Columnas exactas según la plantilla oficial del Ministerio/Auditoría.
Genera SOLO los registros nuevos de la semana actual.
El usuario copia y pega en el repositorio compartido.
"""

import re
import json
import logging
from datetime import datetime
from pathlib import Path

import openpyxl
import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------------
# COLUMNAS OFICIALES — ORDEN Y NOMBRES EXACTOS DEL MINISTERIO
# ------------------------------------------------------------------
COLUMNAS_OFICIALES = [
    "ID CUENTA",
    "Código DANE Departamento",
    "Departamento",
    "Código DANE Municipio",
    "Municipio",
    "Dirección del predio",
    "Barrio",
    "Latitud",
    "Longitud",
    "Estrato",
    "Nombre",
    "Apellido",
    "Nacionalidad",
    "Tipo de documento",
    "Número documento de identidad",
    "Teléfono",
    "Celular",
    "E mail",
    "No ha tenido servicio de INTERNET en los últimos seis (6) meses. Registre si presenta declaración juramentada (SI/NO)",
    "Fecha de instalación",
    "NÚMERO DE SERIE DEL CPE INSTALADO",
    "Fecha de finalización del servicio",
    "Estado del servicio",
    "Si el servicio se encuentra suspendido",
]

# ------------------------------------------------------------------
# CONSTANTES FIJAS DEL CONTRATO
# ------------------------------------------------------------------
DANE_DPTO          = "52"
DEPARTAMENTO       = "Nariño"
DANE_MPIO          = "52356"
MUNICIPIO          = "Ipiales"
NACIONALIDAD       = "Colombia"
TIPO_DOCUMENTO     = "CÉDULA DE CIUDADANÍA"
DECLARACION_JUR    = "SI"

ESTADO_TRADUCCION = {
    "habilitado": "OPERATIVO",
    "enabled":    "OPERATIVO",
    "activo":     "OPERATIVO",
    "suspendido": "SUSPENDIDO",
    "suspended":  "SUSPENDIDO",
    "disabled":   "SUSPENDIDO",
    "retirado":   "RETIRADO",
}


class GeneradorInformeSemanal:
    """
    Genera un Excel con SOLO los registros nuevos de la semana.
    El usuario lo revisa y copia al repositorio compartido del Ministerio.
    El ID CUENTA es secuencial y continúa desde el último registrado.
    """

    def __init__(self, config_path="config/entorno.yaml"):
        import yaml
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        self.ruta_salida = Path("salidas/informes_semanales")
        self.ruta_salida.mkdir(parents=True, exist_ok=True)

        self.ruta_registro = Path(
            "datos/procesados/modelo_contrato/registro_procesados.json"
        )

    # ------------------------------------------------------------------
    # BLOQUE 1: SEPARAR NOMBRE Y APELLIDO
    # Limpia prefijos M-XXXX, luego:
    # 1 palabra  → Nombre = palabra,  Apellido = ""
    # 2 palabras → Nombre = [0],      Apellido = [1]
    # 3 palabras → Nombre = [0][1],   Apellido = [2]
    # 4+ palabras→ Nombre = [0][1],   Apellido = resto
    # ------------------------------------------------------------------
    def _separar_nombre_apellido(self, nombre_completo: str):
        if not nombre_completo:
            return "", ""

        limpio = re.sub(r"^M-\d+\s*", "", nombre_completo.strip()).strip()
        palabras = limpio.split()

        if len(palabras) == 0:
            return "", ""
        elif len(palabras) == 1:
            return palabras[0], ""
        elif len(palabras) == 2:
            return palabras[0], palabras[1]
        elif len(palabras) == 3:
            return f"{palabras[0]} {palabras[1]}", palabras[2]
        else:
            return f"{palabras[0]} {palabras[1]}", " ".join(palabras[2:])

    # ------------------------------------------------------------------
    # BLOQUE 2: TRADUCIR ESTADO WISPRO → MINISTERIO
    # ------------------------------------------------------------------
    def _traducir_estado(self, estado: str) -> str:
        if not estado:
            return "OPERATIVO"
        return ESTADO_TRADUCCION.get(estado.lower().strip(), "OPERATIVO")

    # ------------------------------------------------------------------
    # BLOQUE 3: CRUCE SERIAL CPE ↔ ID CONTRATO
    # Lee el dict {id_contrato: serial} guardado por playwright_extractor
    # ------------------------------------------------------------------
    def _construir_mapa_seriales(self) -> dict:
        """
        Lee registro_procesados.json y retorna dict { id_contrato: serial }.
        El dict fue guardado por playwright_extractor.py BLOQUE 9
        bajo la clave "seriales_cpe".
        """
        mapa = {}

        if not self.ruta_registro.exists():
            logger.warning("registro_procesados.json no encontrado. Seriales en PENDIENTE.")
            return mapa

        with open(self.ruta_registro, "r", encoding="utf-8") as f:
            data = json.load(f)

        mapa = data.get("seriales_cpe", {})

        logger.info(f"Mapa seriales CPE: {len(mapa)} entradas")
        return mapa


    # ------------------------------------------------------------------
    # BLOQUE 4: OBTENER ÚLTIMO NÚMERO DE ID CUENTA
    # Lee el registro para saber desde qué número continuar
    # ------------------------------------------------------------------
    def _obtener_ultimo_id(self) -> int:
        """
        Lee el campo 'ultimo_id_cuenta' del registro.
        Si no existe, retorna 0 (primera semana).
        """
        if not self.ruta_registro.exists():
            return 0

        with open(self.ruta_registro, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data.get("ultimo_id_cuenta", 0)

    # ------------------------------------------------------------------
    # BLOQUE 5: GUARDAR ÚLTIMO ID EN REGISTRO
    # ------------------------------------------------------------------
    def _guardar_ultimo_id(self, ultimo: int, df_nuevos: pd.DataFrame):
        """
        Persiste en registro_procesados.json:
        - ultimo_id_cuenta   → para continuar secuencia la semana siguiente
        - indice_email       → dict {email: id_cuenta} para cruzar tickets
        Preserva todas las claves existentes del JSON (ej: seriales_cpe).
        """
        if not self.ruta_registro.exists():
            return

        with open(self.ruta_registro, "r", encoding="utf-8") as f:
            data = json.load(f)

        # Persistir último número secuencial
        data["ultimo_id_cuenta"]    = ultimo
        data["ultima_actualizacion"] = datetime.now().isoformat()

        # --------------------------------------------------
        # CONSTRUIR Y ACUMULAR ÍNDICE EMAIL → ID CUENTA
        # Acumula semanas anteriores + los nuevos de esta semana
        # --------------------------------------------------
        indice_actual = data.get("indice_email", {})

        for fila in df_nuevos.to_dict(orient="records"):
            email      = str(fila.get("E mail", "")).strip().lower()
            id_cuenta  = str(fila.get("ID CUENTA", "")).strip()
            if email and email not in ("", "nan", "none") and id_cuenta:
                indice_actual[email] = id_cuenta

        data["indice_email"] = indice_actual

        with open(self.ruta_registro, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info(f"Último ID CUENTA guardado: A {str(ultimo).zfill(6)}")
        logger.info(f"Índice email actualizado: {len(indice_actual)} entradas")

    # ------------------------------------------------------------------
    # BLOQUE 6: TRANSFORMAR REGISTROS AL FORMATO OFICIAL
    # ------------------------------------------------------------------
    def _transformar_registros(
        self,
        registros: list,
        mapa_seriales: dict,
        ultimo_numero: int
    ) -> pd.DataFrame:

        filas = []
        contador = ultimo_numero

        for reg in registros:
            contador += 1

            nombre, apellido = self._separar_nombre_apellido(
                reg.get("nombre_completo", "")
            )

            id_contrato = str(reg.get("id_contrato_wispro", "")).strip()
            serial_cpe  = mapa_seriales.get(id_contrato, "PENDIENTE")

            estado = self._traducir_estado(reg.get("estado_servicio", ""))

            causa_suspension = (
                reg.get("causa_suspension", "")
                if estado == "SUSPENDIDO" else ""
            )

            fecha_instalacion = str(reg.get("fecha_instalacion", "")).strip()
            if " " in fecha_instalacion:
                fecha_instalacion = fecha_instalacion.split(" ")[0]

            filas.append({
                "ID CUENTA":                   f"A {str(contador).zfill(6)}",
                "Código DANE Departamento":    DANE_DPTO,
                "Departamento":                DEPARTAMENTO,
                "Código DANE Municipio":       DANE_MPIO,
                "Municipio":                   MUNICIPIO,
                "Dirección del predio":        reg.get("direccion", ""),
                "Barrio":                      reg.get("barrio", ""),
                "Latitud":                     reg.get("latitud", ""),
                "Longitud":                    reg.get("longitud", ""),
                "Estrato":                     reg.get("estrato", "PENDIENTE"),
                "Nombre":                      nombre,
                "Apellido":                    apellido,
                "Nacionalidad":                NACIONALIDAD,
                "Tipo de documento":           TIPO_DOCUMENTO,
                "Número documento de identidad": reg.get("documento", ""),
                "Teléfono":                    reg.get("telefono", ""),
                "Celular":                     reg.get("telefono", ""),
                "E mail":                      reg.get("email", ""),
                "No ha tenido servicio de INTERNET en los últimos seis (6) meses. Registre si presenta declaración juramentada (SI/NO)": DECLARACION_JUR,
                "Fecha de instalación":        fecha_instalacion,
                "NÚMERO DE SERIE DEL CPE INSTALADO": serial_cpe,
                "Fecha de finalización del servicio": reg.get("fecha_finalizacion", ""),
                "Estado del servicio":         estado,
                "Si el servicio se encuentra suspendido": causa_suspension,
            })

        return pd.DataFrame(filas, columns=COLUMNAS_OFICIALES)

    # ------------------------------------------------------------------
    # BLOQUE 7: GUARDAR EXCEL DE LA SEMANA
    # Nombre del archivo incluye la fecha de corte para trazabilidad
    # ------------------------------------------------------------------
    def _guardar_excel(self, df: pd.DataFrame) -> Path:
        fecha_hoy = datetime.now().strftime("%Y-%m-%d")
        nombre_archivo = f"informe_semanal_{fecha_hoy}.xlsx"
        ruta_archivo = self.ruta_salida / nombre_archivo

        with pd.ExcelWriter(ruta_archivo, engine="openpyxl", mode="w") as writer:
            df.to_excel(
                writer,
                sheet_name="INFORMACIÓN BASICA",
                index=False
            )

        logger.info(f"Excel semanal guardado: {ruta_archivo} ({len(df)} filas)")
        return ruta_archivo

    # ------------------------------------------------------------------
    # BLOQUE 8: MÉTODO PRINCIPAL
    # ------------------------------------------------------------------
    def generar(self, registros: list) -> Path:
        """
        Genera el Excel semanal con SOLO los registros nuevos.
        Actualiza el último ID CUENTA en registro_procesados.json.
        Actualiza el índice email → ID CUENTA para el módulo de tickets.
        Retorna la ruta del archivo generado.
        """
        if not registros:
            logger.warning("No hay registros nuevos. No se genera Excel.")
            return None

        logger.info(f"Iniciando generación — {len(registros)} registros nuevos...")

        mapa_seriales = self._construir_mapa_seriales()
        ultimo_numero = self._obtener_ultimo_id()
        df_nuevos     = self._transformar_registros(registros, mapa_seriales, ultimo_numero)
        ruta_excel    = self._guardar_excel(df_nuevos)

        # Persistir último ID e índice email → ID CUENTA
        self._guardar_ultimo_id(ultimo_numero + len(registros), df_nuevos)

        logger.info(
            f"\n{'='*50}\n"
            f"  INFORME SEMANAL GENERADO\n"
            f"  Registros nuevos:  {len(df_nuevos)}\n"
            f"  IDs asignados:     A {str(ultimo_numero+1).zfill(6)} → "
            f"A {str(ultimo_numero+len(registros)).zfill(6)}\n"
            f"  Archivo:           {ruta_excel}\n"
            f"{'='*50}"
        )

        return ruta_excel



# ------------------------------------------------------------------
# FUNCIÓN DE ENTRADA — llamar desde main.py
# ------------------------------------------------------------------
def generar_informe_semanal(registros: list) -> Path:
    generador = GeneradorInformeSemanal()
    return generador.generar(registros)
