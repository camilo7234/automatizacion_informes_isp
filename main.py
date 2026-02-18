import json
import sys
from pathlib import Path

from adaptadores.wispro_adapter import WisproAdapter
from validadores.contrato_validator import (
    ContratoValidator,
    ErrorValidacionContrato,
)
from generadores.informe_mensual import GeneradorInformeMensual


BASE_DIR = Path(__file__).parent


# --------------------------------------------------
# UTILIDADES
# --------------------------------------------------

def cargar_json(ruta):
    if not ruta.exists():
        raise FileNotFoundError(f"No existe el archivo requerido: {ruta}")
    with open(ruta, "r", encoding="utf-8") as f:
        return json.load(f)


# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():
    try:
        print("========== INICIO PROCESO ==========")

        # --------------------------------------------------
        # 1. CARGA DE CONFIGURACIÓN Y MODELOS
        # --------------------------------------------------
        modelo_wispro_path = BASE_DIR / "modelos" / "modelo_wispro.json"
        modelo_contrato_base_path = BASE_DIR / "modelos" / "modelo_contrato.json"

        contrato_reglas_path = BASE_DIR / "config" / "contrato_reglas.yaml"
        validaciones_path = BASE_DIR / "modelos" / "validaciones.json"

        modelo_wispro = cargar_json(modelo_wispro_path)
        modelo_contrato_base = cargar_json(modelo_contrato_base_path)

        print("✔ Modelos cargados.")

        # --------------------------------------------------
        # 2. TRANSFORMACIÓN WISPRO → MODELO CONTRACTUAL
        # --------------------------------------------------
        adapter = WisproAdapter(
            modelo_wispro=modelo_wispro,
            modelo_contrato_base=modelo_contrato_base,
        )

        modelo_contrato = adapter.transformar()

        print("✔ Transformación completada.")

        # --------------------------------------------------
        # 3. VALIDACIÓN CONTRACTUAL (BLOQUEANTE)
        # --------------------------------------------------
        validator = ContratoValidator(
            ruta_contrato_reglas=contrato_reglas_path,
            ruta_validaciones=validaciones_path,
            modelo_en_memoria=modelo_contrato,
        )

        validator.validar()

        print("✔ MODELO CONTRACTUAL VALIDADO.")

        # --------------------------------------------------
        # 4. GENERACIÓN INFORME MENSUAL
        # --------------------------------------------------
        plantilla_path = BASE_DIR / "plantillas" / "informe_mensual_base.md"

        generador = GeneradorInformeMensual(
            modelo_contrato=modelo_contrato,
            plantilla_path=plantilla_path,
        )

        informe_generado = generador.generar()

        # ⚠️ La carpeta debe existir manualmente
        salida_path = BASE_DIR / "salidas" / "borradores" / "informe_mensual_generado.md"

        if not salida_path.parent.exists():
            raise FileNotFoundError(
                f"La carpeta de salida no existe: {salida_path.parent}"
            )

        salida_path.write_text(informe_generado, encoding="utf-8")

        print("✔ Informe mensual generado correctamente.")
        print(f"✔ Archivo creado en: {salida_path}")

        print("========== PROCESO FINALIZADO ==========")

    except ErrorValidacionContrato as e:
        print("✖ ERROR CONTRACTUAL CRÍTICO")
        print(str(e))
        sys.exit(1)

    except Exception as e:
        print("✖ ERROR TÉCNICO NO CONTROLADO")
        print(str(e))
        sys.exit(2)


if __name__ == "__main__":
    main()
