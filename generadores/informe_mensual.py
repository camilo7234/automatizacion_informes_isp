from pathlib import Path


class GeneradorInformeMensual:
    """
    Generador determinístico del Informe Mensual.

    - Recibe un modelo contractual ya VALIDADO.
    - Recibe una plantilla markdown base.
    - Reemplaza placeholders sección por sección.
    - No realiza validaciones contractuales.
    """

    # ==================================================
    # BLOQUE 1 - INICIALIZACIÓN
    # ==================================================

    def __init__(self, modelo_contrato, plantilla_path):
        self.modelo = modelo_contrato
        self.plantilla_path = Path(plantilla_path)

        if not self.plantilla_path.exists():
            raise FileNotFoundError(
                f"No existe plantilla: {plantilla_path}"
            )

    # ==================================================
    # BLOQUE 2 - MÉTODO PRINCIPAL
    # ==================================================

    def generar(self):
        """
        Orquesta la generación completa del informe.
        El orden de ejecución es importante.
        """

        contenido = self.plantilla_path.read_text(encoding="utf-8")

        # ---- Variables generales del documento ----
        contenido = self._reemplazar_variables_basicas(contenido)

        # ---- Sección 1 ----
        contenido = self._generar_resumen_ejecutivo(contenido)

        # ---- Sección 2.1 ----
        contenido = self._generar_tabla_instalaciones(contenido)

        # ---- Limpieza final de placeholders no procesados ----
        contenido = self._limpiar_placeholders_restantes(contenido)

        return contenido

    # ==================================================
    # BLOQUE 3 - VARIABLES GENERALES
    # ==================================================

    def _reemplazar_variables_basicas(self, texto):
        """
        Reemplaza variables globales del encabezado del informe.
        """

        periodo = self.modelo["periodo"]
        proyecto = self.modelo["identificacion_proyecto"]

        reemplazos = {
            "{{anio}}": str(periodo["anio"]),
            "{{mes}}": str(periodo["mes"]),
            "{{municipio}}": proyecto["municipio"],
            "{{departamento}}": proyecto["departamento"],
            "{{numero_informe}}": "1",
            "{{version}}": "1.0",
            "{{fecha_emision}}": "POR DEFINIR"
        }

        for clave, valor in reemplazos.items():
            texto = texto.replace(clave, valor)

        return texto

    # ==================================================
    # BLOQUE 4 - SECCIÓN 1: RESUMEN EJECUTIVO
    # ==================================================

    def _generar_resumen_ejecutivo(self, texto):
        """
        Construye automáticamente el resumen ejecutivo
        con base en el estado del modelo contractual.
        """

        usuarios = self.modelo["usuarios"]
        instalaciones = self.modelo["instalaciones"]["total_instaladas"]

        total = usuarios["total_registrados"]
        activos = usuarios["activos"]
        suspendidos = usuarios["suspendidos"]
        retirados = usuarios["retirados"]

        if activos == 0:
            estado = (
                "El proyecto se encuentra en fase de implementación "
                "y alistamiento operativo."
            )
        else:
            estado = (
                "El proyecto se encuentra en fase operativa "
                "con usuarios activos."
            )

        resumen = (
            f"Durante el periodo reportado se registran {total} usuarios "
            f"en el sistema, de los cuales {activos} se encuentran activos, "
            f"{suspendidos} suspendidos y {retirados} retirados. "
            f"Se realizaron {instalaciones} instalaciones en el periodo. "
            f"{estado}"
        )

        return texto.replace("{{resumen_ejecutivo}}", resumen)

    # ==================================================
    # BLOQUE 5 - SECCIÓN 2.1: INSTALACIONES
    # ==================================================

    def _generar_tabla_instalaciones(self, texto):
        """
        Genera listado estructurado de instalaciones realizadas.
        """

        instalaciones = self.modelo["instalaciones"]["detalle"]

        if not instalaciones:
            tabla = "No se realizaron instalaciones en el periodo reportado."
        else:
            filas = []

            for inst in instalaciones:
                fila = (
                    f"- Usuario: {inst['usuario_id']} | "
                    f"Fecha: {inst['fecha_puesta_servicio']} | "
                    f"Serial CPE: {inst['cpe_serial']}"
                )
                filas.append(fila)

            tabla = "\n".join(filas)

        return texto.replace("{{tabla_instalaciones}}", tabla)

    # ==================================================
    # BLOQUE 6 - CONTROL DE PLACEHOLDERS NO PROCESADOS
    # ==================================================

    def _limpiar_placeholders_restantes(self, texto):
        """
        Reemplaza cualquier placeholder no procesado
        por un texto controlado.

        Esto evita que en producción aparezcan variables sin renderizar.
        """

        import re

        patron = r"\{\{.*?\}\}"

        placeholders = re.findall(patron, texto)

        for ph in placeholders:
            texto = texto.replace(
                ph,
                "Información no disponible para el periodo reportado."
            )

        return texto
