from copy import deepcopy
from datetime import datetime


class WisproAdapter:
    """
    Adaptador WISPRO → Modelo Contractual
    No valida contrato, pero SÍ valida estructura mínima de entrada.
    Aplica filtros contractuales para evitar registros inválidos.
    """

    def __init__(self, modelo_wispro, modelo_contrato_base):
        if not isinstance(modelo_wispro, dict):
            raise TypeError(
                f"modelo_wispro debe ser dict, recibido: {type(modelo_wispro)}"
            )

        if not isinstance(modelo_contrato_base, dict):
            raise TypeError(
                f"modelo_contrato_base debe ser dict, recibido: {type(modelo_contrato_base)}"
            )

        self.wispro = modelo_wispro
        self.modelo = deepcopy(modelo_contrato_base)

    def transformar(self):
        self._mapear_periodo()
        self._mapear_usuarios()
        self._mapear_instalaciones()
        self._mapear_inventario()
        self._mapear_servicio()
        self._mapear_pqrs()
        self._mapear_indicadores()
        self._mapear_facturacion()

        return self.modelo

    # --------------------------------------------------
    # PERIODO
    # --------------------------------------------------
    def _mapear_periodo(self):
        hoy = datetime.now()
        self.modelo["periodo"]["anio"] = hoy.year
        self.modelo["periodo"]["mes"] = hoy.month

    # --------------------------------------------------
    # USUARIOS
    # --------------------------------------------------
    def _mapear_usuarios(self):
        usuarios = self.wispro.get("usuarios", [])
        if not isinstance(usuarios, list):
            raise TypeError("usuarios en modelo_wispro debe ser una lista")

        self.modelo["usuarios"]["total_registrados"] = len(usuarios)
        self.modelo["usuarios"]["activos"] = sum(
            1 for u in usuarios if u.get("estado") == "ACTIVO"
        )
        self.modelo["usuarios"]["suspendidos"] = sum(
            1 for u in usuarios if u.get("estado") == "SUSPENDIDO"
        )
        self.modelo["usuarios"]["retirados"] = sum(
            1 for u in usuarios if u.get("estado") == "RETIRADO"
        )
        self.modelo["usuarios"]["sustitutos"] = sum(
            1 for u in usuarios if u.get("estado") == "SUSTITUTO"
        )

    # --------------------------------------------------
    # INSTALACIONES
    # --------------------------------------------------
    def _mapear_instalaciones(self):
        instalaciones = self.wispro.get("instalaciones", [])
        if not isinstance(instalaciones, list):
            raise TypeError("instalaciones en modelo_wispro debe ser una lista")

        detalle = []

        for inst in instalaciones:

            if not isinstance(inst, dict):
                continue

            usuario_id = inst.get("id_usuario")
            fecha_instalacion = inst.get("fecha_instalacion")
            cpe = inst.get("cpe", {})

            # --------------------------------------------------
            # FILTRO CONTRACTUAL
            # Solo se considera instalación válida si:
            # - Tiene usuario_id
            # - Tiene fecha_instalacion
            # - Tiene serial de CPE
            # --------------------------------------------------
            if (
                not usuario_id
                or not fecha_instalacion
                or not cpe.get("serial")
            ):
                continue

            detalle.append({
                "usuario_id": usuario_id,
                "ubicacion_predio": inst.get("direccion", ""),
                "municipio": inst.get("municipio", ""),
                "fecha_puesta_servicio": fecha_instalacion,
                "cpe_serial": cpe.get("serial"),
                "cpe_marca": cpe.get("marca"),
                "cpe_modelo": cpe.get("modelo"),
                "contrato_prestacion_servicios": {
                    "archivo": inst.get("documentos", {}).get("contrato_servicio"),
                    "formato": "PDF"
                },
                "declaracion_juramentada": {
                    "archivo": inst.get("documentos", {}).get("declaracion_juramentada"),
                    "formato": "PDF"
                },
                "soporte_puesta_servicio": {
                    "archivos": inst.get("documentos", {}).get("soporte_instalacion", []),
                    "tipo": "evidencia_tecnica"
                }
            })

        self.modelo["instalaciones"]["detalle"] = detalle
        self.modelo["instalaciones"]["total_instaladas"] = len(detalle)

    # --------------------------------------------------
    # INVENTARIO CPE
    # --------------------------------------------------
    def _mapear_inventario(self):
        inventario = self.wispro.get("inventario_cpe", [])
        if not isinstance(inventario, list):
            raise TypeError("inventario_cpe debe ser una lista")

        self.modelo["inventario_cpe"]["total_disponible"] = sum(
            1 for i in inventario if i.get("estado") == "DISPONIBLE"
        )
        self.modelo["inventario_cpe"]["total_instalado"] = sum(
            1 for i in inventario if i.get("estado") == "INSTALADO"
        )
        self.modelo["inventario_cpe"]["total_retirado"] = sum(
            1 for i in inventario if i.get("estado") == "RETIRADO"
        )

        self.modelo["inventario_cpe"]["detalle"] = inventario

    # --------------------------------------------------
    # SERVICIO
    # --------------------------------------------------
    def _mapear_servicio(self):
        servicio = self.wispro.get("servicio", {})
        self.modelo["servicio"]["usuarios_fuera_servicio"] = servicio.get("incidentes", [])

    # --------------------------------------------------
    # PQRS
    # --------------------------------------------------
    def _mapear_pqrs(self):
        pqrs = self.wispro.get("pqrs", [])
        if not isinstance(pqrs, list):
            raise TypeError("pqrs debe ser una lista")

        self.modelo["pqrs"]["total"] = len(pqrs)
        self.modelo["pqrs"]["detalle"] = pqrs

    # --------------------------------------------------
    # INDICADORES
    # --------------------------------------------------
    def _mapear_indicadores(self):
        indicadores = self.wispro.get("indicadores", {})
        activos = self.modelo["usuarios"]["activos"]

        self.modelo["indicadores_calidad"]["aplican"] = activos > 0

        if activos > 0:
            self.modelo["indicadores_calidad"]["disponibilidad"] = indicadores.get("disponibilidad")
            self.modelo["indicadores_calidad"]["velocidad_bajada"] = indicadores.get("velocidad_bajada")
            self.modelo["indicadores_calidad"]["velocidad_subida"] = indicadores.get("velocidad_subida")


    # --------------------------------------------------
    # FACTURACIÓN
    # --------------------------------------------------
    def _mapear_facturacion(self):
        facturacion = self.wispro.get("facturacion", {})

        anio = self.modelo["periodo"]["anio"]
        mes = self.modelo["periodo"]["mes"]

        # Generamos periodo automáticamente si Wispro no lo envía
        periodo_generado = f"{anio}-{str(mes).zfill(2)}"

        self.modelo["facturacion"]["periodo"] = (
            facturacion.get("periodo")
            if facturacion.get("periodo")
            else periodo_generado
        )

        self.modelo["facturacion"]["usuarios_facturados"] = (
            facturacion.get("usuarios_facturados", 0)
        )

        self.modelo["facturacion"]["valor_total"] = (
            facturacion.get("valor_total", 0)
        )
