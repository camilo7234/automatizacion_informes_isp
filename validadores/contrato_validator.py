import json
import yaml
from pathlib import Path


class ErrorValidacionContrato(Exception):
    """Error crítico de validación contractual"""
    pass

class ContratoValidator:
    def __init__(self, ruta_contrato_reglas, ruta_validaciones, ruta_modelo=None, modelo_en_memoria=None):
        self.contrato_reglas = self._cargar_yaml(ruta_contrato_reglas)
        self.validaciones = self._cargar_json(ruta_validaciones)

        if modelo_en_memoria is not None:
            self.modelo = modelo_en_memoria
        elif ruta_modelo is not None:
            self.modelo = self._cargar_json(ruta_modelo)
        else:
            raise ErrorValidacionContrato(
                "Debe proporcionarse ruta_modelo o modelo_en_memoria para validar"
            )


    # -------------------------
    # CARGA DE ARCHIVOS
    # -------------------------
    def _cargar_yaml(self, ruta):
        ruta = Path(ruta)
        if not ruta.exists():
            raise ErrorValidacionContrato(f"No existe el archivo YAML: {ruta}")
        with open(ruta, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)

    def _cargar_json(self, ruta):
        ruta = Path(ruta)
        if not ruta.exists():
            raise ErrorValidacionContrato(f"No existe el archivo JSON: {ruta}")
        with open(ruta, "r", encoding="utf-8") as f:
            return json.load(f)


    # -------------------------------------------------
    # VALIDACIONES GENERALES
    # -------------------------------------------------

    # -------------------------
    # VALIDACIÓN DE CONSISTENCIA ESTRUCTURAL
    # -------------------------
    def validar_esquema_modelo(self):
        """
        Verifica que la estructura del modelo contractual coincida
        con la estructura declarada en validaciones.json.

        Ignora metadatos internos del esquema.
        """

        def extraer_rutas_validacion(dic, prefijo=""):
            rutas = set()

            if not isinstance(dic, dict):
                return rutas

            for k, v in dic.items():

                # Ignorar bloques globales
                if k in ["modo_validacion", "reglas_generales"]:
                    continue

                # Ignorar metadatos internos
                if k in ["obligatorio", "_tipo", "campos"]:
                    continue

                ruta = f"{prefijo}.{k}" if prefijo else k
                rutas.add(ruta)

                if isinstance(v, dict):
                    rutas.update(extraer_rutas_validacion(v, ruta))

            return rutas

        def extraer_rutas_modelo(dic, prefijo=""):
            rutas = set()

            if isinstance(dic, dict):
                for k, v in dic.items():
                    ruta = f"{prefijo}.{k}" if prefijo else k
                    rutas.add(ruta)
                    rutas.update(extraer_rutas_modelo(v, ruta))

            elif isinstance(dic, list) and len(dic) > 0:
                rutas.update(extraer_rutas_modelo(dic[0], prefijo))

            return rutas

        rutas_modelo = extraer_rutas_modelo(self.modelo)
        rutas_validacion = extraer_rutas_validacion(self.validaciones)

        diferencias = rutas_validacion - rutas_modelo

        if diferencias:
            raise ErrorValidacionContrato(
                f"Inconsistencias estructurales detectadas entre modelo y validaciones: {diferencias}"
            )

    # -------------------------
    # EJECUCIÓN GLOBAL DE VALIDACIONES
    # -------------------------
    def validar(self):
        """
        Ejecuta todas las validaciones contractuales
        en orden estricto y bloqueante.
        """

        # 1. Validación estructural (ANTI-DESALINEACIÓN)
        self.validar_esquema_modelo()

        # 2. Validación modo estricto
        self._validar_modo_estricto()

        # 3. Validación de campos obligatorios
        self._validar_campos_obligatorios()

        # 4. Validación de texto genérico prohibido
        self._validar_texto_generico()

        # 5. Validación de reglas condicionales
        self._validar_reglas_condicionales()

    # -------------------------
    # VALIDACIÓN MODO ESTRICTO
    # -------------------------
    def _validar_modo_estricto(self):
        """
        Verifica que el contrato esté configurado
        en modo de validación estricta.
        """

        modo = self.contrato_reglas.get("modo", {}).get("validacion")

        if modo != "estricto":
            raise ErrorValidacionContrato(
                "El sistema NO está en modo interventoría estricta"
            )

    # -------------------------
    # VALIDACIÓN DE CAMPOS OBLIGATORIOS
    # -------------------------
    def _validar_campos_obligatorios(self):
        """
        Valida estructura contractual obligatoria basada en validaciones.json.
        Solo procesa reglas estructurales.

        Ignora bloques globales como reglas_generales.
        """
        reglas = self.validaciones
        self._recorrer_reglas(self.modelo, reglas)

    def _recorrer_reglas(self, datos, reglas, ruta=""):
        """
        Recorre recursivamente el modelo contractual comparándolo
        contra las reglas estructurales definidas.

        Soporta:
        - Objetos simples
        - Listas declaradas con "_tipo": "lista"
        - Campos obligatorios
        """

        # Blindaje: solo procesar reglas tipo dict
        if not isinstance(reglas, dict):
            return

        for clave, regla in reglas.items():

            # Ignorar claves técnicas del esquema
            if clave in ["_tipo", "campos"]:
                continue

            # Ignorar entradas que no sean reglas estructurales
            if not isinstance(regla, dict):
                continue

            ruta_actual = f"{ruta}.{clave}" if ruta else clave

            # --------------------------------------------------
            # VALIDACIÓN: CAMPO EXISTE
            # --------------------------------------------------
            if clave not in datos:
                if regla.get("obligatorio", False):
                    raise ErrorValidacionContrato(
                        f"Falta campo obligatorio: {ruta_actual}"
                    )
                continue

            valor = datos[clave]

            # --------------------------------------------------
            # VALIDACIÓN: LISTAS ESTRUCTURALES
            # --------------------------------------------------
            if regla.get("_tipo") == "lista":

                if not isinstance(valor, list):
                    raise ErrorValidacionContrato(
                        f"{ruta_actual} debe ser una lista"
                    )

                for i, item in enumerate(valor):
                    if not isinstance(item, dict):
                        raise ErrorValidacionContrato(
                            f"{ruta_actual}[{i}] debe ser un objeto"
                        )

                    self._recorrer_reglas(
                        item,
                        regla.get("campos", {}),
                        f"{ruta_actual}[{i}]"
                    )

                continue

            # --------------------------------------------------
            # VALIDACIÓN RECURSIVA PARA OBJETOS
            # --------------------------------------------------
            if isinstance(valor, dict):
                self._recorrer_reglas(valor, regla, ruta_actual)
                continue

            # --------------------------------------------------
            # VALIDACIÓN: CAMPO OBLIGATORIO SIMPLE
            # --------------------------------------------------
            if regla.get("obligatorio", False):
                if self._valor_invalido(valor):
                    raise ErrorValidacionContrato(
                        f"Campo obligatorio inválido o vacío: {ruta_actual}"
                    )


            # --------------------------------------------------
            # VALIDACIÓN DE ESTRUCTURAS ANIDADAS
            # --------------------------------------------------
            if "detalle" in regla and isinstance(regla["detalle"], dict):

                # Caso 1: Lista de elementos (ej: instalaciones.detalle)
                if isinstance(valor, list):

                    # Si la lista está vacía, NO se valida contenido interno
                    # (Ejemplo válido: 0 instalaciones en el mes)
                    if len(valor) == 0:
                        continue

                    for i, item in enumerate(valor):
                        self._recorrer_reglas(
                            item,
                            regla["detalle"],
                            f"{ruta_actual}[{i}]"
                        )

                # Caso 2: Diccionario anidado
                elif isinstance(valor, dict):
                    self._recorrer_reglas(
                        valor,
                        regla["detalle"],
                        ruta_actual
                    )

            # --------------------------------------------------
            # VALIDACIÓN DE OBLIGATORIEDAD SIMPLE
            # --------------------------------------------------
            if regla.get("obligatorio", False):

                # Si es lista vacía y no es obligatoria estructuralmente,
                # no se considera inválido (control ya realizado arriba)
                if isinstance(valor, list) and len(valor) == 0:
                    continue

                if self._valor_invalido(valor):
                    raise ErrorValidacionContrato(
                        f"Campo obligatorio inválido o vacío: {ruta_actual}"
                    )

            

    # -------------------------
    # TEXTO GENÉRICO PROHIBIDO
    # -------------------------
    def _validar_texto_generico(self):
        textos_prohibidos = self.validaciones["reglas_generales"][
            "texto_generico_prohibido"
        ]
        self._buscar_texto_generico(self.modelo, textos_prohibidos)

    def _buscar_texto_generico(self, datos, textos_prohibidos, ruta=""):
        if isinstance(datos, dict):
            for k, v in datos.items():
                self._buscar_texto_generico(v, textos_prohibidos, f"{ruta}.{k}")
        elif isinstance(datos, list):
            for i, item in enumerate(datos):
                self._buscar_texto_generico(
                    item, textos_prohibidos, f"{ruta}[{i}]"
                )
        elif isinstance(datos, str):
            if datos.strip().upper() in textos_prohibidos:
                raise ErrorValidacionContrato(
                    f"Texto genérico prohibido detectado en {ruta}: '{datos}'"
                )

    # -------------------------
    # REGLAS CONDICIONALES
    # -------------------------
    def _validar_reglas_condicionales(self):
        """
        Valida reglas condicionales contractuales.
        No asume estructuras inexistentes.
        """

        # Indicadores solo aplican si hay usuarios activos
        usuarios_activos = self.modelo["usuarios"]["activos"]
        indicadores = self.modelo["indicadores_calidad"]

        if usuarios_activos == 0 and indicadores["aplican"]:
            raise ErrorValidacionContrato(
                "Indicadores de calidad NO pueden aplicar sin usuarios activos"
            )

        # Si total_instaladas es 0, simplemente se reporta en informe.
        # No se exige campo analisis estructural aquí.

        # Si PQRS total es 0, se reportará en el informe.
        # No se exige campo analisis estructural aquí.

        
    # -------------------------
    # UTILIDADES
    # -------------------------
    def _valor_invalido(self, valor):
        """
        Determina si un valor se considera inválido
        bajo modo de validación estricta.
        """

        if valor is None:
            return True

        if isinstance(valor, str) and valor.strip() == "":
            return True

        if isinstance(valor, list) and len(valor) == 0:
            return True

        return False
