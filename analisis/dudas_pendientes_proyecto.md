# Dudas pendientes para continuar el proyecto

## 1) Fuente y alcance real de datos (Wispro)
1. ¿Cuáles son exactamente los **módulos/exportaciones** que sí podemos obtener hoy de Wispro (clientes, contratos, inventario, PQRS, facturación, incidentes, nodos, PPPoE)?
2. ¿El acceso será por **CSV manual**, exportación automatizada, o API?
3. ¿Cuál es la **frecuencia oficial de corte** para mensual y semanal (fecha/hora)?
4. ¿Existe un **diccionario de datos oficial** por cada exportación (columnas, tipos, catálogos de estado)?
5. ¿Qué campos contractuales no existen en Wispro y se capturarán por otra vía?

## 2) Reglas contractuales que faltan de cerrar
6. ¿Cuál es la definición contractual exacta de **usuario activo/suspendido/retirado/sustituto** para evitar ambigüedad en el mapeo?
7. ¿Cómo se calcula formalmente cada **indicador de calidad** (fórmula, unidad, redondeo y fuente)?
8. ¿Qué condiciones activan **compensaciones** y cómo se valoran (tabla/regla)?
9. ¿Qué estructura final tendrá **hitos** (catálogo fijo vs libre)?
10. ¿Qué estructura final tendrán **obligaciones contractuales** y periodicidad de seguimiento?

## 3) Plantilla y salida de informes
11. ¿La plantilla mensual actual ya está **congelada** o aún está sujeta a cambios de interventoría?
12. ¿El entregable oficial debe ser solo **DOCX**, o también PDF y/o Markdown como soporte interno?
13. ¿Hay un formato corporativo obligatorio (logos, portada, numeración, tablas, firmas)?
14. ¿Qué secciones deben ser 100% determinísticas y cuáles permiten texto asistido (IA)?

## 4) Informe semanal (aún no definido)
15. ¿Cuál es la **estructura exacta** del Excel semanal (columnas, hojas, validaciones)?
16. ¿El semanal es incremental por semana o acumulado mensual con corte semanal?
17. ¿Qué reglas de conciliación debe cumplir frente al mensual?

## 5) Evidencias documentales por instalación
18. ¿Dónde se almacenarán los soportes (contrato, declaración juramentada, evidencia técnica)?
19. ¿Se requiere validar existencia de archivo, metadatos o trazabilidad de firma?
20. ¿Cómo se relaciona cada evidencia con el registro técnico (ID único de instalación/usuario)?

## 6) Gobierno de datos, auditoría y operación
21. ¿Qué nivel de **auditoría** se espera (logs por ejecución, cambios, errores, usuario operador)?
22. ¿Habrá flujo de revisión/aprobación previo al envío a interventoría?
23. ¿Qué ambiente será productivo (servidor local, nube, CI/CD)?
24. ¿Qué SLA de tiempos de generación y recuperación ante fallos se exige?

## 7) Estado técnico detectado para confirmar contigo
25. ¿Confirmas que el repositorio actual usa un **modelo_wispro.json estático** como entrada temporal y aún no integra CSV reales?
26. ¿Confirmas que la generación mensual está **parcial** (secciones incompletas) y que semanal/IA/DOCX todavía no están operativos?
27. ¿Confirmas que debemos priorizar en este orden: (1) integración real de datos, (2) definición semanal, (3) DOCX, (4) IA controlada?

## Prioridad recomendada para resolver dudas
- **Críticas (bloqueantes):** 1, 2, 4, 7, 11, 15, 18, 25.
- **Altas:** 6, 8, 9, 10, 12, 16, 17, 21.
- **Medias:** 3, 5, 13, 14, 19, 20, 22, 23, 24, 26, 27.
