
# test.py
from procesadores.csv_merger import procesar_csvs
from generadores.informe_semanal import generar_informe_semanal

# 1. Obtener registros nuevos
registros = procesar_csvs()
print(f"Registros nuevos: {len(registros)}")

# 2. Generar Excel
ruta = generar_informe_semanal(registros)
print(f"Excel generado en: {ruta}")
