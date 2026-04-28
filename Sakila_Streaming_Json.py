# =========================================================================
# Generador 1 a 1 de Colecciones JSON para MongoDB (Sakila Streaming)
# =========================================================================
import pandas as pd
import json
import os
import numpy as np

# ---------------------------------------------------------
# 1. FUNCIÓN PARA CARGAR ARCHIVOS DE FORMA SEGURA
# ---------------------------------------------------------
def load_data(filename):
    pwd = os.path.dirname(__file__) if '__file__' in globals() else os.getcwd()
    ruta = os.path.join(pwd, filename)
    try:
        return pd.read_excel(ruta)
    except Exception as e:
        print(f"  -> [Aviso] No se encontró o falló lectura de {filename}. Se saltará.")
        return pd.DataFrame()

# Limpia valores NaN que Pandas genera y MongoDB rechazaría
def is_nan(val):
    if isinstance(val, float) and np.isnan(val):
        return True
    if pd.isna(val) if not isinstance(val, (list, tuple, dict, np.ndarray)) else False:
        return True
    return False

def clean_nan(obj):
    if isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items() if not is_nan(v)}
    elif isinstance(obj, (list, tuple, np.ndarray)):
        return [clean_nan(i) for i in obj]
    elif is_nan(obj):
        return None
    return obj

# ---------------------------------------------------------
# 2. DEFINICIÓN DE ARCHIVOS Y SUS COLECCIONES DESTINO
# ---------------------------------------------------------
ARCHIVOS = {
    "DimCategory.xls": "DimCategory.json",
    "DimContent.xls": "DimContent.json",
    "dimContentCategory.xls": "dimContentCategory.json",
    "dimContentTalent.xls": "dimContentTalent.json",
    "DimCustomer.xls": "DimCustomer.json",
    "DimDate.xls": "DimDate.json",
    "DimGeography.xls": "DimGeography.json",
    "DimLanguage.xls": "DimLanguage.json",
    "dimPaymentMethod.xls": "dimPaymentMethod.json",
    "dimProvider.xls": "dimProvider.json",
    "dimSuscriptionPlan.xls": "dimSuscriptionPlan.json",
    "dimTalent.xls": "dimTalent.json",
    "FacCatalogAvailability.xls": "FacCatalogAvailability.json", # También checaremos dimCatalogAvailability.xls
    "FactPayment.xls": "FactPayment.json",
    "FactStreamingEvent.xls": "FactStreamingEvent.json",
    "FactSuscription.xls": "FactSuscription.json"
}

print("\n=== CONVIRTIENDO 16 EXCELS DIRECTAMENTE A 16 COLECCIONES ===")
pwd = os.path.dirname(__file__) if '__file__' in globals() else os.getcwd()
json_dir = os.path.join(pwd, 'json')
os.makedirs(json_dir, exist_ok=True)

# ---------------------------------------------------------
# 3. PROCESAMIENTO Y EXPORTACIÓN 1 a 1
# ---------------------------------------------------------
for excel_file, json_file in ARCHIVOS.items():
    print(f"Procesando: {excel_file} -> {json_file}...")
    df = load_data(excel_file)
    
    # Manejo especial por si el nombre es dimCatalogAvailability
    if df.empty and excel_file == "FacCatalogAvailability.xls":
        print(f"  -> Intentando cargar dimCatalogAvailability.xls alternativo...")
        df = load_data("dimCatalogAvailability.xls")
        
    if not df.empty:
        # Convertir a diccionario de Python (eliminando columnas completamente vacías)
        lista_dicts = df.dropna(axis=1, how='all').to_dict(orient='records')
        
        # Limpiar los NaNs restantes
        lista_limpia = clean_nan(lista_dicts)
        
        # Exportar a JSON
        out_path = os.path.join(json_dir, json_file)
        with open(out_path, 'w', encoding='utf-8') as f:
            json.dump(lista_limpia, f, indent=4, ensure_ascii=False, default=str)
        print(f"  ✅ Generado {json_file} con {len(lista_limpia)} documentos.")
    else:
        print(f"  ❌ Omitiendo {excel_file} porque está vacío o no existe.")

print("\n🚀 ¡PROCESO FINALIZADO! Se han generado las 16 colecciones limpias en la carpeta 'json' 🚀")
