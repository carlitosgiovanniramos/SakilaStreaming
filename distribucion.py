import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Configuración estética profesional para tus reportes de BI
sns.set_theme(style="whitegrid")
plt.rcParams.update({
    'font.size': 10, 
    'axes.labelsize': 11, 
    'axes.titlesize': 12,
    'figure.max_open_warning': 100
})

# Definimos las posibles ubicaciones de tus archivos
carpetas_a_buscar = [".", "BD normalizada"]
carpeta_salida = "Reporte_Graficos"

if not os.path.exists(carpeta_salida):
    os.makedirs(carpeta_salida)
    print(f"📁 Carpeta de destino creada: '{carpeta_salida}'")

# Listado de los 5 datamarts objetivo
datamarts_objetivo = [
    'Datamart_Churn.csv',
    'Datamart_Contenido.csv',
    'Datamart_Geografia.csv',
    'Datamart_Pagos.csv',
    'Datamart_Talento.csv'
]

print("⚡ Iniciando escaneo inteligente de Datamarts...\n")

# 2. Bucle de procesamiento 100% dinámico
for archivo in datamarts_objetivo:
    ruta_encontrada = None
    
    # Buscar el archivo en las ubicaciones configuradas
    for carpeta in carpetas_a_buscar:
        ruta_intento = os.path.join(carpeta, archivo)
        if os.path.exists(ruta_intento):
            ruta_encontrada = ruta_intento
            break
            
    if ruta_encontrada is None:
        # Intento de búsqueda insensible a mayúsculas/minúsculas si falló la exacta
        for carpeta in carpetas_a_buscar:
            if os.path.exists(carpeta):
                for f_real in os.listdir(carpeta):
                    if f_real.lower() == archivo.lower():
                        ruta_encontrada = os.path.join(carpeta, f_real)
                        break
                        
    if not ruta_encontrada:
        print(f"❌ No se encontró el archivo '{archivo}' en la raíz ni en 'BD normalizada'.")
        continue

    # Leer el dataset encontrado
    df = pd.read_csv(ruta_encontrada)
    
    # --- AUTO-DETECCIÓN INTELIGENTE DE COLUMNAS ---
    cols_para_graficar = []
    for col in df.select_dtypes(include=['int64', 'float64']).columns:
        col_lower = col.lower()
        
        # 1. Ignorar llaves, IDs y coordenadas (No aportan valor en dispersión global)
        if 'key' in col_lower or 'id' in col_lower or 'latitude' in col_lower or 'longitude' in col_lower:
            continue
            
        # 2. Ignorar banderas binarias estrictas (Valores que solo son 0 o 1)
        datos_unicos = df[col].dropna().unique()
        if len(datos_unicos) <= 2 and set(datos_unicos).issubset({0, 1, 0.0, 1.0}):
            continue
            
        cols_para_graficar.append(col)
        
    # Limitamos a un máximo de 10 columnas por reporte para evitar imágenes gigantescas
    if len(cols_para_graficar) > 10:
        cols_para_graficar = cols_para_graficar[:10]
        
    if not cols_para_graficar:
        print(f"⚠️ {archivo} no tiene columnas numéricas continuas válidas para analizar.")
        continue
        
    print(f"📊 Graficando: {os.path.basename(ruta_encontrada)}")
    print(f"   -> Columnas detectadas: {cols_para_graficar}")
    
    # Configurar el lienzo dinámico (N variables filas x 2 columnas de gráficos)
    fig, axes = plt.subplots(len(cols_para_graficar), 2, figsize=(15, 3.8 * len(cols_para_graficar)))
    if len(cols_para_graficar) == 1:
        axes = [axes]
        
    for i, col in enumerate(cols_para_graficar):
        data_clean = df[col].dropna()
        # Ajustar los "bins" si la variable es discreta o continua
        bins_param = len(data_clean.unique()) if data_clean.nunique() <= 10 else 'auto'
        
        # Gráfico Izquierdo: Distribución de Frecuencias (Histograma + KDE)
        sns.histplot(x=data_clean, kde=True, ax=axes[i][0], color='#4C72B0', bins=bins_param)
        axes[i][0].set_title(f'Distribución de {col}')
        axes[i][0].set_ylabel('Frecuencia')
        axes[i][0].set_xlabel('')
        
        # Gráfico Derecho: Dispersión y Outliers (Boxplot)
        sns.boxplot(x=data_clean, ax=axes[i][1], color='#DD8452', 
                    flierprops=dict(markerfacecolor='red', marker='o', alpha=0.5, markersize=4))
        axes[i][1].set_title(f'Dispersión y Outliers de {col}')
        axes[i][1].set_xlabel('')
        
    # Estructuración de títulos y guardado
    nombre_base = os.path.basename(ruta_encontrada)
    plt.suptitle(f"ANÁLISIS DE DATAMART: {nombre_base.upper()}", y=1.01, fontsize=14, fontweight='bold', color='#222222')
    plt.tight_layout()
    
    nombre_salida = nombre_base.replace('.csv', '_analisis.png')
    ruta_guardado = os.path.join(carpeta_salida, nombre_salida)
    
    plt.savefig(ruta_guardado, dpi=140, bbox_inches='tight')
    plt.close()
    print(f"💾 Guardado con éxito en: {ruta_guardado}\n")

print("✨ ¡Excelente! Proceso finalizado. Ve a revisar tu carpeta 'Reporte_Graficos'.")