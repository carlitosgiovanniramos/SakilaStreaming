import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Configuración estética idéntica a tu ejemplo
sns.set_theme(style="whitegrid")
plt.rcParams.update({
    'font.size': 10, 
    'axes.labelsize': 11, 
    'axes.titlesize': 12,
    'figure.max_open_warning': 50
})

carpeta_datos = "BD normalizada"
carpeta_salida = "Reporte_ScatterPlots"

if not os.path.exists(carpeta_salida):
    os.makedirs(carpeta_salida)

# 2. Configuración manual de los cruces de variables lógicos por Datamart
# Formato: 'Archivo.csv': (Variable_X, Variable_Y, Variable_Color/Categoría)
config_scatter = {
    "Datamart_Churn.csv": {
        "x": "AvgSessionMinutes", 
        "y": "LifetimeValue", 
        "hue": "CustomerSegment",
        "title": "Relación entre Minutos de Sesión y Valor de Vida del Cliente"
    },
    "Datamart_Contenido.csv": {
        "x": "Budget", 
        "y": "Revenue", 
        "hue": "RatingCategory",
        "title": "Correlación entre Presupuesto e Ingresos por Contenido"
    },
    "Datamart_Geografia.csv": {
        "x": "CustomerCount", 
        "y": "TotalLifetimeValue", 
        "hue": "Continent",
        "title": "Distribución Geográfica: Volumen de Clientes vs Ingresos Totales"
    },
    "Datamart_Pagos.csv": {
        "x": "DaysToNextExpiry", 
        "y": "LifetimeValue", 
        "hue": "PaymentTrustSegment",
        "title": "Lifetime Value vs Días de Vencimiento de Tarjeta"
    },
    "Datamart_Talento.csv": {
        "x": "popularity_score", 
        "y": "TotalRevenue", 
        "hue": "TalentTier",
        "title": "Popularidad del Talento frente a los Ingresos Totales Generados"
    }
}

print("🚀 Iniciando generación de Gráficos de Dispersión (Scatter Plots)...\n")

# 3. Bucle de procesamiento y construcción de gráficos
for archivo, params in config_scatter.items():
    # Buscar el archivo en la raíz o en BD normalizada
    if os.path.exists(archivo):
        ruta_completa = archivo
    elif os.path.exists(os.path.join(carpeta_datos, archivo)):
        ruta_completa = os.path.join(carpeta_datos, archivo)
    else:
        # Intento con minúsculas
        archivo_min = archivo.lower()
        if os.path.exists(carpeta_datos):
            match = [f for f in os.listdir(carpeta_datos) if f.lower() == archivo_min]
            if match:
                ruta_completa = os.path.join(carpeta_datos, match[0])
            else:
                print(f"❌ No se encontró el archivo: {archivo}")
                continue
        else:
            print(f"❌ No se encontró el archivo: {archivo}")
            continue

    # Cargar datos
    df = pd.read_csv(ruta_completa)
    
    # Validar que las 3 columnas configuradas existan en el archivo
    if params["x"] in df.columns and params["y"] in df.columns and params["hue"] in df.columns:
        print(f"📊 Generando Scatter Plot para: {os.path.basename(ruta_completa)}...")
        
        plt.figure(figsize=(10, 6))
        
        # Crear gráfico de dispersión con transparencia (alpha) para evitar el solapamiento masivo de puntos
        sns.scatterplot(
            data=df, 
            x=params["x"], 
            y=params["y"], 
            hue=params["hue"], 
            palette="viridis",  # Paleta de colores profesional y vistosa
            alpha=0.7, 
            edgecolor=None
        )
        
        # Personalización de títulos y etiquetas
        plt.title(params["title"], fontsize=13, fontweight='bold', pad=15)
        plt.xlabel(params["x"], fontsize=11)
        plt.ylabel(params["y"], fontsize=11)
        
        # Mover la leyenda/label de categorías fuera del gráfico para que no tape los puntos
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', title=params["hue"])
        
        plt.tight_layout()
        
        # Guardar imagen PNG
        nombre_salida = archivo.replace('.csv', '_scatter_dispersion.png')
        ruta_guardado = os.path.join(carpeta_salida, nombre_salida)
        plt.savefig(ruta_guardado, dpi=140, bbox_inches='tight')
        plt.close()
        
        print(f"💾 Guardado en: {ruta_guardado}\n")
    else:
        print(f"⚠️ Saltando {archivo}: No tiene todas las columnas requeridas ({params['x']}, {params['y']}, {params['hue']}).")

print("✨ ¡Proceso completado! Tus 5 gráficos de dispersión están listos en la carpeta 'Reporte_ScatterPlots'.")