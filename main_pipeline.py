# ============================================================================
# PIPELINE PRINCIPAL - SAKILA STREAMING
# ============================================================================
# PROPÓSITO: Ejecutar todo el proceso de análisis en una línea
#            Integra: Clustering + Recommendation System + Visualizaciones
#
# EJECUCIÓN: python main_pipeline.py
# ============================================================================

import os
import sys
import subprocess
import time

# ============================================================================
# PASO 0: CREAR DIRECTORIOS DE SALIDA
# ============================================================================
print("\n" + "="*80)
print("PIPELINE COMPLETO - SAKILA STREAMING")
print("="*80)
print("\n📁 Preparando directorios...")

directorios = ['resultados_clustering', 'resultados_recomendaciones']
for directorio in directorios:
    if not os.path.exists(directorio):
        os.makedirs(directorio)
        print(f"✅ Creado: {directorio}/")
    else:
        print(f"✅ Existe: {directorio}/")

# ============================================================================
# PASO 1: EJECUTAR CLUSTERING
# ============================================================================
print("\n" + "-"*80)
print("FASE 1: ANÁLISIS DE CLUSTERING")
print("-"*80)

print("\n🚀 Ejecutando clustering_analysis.py...")
tiempo_inicio_clustering = time.time()

try:
    resultado_clustering = subprocess.run(
        ['python', 'clustering_analysis.py'],
        capture_output=False,
        text=True,
        check=True
    )
    
    tiempo_clustering = time.time() - tiempo_inicio_clustering
    print(f"\n✅ Clustering completado en {tiempo_clustering:.2f} segundos")
    
except subprocess.CalledProcessError as e:
    print(f"❌ Error en clustering: {e}")
    sys.exit(1)

# ============================================================================
# PASO 2: EJECUTAR SISTEMA DE RECOMENDACIÓN
# ============================================================================
print("\n" + "-"*80)
print("FASE 2: SISTEMA DE RECOMENDACIÓN")
print("-"*80)

print("\n🚀 Ejecutando recommendation_system.py...")
tiempo_inicio_recomendaciones = time.time()

try:
    resultado_recomendaciones = subprocess.run(
        ['python', 'recommendation_system.py'],
        capture_output=False,
        text=True,
        check=True
    )
    
    tiempo_recomendaciones = time.time() - tiempo_inicio_recomendaciones
    print(f"\n✅ Recomendaciones completadas en {tiempo_recomendaciones:.2f} segundos")
    
except subprocess.CalledProcessError as e:
    print(f"❌ Error en recomendaciones: {e}")
    sys.exit(1)

# ============================================================================
# PASO 3: RESUMEN FINAL
# ============================================================================
print("\n" + "="*80)
print("✅ PIPELINE COMPLETADO CON ÉXITO")
print("="*80)

tiempo_total = tiempo_clustering + tiempo_recomendaciones

print(f"\n⏱️  TIEMPOS DE EJECUCIÓN:")
print(f"   Clustering:      {tiempo_clustering:.2f}s")
print(f"   Recomendaciones: {tiempo_recomendaciones:.2f}s")
print(f"   TOTAL:           {tiempo_total:.2f}s")

print(f"\n📁 ARCHIVOS GENERADOS:")
print(f"\n   📊 CLUSTERING:")
print(f"      - resultados_clustering/clustering_results.csv")
print(f"      - resultados_clustering/clustering_analysis.png")

print(f"\n   💡 RECOMENDACIONES:")
print(f"      - resultados_recomendaciones/recomendaciones_top10.csv")
print(f"      - resultados_recomendaciones/recomendaciones_item_item_top10.csv")
print(f"      - resultados_recomendaciones/recomendaciones_contenido_tokenizacion_top10.csv")
print(f"      - resultados_recomendaciones/comparacion_sistemas.csv")
print(f"      - resultados_recomendaciones/recomendacion_analysis.png")
print(f"      - resultados_recomendaciones/recomendacion_item_item.png")
print(f"      - resultados_recomendaciones/recomendacion_contenido_tokenizacion.png")

print(f"\n" + "="*80)
print("🎉 ¡PIPELINE LISTO PARA TU PRESENTACIÓN!")
print("="*80 + "\n")
