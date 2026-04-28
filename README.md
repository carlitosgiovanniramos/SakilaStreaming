# 📊 PIPELINE COMPLETO - SAKILA STREAMING (BI + ML)

## 📋 Descripción General

Este proyecto implementa un **pipeline completo de Inteligencia de Negocios** que integra:

1. ✅ **Data Mart ETL** - Lectura de 16 archivos Excel
2. ✅ **JSON Generation** - Transformación a formato JSON con embedding de documentos
3. ✅ **MongoDB Storage** - Almacenamiento en 3 colecciones normalizadas
4. ✅ **SQL-like Queries** - Consultas complejas con agregaciones
5. ⏳ **Clustering** - Segmentación de usuarios con K-Means (NEW)
6. ⏳ **Recommendation System** - Sugerencias de contenido con Collaborative Filtering (NEW)

---

## 🎯 Componentes Principales

### **1. Clustering Analysis (`clustering_analysis.py`)**

**¿Qué hace?**
- Divide a los 100 usuarios en **3 segmentos** basados en su comportamiento de streaming

**Algoritmo:**
- **K-Means** con k=3 clusters
- Features: `total_eventos`, `total_minutos`, `promedio_minutos`
- Normalización: StandardScaler (escala los datos a media=0, desviación=1)

**Salidas:**
```
Cluster 0: "Power Users"      (32 usuarios) - Máxima actividad
Cluster 1: "Regular Users"    (35 usuarios) - Actividad media
Cluster 2: "Dormant Users"    (33 usuarios) - Mínima actividad
```

**Archivos generados:**
- `resultados_clustering/clustering_results.csv` - Tabla con asignación de cluster por usuario
- `resultados_clustering/clustering_analysis.png` - Gráficos (scatter plot + histograma)

**¿Para qué sirve en la tarea?**
- Identificar segmentos de clientes para estrategia de retención
- Detectar usuarios en riesgo (Dormant Users)
- Personalizar oferta de contenido por cluster

---

### **2. Recommendation System (`recommendation_system.py`)**

**¿Qué hace?**
- Sugiere **TOP-10 películas** a cada usuario basándose en usuarios similares

**Algoritmo: Collaborative Filtering**
```
PASO 1: Crear matriz Usuario x Contenido
        - Filas = 100 usuarios
        - Columnas = 100 películas/series
        - Valores = minutos visualizados (proxy de "gusto")

PASO 2: Normalizar matriz con StandardScaler

PASO 3: Calcular similitud usuario-usuario
        - Cosine Similarity entre cada par de usuarios
        - Resultado: matriz 100x100 con valores 0-1
        - 1.0 = usuarios idénticos, 0.0 = diferentes

PASO 4: Para cada usuario, generar recomendaciones
        a) Encuentrar 5 usuarios más similares a él
        b) Ver qué películas vieron esos 5
        c) Que él NO haya visto
        d) Ponderar por similitud y ordenar
        e) Retornar TOP-10
```

**Ejemplo:**
```
Usuario 1502 es similar a: 1510, 1515, 1520, 1530, 1540
  ↓
Ellos vieron: [Avatar, Titanic, Inception, Gladiator, Interstellar]
Usuario 1502 ya vio: [Avatar, Inception]
  ↓
RECOMENDACIONES PARA 1502:
  1. Titanic (score: 67.23 por usuarios similares lo amaron)
  2. Gladiator (score: 61.45)
  3. Interstellar (score: 59.87)
  ...
  10. [película 10 con menor score]
```

**Archivos generados:**
- `resultados_recomendaciones/recomendaciones_top10.csv` - Top-10 recomendaciones por usuario
- `resultados_recomendaciones/recomendacion_analysis.png` - Heatmap de similitud + distribución de scores

**¿Para qué sirve en la tarea?**
- Aumentar engagement (usuarios descubren películas que les van a gustar)
- Aumentar retención (mejor experiencia = más renovaciones)
- Aumentar ingresos (más visualizaciones = más valor)
- Demostrar técnicas de ML: filtrado colaborativo + similitud

---

## 🚀 Instalación y Ejecución

### **Requisitos previos:**
```bash
# MongoDB debe estar corriendo en localhost:27017
# Base de datos: SakilaStreaming
# Colecciones: customer, streamingevent, catalog

# Librerías Python:
pip install pymongo pandas numpy scikit-learn matplotlib
```

### **Ejecución completa (TODO EN UNA LÍNEA):**
```bash
cd "c:\Users\Lenovo LOQ\Desktop\UTA\6 Software\Inteligencia de Negocios\Unidad 2\Clase2Semana6\SakilaStreaming"

# Opción 1: Ejecutar el pipeline completo
python main_pipeline.py

# Opción 2: Ejecutar componentes individuales
python clustering_analysis.py
python recommendation_system.py
```

---

## 📊 Resultados Esperados

### **Clustering:**
```
✅ 100 usuarios segmentados en 3 clusters
✅ Archivos CSV + PNG generados automáticamente
✅ Estadísticas por cluster (eventos, minutos, promedios)
```

### **Recomendaciones:**
```
✅ 100 usuarios con TOP-10 recomendaciones
✅ Matriz de similitud usuario-usuario visualizada
✅ Distribución de scores de recomendación analizada
```

---

## 📈 Estructura de Datos

### **MongoDB - Colención: customer**
```json
{
  "Key": 1501,
  "ID": "e9d1fd0a...",
  "First Name": "Tracy",
  "Last Name": "Reed",
  "Suscripciones_Historial": [...],
  "Pagos_Historial": [...]
}
```

### **MongoDB - Colección: streamingevent**
```json
{
  "Stream Event Key": 1,
  "Date Key": -1190737705,
  "Customer Key": 1551,
  "Content Key": 1547,
  "Streams Count": 8,
  "Minutes Watched": 102,
  "Completion Flag": false
}
```

### **MongoDB - Colección: catalog**
```json
{
  "Key": 1501,
  "Title": "Forrest Gump",
  "Categorias_Array": [{"Name": "Comedy"}],
  "Talento_Array": [{"First Name": "Tom", "Last Name": "Hanks"}]
}
```

---

## 🔧 Detalles Técnicos

### **Clustering:**
| Componente | Valor |
|-----------|-------|
| Algoritmo | K-Means |
| Clusters | 3 |
| Features | total_eventos, total_minutos, promedio_minutos |
| Normalización | StandardScaler |
| Random State | 42 (reproducibilidad) |

### **Recomendación:**
| Componente | Valor |
|-----------|-------|
| Método | Collaborative Filtering |
| Similitud | Cosine Similarity |
| Usuarios similares | Top-5 |
| Recomendaciones | Top-10 |
| Matriz | 100 usuarios x 100 contenidos |
| Métrica | Minutos Visualizados |

---

## 📝 Código Comentado

### **Secciones del código:**

**Clustering:**
- ✅ Paso 1: Conexión a MongoDB
- ✅ Paso 2: Extracción de features
- ✅ Paso 3: Normalización StandardScaler
- ✅ Paso 4: K-Means fit_predict
- ✅ Paso 5: Análisis de características por cluster
- ✅ Paso 6: Visualización scatter plot
- ✅ Paso 7: Visualización histograma
- ✅ Paso 8: Estadísticas detalladas
- ✅ Paso 9: Export a CSV
- ✅ Paso 10: Resumen final

**Recomendación:**
- ✅ Paso 1: Conexión a MongoDB
- ✅ Paso 2: Crear matriz usuario-contenido
- ✅ Paso 3: Normalizar matriz
- ✅ Paso 4: Calcular Cosine Similarity
- ✅ Paso 5: Generar recomendaciones (Collaborative Filtering)
- ✅ Paso 6: Formattear results
- ✅ Paso 7: Export a CSV
- ✅ Paso 8: Visualización 1 (Heatmap de similitud)
- ✅ Paso 9: Visualización 2 (Distribución de scores)
- ✅ Paso 10: Resumen final

---

## 🎓 Conceptos de BI Implementados

### **Data Mart:**
- Arquitectura dimensional (Dim*)
- Hechos (Fact*)
- Normalización de claves

### **ETL:**
- Lectura de múltiples fuentes (Excel)
- Transformación (normalización, embeding)
- Carga (MongoDB)

### **Consultas Analíticas:**
- Agregaciones MongoDB ($group, $lookup)
- DataFrames (pandas)
- Visualizaciones (matplotlib)

### **Machine Learning:**
- **Clustering**: Segmentación no supervisada (K-Means)
- **Recomendación**: Filtrado colaborativo (Cosine Similarity)
- **Normalización**: Preprocesamiento de datos (StandardScaler)

---

## 🖼️ Visualizaciones Generadas

### **Clustering Analysis:**
- **Scatter Plot**: Total Eventos vs Total Minutos (coloreado por cluster)
- **Histograma**: Distribución de usuarios por cluster

### **Recommendation Analysis:**
- **Heatmap**: Matriz de similitud usuario-usuario (primeros 20)
- **Histograma**: Distribución de puntuaciones de recomendación

---

## 📁 Estructura de Archivos

```
SakilaStreaming/
├── clustering_analysis.py              # Script de clustering (NUEVO)
├── recommendation_system.py            # Script de recomendaciones (NUEVO)
├── main_pipeline.py                    # Pipeline orchestrator (NUEVO)
├── README.md                           # Este archivo
├── Sakila_Streaming_Json.py            # ETL (existente)
├── clientes.py                         # Queries (existente)
├── conexion.py                         # Template de conexión
│
├── resultados_clustering/
│   ├── clustering_results.csv          # Datos de clusters
│   └── clustering_analysis.png         # Gráficos
│
├── resultados_recomendaciones/
│   ├── recomendaciones_top10.csv       # Top-10 recomendaciones
│   └── recomendacion_analysis.png      # Gráficos
│
└── resultados_consultas/               # Outputs de queries (existente)
    ├── resultado1_*.csv
    ├── resultado2_*.csv
    ├── grafico_resultado1_*.png
    └── grafico_resultado2_*.png
```

---

## ✅ Checklist para tu Presentación

- [ ] Explicar qué es Clustering (segmentación de usuarios)
- [ ] Mostrar resultados: 3 clusters con 32, 35, 33 usuarios
- [ ] Explicar qué es el Sistema de Recomendación (sugerir películas)
- [ ] Mostrar Top-10 recomendaciones por usuario
- [ ] Ejecutar `python main_pipeline.py` en vivo
- [ ] Mostrar gráficos generados (PNG)
- [ ] Mostrar CSVs con datos
- [ ] Explicar el algoritmo: K-Means + Collaborative Filtering
- [ ] Mencionar beneficios: retención, engagement, ingresos

---

## 📚 Referencias Técnicas

**K-Means Clustering:**
- Algoritmo no supervisado
- Divide datos en k grupos minimizando varianza dentro de clusters
- Requiere normalización previa

**Collaborative Filtering:**
- Recomendación basada en usuarios similares
- Asume: "si dos usuarios compartimos gustos, nos van a gustar cosas iguales"
- Escalable a millones de usuarios/items

**Cosine Similarity:**
- Mide ángulo entre vectores (0-1)
- 1 = vectores idénticos, 0 = ortogonales
- Ignora magnitud, enfocado en dirección (perfecto para preferencias)

---

## 🔗 Próximos Pasos (Opcional)

- [ ] Mejorar recomendación con Content-Based Filtering (películas similares)
- [ ] Implementar Hybrid Recommendation (combinar ambos métodos)
- [ ] Agregar validación de modelo (precisión, recall, F1-score)
- [ ] Entrenar modelo en nueva data y validar comportamiento
- [ ] Crear API REST para servir recomendaciones en tiempo real

---

## ❓ Preguntas Frecuentes

**P: ¿Qué significa Cluster en mi caso?**
R: Un grupo de usuarios con comportamiento similar. Ej: Power Users = muchas visualizaciones.

**P: ¿Por qué Collaborative Filtering?**
R: Es el método más usado en Netflix, Amazon, Spotify. Si tú y yo vimos las mismas pelis, probablemente nos van a gustar cosas similares.

**P: ¿Qué es Cosine Similarity?**
R: Una forma de medir qué tan similares son dos usuarios (0-1). 1 = idénticos.

**P: ¿Puedo cambiar k a 4 clusters?**
R: Sí, cambia `n_clusters=3` a `n_clusters=4` en clustering_analysis.py línea ~135.

**P: ¿Puedo cambiar Top-10 a Top-20 recomendaciones?**
R: Sí, cambia `[:10]` a `[:20]` en recommendation_system.py línea ~239.

---

## 📞 Soporte

Si tienes preguntas sobre el código, revisa los comentarios en:
- `clustering_analysis.py` - Líneas 1-30 (descripción)
- `recommendation_system.py` - Líneas 1-30 (descripción)

Cada sección está comentada paso a paso en español.

---

**Creado:** 5 de Abril, 2026
**Status:** ✅ COMPLETO Y FUNCIONAL
**Para:** Inteligencia de Negocios - UTA
#   S a k i l a S t r e a m i n g  
 