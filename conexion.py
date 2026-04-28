from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path


MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "SakilaStreaming"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]


EXPORT_DIR = Path("resultados_consultas")
EXPORT_DIR.mkdir(exist_ok=True)
MOSTRAR_GRAFICOS = True  # Cambiar a False para no mostrar gráficos al generar


def pick_collection(database, candidates):
	"""Selecciona la primera colección existente de una lista de candidatos."""
	existing = set(database.list_collection_names())
	for name in candidates:
		if name in existing:
			return database[name]
	# Si no existe ninguna aún, retorna el primer nombre para que falle de forma clara al consultar.
	return database[candidates[0]]


def mostrar_resultado(titulo, docs, max_rows=15):
	print("\n" + "=" * 120)
	print(titulo)
	print("=" * 120)

	if not docs:
		print("Sin resultados para esta consulta.")
		return

	df = pd.json_normalize(docs)
	if len(df) > max_rows:
		print(df.head(max_rows).to_string(index=False))
		print(f"\n... mostrando {max_rows} de {len(df)} filas")
	else:
		print(df.to_string(index=False))


def to_dataframe(docs):
	if not docs:
		return pd.DataFrame()
	return pd.json_normalize(docs, sep=".")


def exportar_dataframe(df, nombre):
	df_export = df.copy()
	if df_export.empty:
		df_export = pd.DataFrame([{"Nota": "Sin resultados para esta consulta"}])
	df_export.to_csv(EXPORT_DIR / f"{nombre}.csv", index=False, encoding="utf-8-sig")


def guardar_figura(fig, nombre_archivo):
	fig.tight_layout()
	fig.savefig(EXPORT_DIR / nombre_archivo, dpi=140, bbox_inches="tight")
	if MOSTRAR_GRAFICOS:
		plt.show()
	plt.close(fig)


def get_event_date_stages():
	"""Genera campos de fecha legibles a partir de Date Key (timestamp en segundos con signo invertido)."""
	return [
		{"$addFields": {"DateKeyAbs": {"$abs": "$Date Key"}}},
		{"$addFields": {"EventDate": {"$toDate": {"$multiply": ["$DateKeyAbs", 1000]}}}},
		{"$addFields": {"EventYear": {"$year": "$EventDate"}, "EventMonth": {"$month": "$EventDate"}}},
	]


def obtener_periodo_eventos(eventos_col):
	date_keys = [k for k in eventos_col.distinct("Date Key") if isinstance(k, (int, float))]
	if not date_keys:
		return None, None

	fechas = pd.to_datetime(pd.Series(date_keys).abs(), unit="s", origin="unix", errors="coerce").dropna()
	if fechas.empty:
		return None, None

	return fechas.min(), fechas.max()


def obtener_area_mas_activa(eventos_col, clientes_col_name):
	pipeline = [
		{
			"$lookup": {
				"from": clientes_col_name,
				"localField": "Customer Key",
				"foreignField": "Key",
				"as": "Cliente",
			}
		},
		{"$unwind": "$Cliente"},
		{
			"$group": {
				"_id": {
					"Pais": "$Cliente.Ubicacion.Country",
					"Ciudad": "$Cliente.Ubicacion.City",
				},
				"Total_Eventos": {"$sum": 1},
			}
		},
		{"$sort": {"Total_Eventos": -1}},
		{"$limit": 1},
	]

	res = list(eventos_col.aggregate(pipeline))
	if not res:
		return None, None

	return res[0]["_id"].get("Pais"), res[0]["_id"].get("Ciudad")


def obtener_actor_mas_frecuente(catalogo_col):
	pipeline = [
		{"$unwind": {"path": "$Talento_Array", "preserveNullAndEmptyArrays": False}},
		{
			"$project": {
				"Actor": {
					"$trim": {
						"input": {
							"$concat": [
								{"$ifNull": ["$Talento_Array.First Name", ""]},
								" ",
								{"$ifNull": ["$Talento_Array.Last Name", ""]},
							]
						}
					}
				}
			}
		},
		{"$match": {"Actor": {"$ne": ""}}},
		{"$group": {"_id": "$Actor", "Frecuencia": {"$sum": 1}}},
		{"$sort": {"Frecuencia": -1}},
		{"$limit": 1},
	]

	res = list(catalogo_col.aggregate(pipeline))
	if not res:
		return None
	return res[0]["_id"]


# Parametros opcionales para las nuevas consultas (si quedan en None, se auto-resuelven)
CONSULTA_6_PAIS = None
CONSULTA_6_CIUDAD = None
CONSULTA_6_FECHA_INICIO = None
CONSULTA_6_FECHA_FIN = None

CONSULTA_7_ACTOR = None

CONSULTA_8_PAIS = None
CONSULTA_8_ANIO = None


# Colecciones (nombres robustos por si cambian al importar JSON)
clientes_collection = pick_collection(db, ["clientes", "customer", "Customers", "customer_dim"])
catalogo_collection = pick_collection(db, ["catalogo", "catalog", "Catalog", "content"])
eventos_collection = pick_collection(
	db,
	[
		"eventos_streaming",
		"streamingevent",
		"streaming_events",
		"FactStreamingEvent",
		"fact_streaming",
	],
)


# ---------------------------------------------------------------------------------
# Consulta 1
# ¿Qué suscriptores activos no registraron eventos de streaming en las últimas
# 30 fechas registradas en el sistema?
# ---------------------------------------------------------------------------------
date_keys = [k for k in eventos_collection.distinct("Date Key") if k is not None]
recent_30_keys = sorted(date_keys, reverse=True)[:30]

pipeline_1 = [
	{"$match": {"Suscripciones_Historial": {"$elemMatch": {"Is Active": True}}}},
	{
		"$lookup": {
			"from": eventos_collection.name,
			"let": {"cust_key": "$Key"},
			"pipeline": [
				{"$match": {"$expr": {"$eq": ["$Customer Key", "$$cust_key"]}}},
				{"$match": {"Date Key": {"$in": recent_30_keys}}},
				{"$limit": 1},
			],
			"as": "Eventos_Recientes",
		}
	},
	{"$match": {"Eventos_Recientes": {"$size": 0}}},
	{
		"$project": {
			"_id": 0,
			"Customer Key": "$Key",
			"Nombre": {"$concat": ["$First Name", " ", "$Last Name"]},
			"Email": "$Email",
			"Pais": "$Ubicacion.Country",
			"Ciudad": "$Ubicacion.City",
		}
	},
	{"$sort": {"Pais": 1, "Ciudad": 1, "Nombre": 1}},
]
resultado1 = list(clientes_collection.aggregate(pipeline_1))


# ---------------------------------------------------------------------------------
# Consulta 2
# ¿Qué contenidos generan mayor ingreso total en relación con su volumen de
# reproducciones? (Índice de rentabilidad aproximado)
# ---------------------------------------------------------------------------------
pipeline_2 = [
	{
		"$group": {
			"_id": "$Content Key",
			"Total_Streams": {"$sum": "$Streams Count"},
			"Total_Minutos": {"$sum": "$Minutes Watched"},
			"Total_Eventos": {"$sum": 1},
		}
	},
	{
		"$lookup": {
			"from": catalogo_collection.name,
			"localField": "_id",
			"foreignField": "Key",
			"as": "Contenido",
		}
	},
	{"$unwind": "$Contenido"},
	{
		"$project": {
			"_id": 0,
			"Content Key": "$_id",
			"Titulo": "$Contenido.Title",
			"Revenue": "$Contenido.Revenue",
			"Total_Streams": 1,
			"Total_Minutos": 1,
			"Total_Eventos": 1,
			"Ingreso_por_Stream": {
				"$cond": [
					{"$gt": ["$Total_Streams", 0]},
					{"$divide": ["$Contenido.Revenue", "$Total_Streams"]},
					0,
				]
			},
		}
	},
	{"$sort": {"Ingreso_por_Stream": -1, "Total_Streams": -1}},
	{"$limit": 20},
]
resultado2 = list(eventos_collection.aggregate(pipeline_2))


# ---------------------------------------------------------------------------------
# Consulta 3
# ¿Qué categorías de contenido presentan mayor volumen de visualizaciones por país?
# ---------------------------------------------------------------------------------
pipeline_3 = [
	{
		"$lookup": {
			"from": clientes_collection.name,
			"localField": "Customer Key",
			"foreignField": "Key",
			"as": "Cliente",
		}
	},
	{"$unwind": "$Cliente"},
	{
		"$lookup": {
			"from": catalogo_collection.name,
			"localField": "Content Key",
			"foreignField": "Key",
			"as": "Contenido",
		}
	},
	{"$unwind": "$Contenido"},
	{"$unwind": "$Contenido.Categorias_Array"},
	{
		"$group": {
			"_id": {
				"Pais": "$Cliente.Ubicacion.Country",
				"Categoria": "$Contenido.Categorias_Array.Name",
			},
			"Total_Streams": {"$sum": "$Streams Count"},
			"Total_Minutos": {"$sum": "$Minutes Watched"},
			"Total_Eventos": {"$sum": 1},
		}
	},
	{
		"$project": {
			"_id": 0,
			"Pais": "$_id.Pais",
			"Categoria": "$_id.Categoria",
			"Total_Streams": 1,
			"Total_Minutos": 1,
			"Total_Eventos": 1,
		}
	},
	{"$sort": {"Total_Streams": -1, "Total_Minutos": -1}},
	{"$limit": 25},
]
resultado3 = list(eventos_collection.aggregate(pipeline_3))


# ---------------------------------------------------------------------------------
# Consulta 4
# ¿Qué suscriptores registran mayor número de eventos de streaming por servidor?
# ---------------------------------------------------------------------------------
pipeline_4 = [
	{
		"$group": {
			"_id": {"Customer Key": "$Customer Key", "Provider Key": "$Provider Key"},
			"Total_Eventos": {"$sum": 1},
			"Total_Streams": {"$sum": "$Streams Count"},
			"Total_Minutos": {"$sum": "$Minutes Watched"},
		}
	},
	{
		"$lookup": {
			"from": clientes_collection.name,
			"localField": "_id.Customer Key",
			"foreignField": "Key",
			"as": "Cliente",
		}
	},
	{"$unwind": {"path": "$Cliente", "preserveNullAndEmptyArrays": True}},
	{
		"$project": {
			"_id": 0,
			"Customer Key": "$_id.Customer Key",
			"Provider Key": "$_id.Provider Key",
			"Nombre": {
				"$trim": {
					"input": {
						"$concat": [
							{"$ifNull": ["$Cliente.First Name", ""]},
							" ",
							{"$ifNull": ["$Cliente.Last Name", ""]},
						]
					}
				}
			},
			"Pais": "$Cliente.Ubicacion.Country",
			"Total_Eventos": 1,
			"Total_Streams": 1,
			"Total_Minutos": 1,
		}
	},
	{"$sort": {"Total_Eventos": -1, "Total_Streams": -1}},
	{"$limit": 20},
]
resultado4 = list(eventos_collection.aggregate(pipeline_4))


# ---------------------------------------------------------------------------------
# Consulta 5
# ¿Qué ciudades generan mayor ingreso total proveniente de suscriptores?
# ---------------------------------------------------------------------------------
pipeline_5 = [
	{"$unwind": "$Pagos_Historial"},
	{
		"$group": {
			"_id": {
				"Pais": "$Ubicacion.Country",
				"Ciudad": "$Ubicacion.City",
			},
			"Ingreso_Total": {"$sum": "$Pagos_Historial.Amount"},
			"Total_Pagos": {"$sum": 1},
			"Suscriptores": {"$addToSet": "$Key"},
		}
	},
	{
		"$project": {
			"_id": 0,
			"Pais": "$_id.Pais",
			"Ciudad": "$_id.Ciudad",
			"Ingreso_Total": {"$round": ["$Ingreso_Total", 2]},
			"Total_Pagos": 1,
			"Cantidad_Suscriptores": {"$size": "$Suscriptores"},
		}
	},
	{"$sort": {"Ingreso_Total": -1, "Total_Pagos": -1}},
	{"$limit": 20},
]
resultado5 = list(clientes_collection.aggregate(pipeline_5))


# ---------------------------------------------------------------------------------
# Parametros resueltos para nuevas consultas
# ---------------------------------------------------------------------------------
periodo_min, periodo_max = obtener_periodo_eventos(eventos_collection)
pais_auto, ciudad_auto = obtener_area_mas_activa(eventos_collection, clientes_collection.name)

PAIS_Q6 = CONSULTA_6_PAIS or pais_auto
CIUDAD_Q6 = CONSULTA_6_CIUDAD or ciudad_auto

if CONSULTA_6_FECHA_INICIO:
	FECHA_INICIO_Q6 = pd.to_datetime(CONSULTA_6_FECHA_INICIO, errors="coerce")
else:
	FECHA_INICIO_Q6 = periodo_min

if CONSULTA_6_FECHA_FIN:
	FECHA_FIN_Q6 = pd.to_datetime(CONSULTA_6_FECHA_FIN, errors="coerce")
else:
	FECHA_FIN_Q6 = periodo_max

if FECHA_INICIO_Q6 is not None and pd.notna(FECHA_INICIO_Q6):
	FECHA_INICIO_Q6 = pd.to_datetime(FECHA_INICIO_Q6).to_pydatetime()
else:
	FECHA_INICIO_Q6 = None

if FECHA_FIN_Q6 is not None and pd.notna(FECHA_FIN_Q6):
	FECHA_FIN_Q6 = pd.to_datetime(FECHA_FIN_Q6).to_pydatetime()
else:
	FECHA_FIN_Q6 = None

ACTOR_Q7 = CONSULTA_7_ACTOR or obtener_actor_mas_frecuente(catalogo_collection)
PAIS_Q8 = CONSULTA_8_PAIS or PAIS_Q6
ANIO_Q8 = CONSULTA_8_ANIO or (FECHA_FIN_Q6.year if FECHA_FIN_Q6 is not None else 2007)


# ---------------------------------------------------------------------------------
# Consulta 6
# ¿Qué servidores registran mayor número de eventos de streaming por categoría,
# en una ciudad y país específicos durante un período determinado?
# ---------------------------------------------------------------------------------
match_q6 = {}
if PAIS_Q6:
	match_q6["Cliente.Ubicacion.Country"] = PAIS_Q6
if CIUDAD_Q6:
	match_q6["Cliente.Ubicacion.City"] = CIUDAD_Q6
if FECHA_INICIO_Q6 is not None and FECHA_FIN_Q6 is not None:
	match_q6["EventDate"] = {"$gte": FECHA_INICIO_Q6, "$lte": FECHA_FIN_Q6}

pipeline_6 = (
	get_event_date_stages()
	+ [
		{
			"$lookup": {
				"from": clientes_collection.name,
				"localField": "Customer Key",
				"foreignField": "Key",
				"as": "Cliente",
			}
		},
		{"$unwind": "$Cliente"},
		{"$match": match_q6},
		{
			"$lookup": {
				"from": catalogo_collection.name,
				"localField": "Content Key",
				"foreignField": "Key",
				"as": "Contenido",
			}
		},
		{"$unwind": {"path": "$Contenido", "preserveNullAndEmptyArrays": True}},
		{"$unwind": {"path": "$Contenido.Categorias_Array", "preserveNullAndEmptyArrays": True}},
		{
			"$group": {
				"_id": {
					"Provider Key": "$Provider Key",
					"Pais": "$Cliente.Ubicacion.Country",
					"Ciudad": "$Cliente.Ubicacion.City",
					"Categoria": {"$ifNull": ["$Contenido.Categorias_Array.Name", "Sin categoria"]},
					"Anio": "$EventYear",
					"Mes": "$EventMonth",
				},
				"Total_Eventos": {"$sum": 1},
				"Total_Streams": {"$sum": "$Streams Count"},
				"Total_Minutos": {"$sum": "$Minutes Watched"},
			}
		},
		{
			"$project": {
				"_id": 0,
				"Provider Key": "$_id.Provider Key",
				"Pais": "$_id.Pais",
				"Ciudad": "$_id.Ciudad",
				"Categoria": "$_id.Categoria",
				"Anio": "$_id.Anio",
				"Mes": "$_id.Mes",
				"Total_Eventos": 1,
				"Total_Streams": 1,
				"Total_Minutos": 1,
			}
		},
		{"$sort": {"Anio": 1, "Mes": 1, "Total_Eventos": -1}},
		{"$limit": 300},
	]
)
resultado6 = list(eventos_collection.aggregate(pipeline_6))


# ---------------------------------------------------------------------------------
# Consulta 7
# ¿Qué categorías de contenido registran mayor número de reproducciones
# para un actor específico en cada país?
# ---------------------------------------------------------------------------------
pipeline_7 = [
	{
		"$lookup": {
			"from": clientes_collection.name,
			"localField": "Customer Key",
			"foreignField": "Key",
			"as": "Cliente",
		}
	},
	{"$unwind": "$Cliente"},
	{
		"$lookup": {
			"from": catalogo_collection.name,
			"localField": "Content Key",
			"foreignField": "Key",
			"as": "Contenido",
		}
	},
	{"$unwind": "$Contenido"},
	{"$unwind": {"path": "$Contenido.Categorias_Array", "preserveNullAndEmptyArrays": True}},
	{"$unwind": {"path": "$Contenido.Talento_Array", "preserveNullAndEmptyArrays": True}},
	{
		"$addFields": {
			"Actor": {
				"$trim": {
					"input": {
						"$concat": [
							{"$ifNull": ["$Contenido.Talento_Array.First Name", ""]},
							" ",
							{"$ifNull": ["$Contenido.Talento_Array.Last Name", ""]},
						]
					}
				}
			}
		}
	},
	{"$match": {"Actor": ACTOR_Q7}},
	{
		"$group": {
			"_id": {
				"Pais": "$Cliente.Ubicacion.Country",
				"Categoria": {"$ifNull": ["$Contenido.Categorias_Array.Name", "Sin categoria"]},
				"Actor": "$Actor",
			},
			"Total_Streams": {"$sum": "$Streams Count"},
			"Total_Eventos": {"$sum": 1},
			"Total_Minutos": {"$sum": "$Minutes Watched"},
		}
	},
	{
		"$project": {
			"_id": 0,
			"Pais": "$_id.Pais",
			"Categoria": "$_id.Categoria",
			"Actor": "$_id.Actor",
			"Total_Streams": 1,
			"Total_Eventos": 1,
			"Total_Minutos": 1,
		}
	},
	{"$sort": {"Total_Streams": -1, "Total_Minutos": -1}},
	{"$limit": 50},
]
resultado7 = list(eventos_collection.aggregate(pipeline_7))


# ---------------------------------------------------------------------------------
# Consulta 8
# ¿Qué actores participan en las películas con mayor número de reproducciones
# en la plataforma para un país y año asignados?
# ---------------------------------------------------------------------------------
pipeline_8 = (
	get_event_date_stages()
	+ [
		{
			"$lookup": {
				"from": clientes_collection.name,
				"localField": "Customer Key",
				"foreignField": "Key",
				"as": "Cliente",
			}
		},
		{"$unwind": "$Cliente"},
		{"$match": {"Cliente.Ubicacion.Country": PAIS_Q8, "EventYear": ANIO_Q8}},
		{
			"$group": {
				"_id": "$Content Key",
				"Total_Streams": {"$sum": "$Streams Count"},
				"Total_Eventos": {"$sum": 1},
				"Total_Minutos": {"$sum": "$Minutes Watched"},
			}
		},
		{"$sort": {"Total_Streams": -1, "Total_Minutos": -1}},
		{"$limit": 20},
		{
			"$lookup": {
				"from": catalogo_collection.name,
				"localField": "_id",
				"foreignField": "Key",
				"as": "Contenido",
			}
		},
		{"$unwind": "$Contenido"},
		{"$unwind": {"path": "$Contenido.Talento_Array", "preserveNullAndEmptyArrays": True}},
		{
			"$addFields": {
				"Actor": {
					"$trim": {
						"input": {
							"$concat": [
								{"$ifNull": ["$Contenido.Talento_Array.First Name", ""]},
								" ",
								{"$ifNull": ["$Contenido.Talento_Array.Last Name", ""]},
							]
						}
					}
				}
			}
		},
		{"$match": {"Actor": {"$ne": ""}}},
		{
			"$group": {
				"_id": "$Actor",
				"Streams_Asociados": {"$sum": "$Total_Streams"},
				"Peliculas_Top": {"$addToSet": "$Contenido.Title"},
			}
		},
		{
			"$project": {
				"_id": 0,
				"Actor": "$_id",
				"Streams_Asociados": 1,
				"Cantidad_Peliculas_Top": {"$size": "$Peliculas_Top"},
				"Peliculas_Top": {"$slice": ["$Peliculas_Top", 5]},
			}
		},
		{"$sort": {"Streams_Asociados": -1, "Cantidad_Peliculas_Top": -1}},
		{"$limit": 25},
	]
)
resultado8 = list(eventos_collection.aggregate(pipeline_8))


# ---------------------------------------------------------------------------------
# Consulta 9
# ¿Cuáles son las películas con mayor número de reproducciones por mes y año?
# ---------------------------------------------------------------------------------
pipeline_9 = (
	get_event_date_stages()
	+ [
		{
			"$lookup": {
				"from": catalogo_collection.name,
				"localField": "Content Key",
				"foreignField": "Key",
				"as": "Contenido",
			}
		},
		{"$unwind": {"path": "$Contenido", "preserveNullAndEmptyArrays": True}},
		{
			"$group": {
				"_id": {
					"Anio": "$EventYear",
					"Mes": "$EventMonth",
					"Content Key": "$Content Key",
					"Titulo": {"$ifNull": ["$Contenido.Title", "Sin titulo"]},
				},
				"Total_Streams": {"$sum": "$Streams Count"},
				"Total_Eventos": {"$sum": 1},
				"Total_Minutos": {"$sum": "$Minutes Watched"},
			}
		},
		{
			"$project": {
				"_id": 0,
				"Anio": "$_id.Anio",
				"Mes": "$_id.Mes",
				"Content Key": "$_id.Content Key",
				"Titulo": "$_id.Titulo",
				"Total_Streams": 1,
				"Total_Eventos": 1,
				"Total_Minutos": 1,
			}
		},
		{"$sort": {"Anio": 1, "Mes": 1, "Total_Streams": -1}},
	]
)
resultado9_raw = list(eventos_collection.aggregate(pipeline_9))

if resultado9_raw:
	df_r9_raw = to_dataframe(resultado9_raw)
	if {"Anio", "Mes", "Total_Streams"}.issubset(df_r9_raw.columns):
		df_r9_raw["Anio"] = pd.to_numeric(df_r9_raw["Anio"], errors="coerce")
		df_r9_raw["Mes"] = pd.to_numeric(df_r9_raw["Mes"], errors="coerce")
		df_r9_raw["Total_Streams"] = pd.to_numeric(df_r9_raw["Total_Streams"], errors="coerce")
		df_r9_raw = df_r9_raw.dropna(subset=["Anio", "Mes", "Total_Streams"])
		df_r9_top = (
			df_r9_raw.sort_values(["Anio", "Mes", "Total_Streams"], ascending=[True, True, False])
			.groupby(["Anio", "Mes"], as_index=False)
			.first()
		)
		resultado9 = df_r9_top.to_dict(orient="records")
	else:
		resultado9 = resultado9_raw
else:
	resultado9 = []


# Mostrar resultados
mostrar_resultado(
	"RESULTADO 1: Suscriptores activos sin streaming en las ultimas 30 fechas registradas",
	resultado1,
)
mostrar_resultado(
	"RESULTADO 2: Contenidos con mayor indice de rentabilidad aproximado (Revenue / Streams)",
	resultado2,
)
mostrar_resultado(
	"RESULTADO 3: Categorias con mayor volumen de visualizaciones por pais",
	resultado3,
)
mostrar_resultado(
	"RESULTADO 4: Suscriptores con mayor actividad de streaming por servidor",
	resultado4,
)
mostrar_resultado(
	"RESULTADO 5: Ciudades con mayor ingreso total proveniente de suscriptores",
	resultado5,
)
mostrar_resultado(
	"RESULTADO 6: CRECIMIENTO DE EVENTOS POR SERVIDOR Y CATEGORIA EN AREA ESPECIFICA",
	resultado6,
)
mostrar_resultado(
	"RESULTADO 7: CATEGORIAS MAS REPRODUCIDAS POR ACTOR ESPECIFICO Y PAIS",
	resultado7,
)
mostrar_resultado(
	"RESULTADO 8: ACTORES EN PELICULAS TOP DE REPRODUCCIONES POR PAIS Y ANIO",
	resultado8,
)
mostrar_resultado(
	"RESULTADO 9: PELICULAS CON MAYOR NUMERO DE REPRODUCCIONES POR MES Y ANIO",
	resultado9,
)


# ---------------------------------------------------------------------------------
# DataFrames por resultado (para analisis y sustentacion)
# ---------------------------------------------------------------------------------
df_resultado1 = to_dataframe(resultado1)
df_resultado2 = to_dataframe(resultado2)
df_resultado3 = to_dataframe(resultado3)
df_resultado4 = to_dataframe(resultado4)
df_resultado5 = to_dataframe(resultado5)
df_resultado6 = to_dataframe(resultado6)
df_resultado7 = to_dataframe(resultado7)
df_resultado8 = to_dataframe(resultado8)
df_resultado9 = to_dataframe(resultado9)

exportar_dataframe(df_resultado1, "resultado1_suscriptores_inactivos")
exportar_dataframe(df_resultado2, "resultado2_rentabilidad_contenido")
exportar_dataframe(df_resultado3, "resultado3_demanda_categoria_pais")
exportar_dataframe(df_resultado4, "resultado4_top_suscriptores_servidor")
exportar_dataframe(df_resultado5, "resultado5_ingreso_ciudad")
exportar_dataframe(df_resultado6, "resultado6_crecimiento_servidor_categoria_area")
exportar_dataframe(df_resultado7, "resultado7_categoria_actor_pais")
exportar_dataframe(df_resultado8, "resultado8_actores_top_peliculas_pais_anio")
exportar_dataframe(df_resultado9, "resultado9_top_peliculas_mes_anio")

print("\n" + "-" * 120)
print("DataFrames generados y exportados en carpeta resultados_consultas/")
print("-" * 120)


# ---------------------------------------------------------------------------------
# Graficos (1 por cada consulta)
# ---------------------------------------------------------------------------------
# Grafico 1: Suscriptores inactivos por pais
if not df_resultado1.empty and "Pais" in df_resultado1.columns:
	serie_1 = df_resultado1.groupby("Pais").size().sort_values(ascending=False).head(10)
	fig, ax = plt.subplots(figsize=(10, 5))
	serie_1.plot(kind="bar", ax=ax, color="#2E86AB")
	ax.set_title("Suscriptores activos sin streaming por pais")
	ax.set_xlabel("Pais")
	ax.set_ylabel("Cantidad de suscriptores")
	guardar_figura(fig, "grafico_resultado1_inactivos_pais.png")
else:
	fig, ax = plt.subplots(figsize=(10, 4))
	ax.axis("off")
	ax.text(
		0.5,
		0.5,
		"No se encontraron suscriptores activos\nsin streaming en la ventana analizada.",
		ha="center",
		va="center",
		fontsize=12,
	)
	ax.set_title("Resultado 1 - Sin datos para graficar")
	guardar_figura(fig, "grafico_resultado1_inactivos_pais.png")

# Grafico 2: Rentabilidad por contenido (top 10)
if not df_resultado2.empty and {"Titulo", "Ingreso_por_Stream"}.issubset(df_resultado2.columns):
	top_2 = df_resultado2.nlargest(10, "Ingreso_por_Stream").copy()
	top_2 = top_2.iloc[::-1]
	fig, ax = plt.subplots(figsize=(10, 6))
	ax.barh(top_2["Titulo"], top_2["Ingreso_por_Stream"], color="#4CAF50")
	ax.set_title("Top 10 contenidos por ingreso por stream")
	ax.set_xlabel("Ingreso por stream")
	ax.set_ylabel("Contenido")
	guardar_figura(fig, "grafico_resultado2_rentabilidad_top10.png")

# Grafico 3: Demanda por pais y categoria (top 10)
if not df_resultado3.empty and {"Pais", "Categoria", "Total_Streams"}.issubset(df_resultado3.columns):
	top_3 = df_resultado3.nlargest(10, "Total_Streams").copy()
	top_3["Etiqueta"] = top_3["Pais"].astype(str) + " | " + top_3["Categoria"].astype(str)
	top_3 = top_3.iloc[::-1]
	fig, ax = plt.subplots(figsize=(11, 6))
	ax.barh(top_3["Etiqueta"], top_3["Total_Streams"], color="#8E44AD")
	ax.set_title("Top 10 demanda de visualizaciones por pais y categoria")
	ax.set_xlabel("Total de streams")
	ax.set_ylabel("Pais | Categoria")
	guardar_figura(fig, "grafico_resultado3_demanda_categoria_pais_top10.png")

# Grafico 4: Actividad de streaming por suscriptor-servidor (top 10)
if not df_resultado4.empty and {"Nombre", "Provider Key", "Total_Eventos"}.issubset(df_resultado4.columns):
	top_4 = df_resultado4.nlargest(10, "Total_Eventos").copy()
	top_4["Etiqueta"] = top_4["Nombre"].astype(str) + " | Srv " + top_4["Provider Key"].astype(str)
	top_4 = top_4.iloc[::-1]
	fig, ax = plt.subplots(figsize=(11, 6))
	ax.barh(top_4["Etiqueta"], top_4["Total_Eventos"], color="#F39C12")
	ax.set_title("Top 10 suscriptores por eventos de streaming y servidor")
	ax.set_xlabel("Total de eventos")
	ax.set_ylabel("Suscriptor | Servidor")
	guardar_figura(fig, "grafico_resultado4_top_suscriptor_servidor_top10.png")

# Grafico 5: Ingreso total por ciudad (top 10)
if not df_resultado5.empty and {"Pais", "Ciudad", "Ingreso_Total"}.issubset(df_resultado5.columns):
	top_5 = df_resultado5.nlargest(10, "Ingreso_Total").copy()
	top_5["Etiqueta"] = top_5["Ciudad"].astype(str) + ", " + top_5["Pais"].astype(str)
	top_5 = top_5.iloc[::-1]
	fig, ax = plt.subplots(figsize=(11, 6))
	ax.barh(top_5["Etiqueta"], top_5["Ingreso_Total"], color="#C0392B")
	ax.set_title("Top 10 ciudades por ingreso total")
	ax.set_xlabel("Ingreso total")
	ax.set_ylabel("Ciudad, Pais")
	guardar_figura(fig, "grafico_resultado5_ingreso_ciudad_top10.png")

# Grafico 6: Tendencia de eventos por servidor en area y periodo definidos
if not df_resultado6.empty and {"Provider Key", "Anio", "Mes", "Total_Eventos"}.issubset(df_resultado6.columns):
	df6 = df_resultado6.copy()
	df6["Anio"] = pd.to_numeric(df6["Anio"], errors="coerce")
	df6["Mes"] = pd.to_numeric(df6["Mes"], errors="coerce")
	df6["Total_Eventos"] = pd.to_numeric(df6["Total_Eventos"], errors="coerce")
	df6 = df6.dropna(subset=["Anio", "Mes", "Total_Eventos"])

	if not df6.empty:
		df6["Periodo"] = (
			df6["Anio"].astype(int).astype(str)
			+ "-"
			+ df6["Mes"].astype(int).astype(str).str.zfill(2)
		)
		top_providers = (
			df6.groupby("Provider Key", as_index=False)["Total_Eventos"]
			.sum()
			.nlargest(3, "Total_Eventos")["Provider Key"]
		)
		df6_top = df6[df6["Provider Key"].isin(top_providers)]
		serie6 = (
			df6_top.groupby(["Periodo", "Provider Key"], as_index=False)["Total_Eventos"]
			.sum()
			.pivot(index="Periodo", columns="Provider Key", values="Total_Eventos")
			.fillna(0)
			.sort_index()
		)

		fig, ax = plt.subplots(figsize=(11, 6))
		serie6.plot(ax=ax, marker="o", linewidth=2)
		titulo6 = f"Tendencia de eventos por servidor en {PAIS_Q6 or 'N/A'} - {CIUDAD_Q6 or 'N/A'}"
		ax.set_title(titulo6)
		ax.set_xlabel("Periodo (Anio-Mes)")
		ax.set_ylabel("Total de eventos")
		ax.grid(True, alpha=0.3)
		ax.legend(title="Provider Key")
		guardar_figura(fig, "grafico_resultado6_crecimiento_servidor_categoria_area.png")

# Grafico 7: Categorias con mas reproducciones para el actor especifico por pais
if not df_resultado7.empty and {"Pais", "Categoria", "Total_Streams"}.issubset(df_resultado7.columns):
	top_7 = df_resultado7.nlargest(12, "Total_Streams").copy()
	top_7["Etiqueta"] = top_7["Pais"].astype(str) + " | " + top_7["Categoria"].astype(str)
	top_7 = top_7.iloc[::-1]
	fig, ax = plt.subplots(figsize=(12, 6))
	ax.barh(top_7["Etiqueta"], top_7["Total_Streams"], color="#1ABC9C")
	ax.set_title(f"Categorias con mas reproducciones para actor: {ACTOR_Q7}")
	ax.set_xlabel("Total de streams")
	ax.set_ylabel("Pais | Categoria")
	guardar_figura(fig, "grafico_resultado7_categoria_actor_pais_top12.png")

# Grafico 8: Actores en peliculas top por reproducciones en pais y anio
if not df_resultado8.empty and {"Actor", "Streams_Asociados"}.issubset(df_resultado8.columns):
	top_8 = df_resultado8.nlargest(12, "Streams_Asociados").copy().iloc[::-1]
	fig, ax = plt.subplots(figsize=(11, 6))
	ax.barh(top_8["Actor"], top_8["Streams_Asociados"], color="#9B59B6")
	ax.set_title(f"Actores en peliculas top de reproducciones - {PAIS_Q8}, {ANIO_Q8}")
	ax.set_xlabel("Streams asociados")
	ax.set_ylabel("Actor")
	guardar_figura(fig, "grafico_resultado8_actores_top_peliculas_pais_anio.png")

# Grafico 9: Pelicula lider por cada mes-anio
if not df_resultado9.empty and {"Anio", "Mes", "Titulo", "Total_Streams"}.issubset(df_resultado9.columns):
	df9 = df_resultado9.copy()
	df9["Anio"] = pd.to_numeric(df9["Anio"], errors="coerce")
	df9["Mes"] = pd.to_numeric(df9["Mes"], errors="coerce")
	df9["Total_Streams"] = pd.to_numeric(df9["Total_Streams"], errors="coerce")
	df9 = df9.dropna(subset=["Anio", "Mes", "Total_Streams"])

	if not df9.empty:
		df9["Periodo"] = (
			df9["Anio"].astype(int).astype(str)
			+ "-"
			+ df9["Mes"].astype(int).astype(str).str.zfill(2)
		)
		df9["Etiqueta"] = df9["Periodo"] + " | " + df9["Titulo"].astype(str)
		df9 = df9.sort_values(["Anio", "Mes"]).iloc[::-1]

		fig, ax = plt.subplots(figsize=(12, 7))
		ax.barh(df9["Etiqueta"], df9["Total_Streams"], color="#E67E22")
		ax.set_title("Peliculas lideres por reproducciones en cada mes-anio")
		ax.set_xlabel("Total de streams")
		ax.set_ylabel("Periodo | Pelicula")
		guardar_figura(fig, "grafico_resultado9_top_peliculas_mes_anio.png")

print("Graficos generados en carpeta resultados_consultas/")