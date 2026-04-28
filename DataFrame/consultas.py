import pandas as pd
from conexion import *

# ---------------------------------------------------------------------------------
# Consulta 1
# ¿Qué suscriptores activos no registraron eventos de streaming en las últimas
# 30 fechas registradas en el sistema?
# ---------------------------------------------------------------------------------

def consulta_1():
    ultimas_fechas = eventostreaming.distinct("Date Key")
    ultimas_fechas = sorted(ultimas_fechas, reverse=True)[:30]

    resultado = list(cliente.aggregate([
        {"$match": {"Is Active": True}},
        {
            "$lookup": {
                "from": "eventostreaming",
                "localField": "Key",
                "foreignField": "Customer Key",
                "as": "eventos"
            }
        },
        {
            "$addFields": {
                "eventos_recientes": {
                    "$filter": {
                        "input": "$eventos",
                        "as": "e",
                        "cond": {"$in": ["$$e.Date Key", ultimas_fechas]}
                    }
                }
            }
        },
        {"$match": {"eventos_recientes": {"$size": 0}}}
    ]))

    return pd.DataFrame(resultado)

# ---------------------------------------------------------------------------------
# Consulta 2
# ¿Qué contenidos generan mayor ingreso total en relación con su volumen de
# reproducciones? (Índice de rentabilidad aproximado)
# ---------------------------------------------------------------------------------

def consulta_2():
    resultado = list(eventostreaming.aggregate([

        # 1. Agrupar eventos por contenido
        {
            "$group": {
                "_id": "$Content Key",
                "Total_Streams": {"$sum": "$Streams Count"},
                "Total_Eventos": {"$sum": 1},
                "Total_Minutos": {"$sum": "$Minutes Watched"}
            }
        },

        # 2. Unir con la colección contenido
        {
            "$lookup": {
                "from": "contenido",
                "localField": "_id",
                "foreignField": "Key",
                "as": "contenido_info"
            }
        },

        # 3. Desarmar el array
        {"$unwind": "$contenido_info"},

        # 4. Mostrar campos importantes
        {
            "$project": {
                "_id": 0,
                "Content Key": "$_id",
                "Titulo": "$contenido_info.Title",
                "Revenue": "$contenido_info.Revenue",
                "Total_Streams": 1,
                "Total_Eventos": 1,
                "Total_Minutos": 1,
                "Ingreso_por_Stream": {
                    "$divide": [
                        "$contenido_info.Revenue",
                        "$Total_Streams"
                    ]
                }
            }
        },

        # 5. Ordenar
        {"$sort": {"Ingreso_por_Stream": -1}}
    ]))

    return pd.DataFrame(resultado)


# ---------------------------------------------------------------------------------
# Consulta 3
# ¿Qué categorías de contenido presentan mayor volumen de visualizaciones por país?
# ---------------------------------------------------------------------------------

def consulta_3():
    resultado = list(eventostreaming.aggregate([

        # 1. Agrupar primero para reducir datos
        {
            "$group": {
                "_id": {
                    "Customer Key": "$Customer Key",
                    "Content Key": "$Content Key"
                },
                "Total_Visualizaciones": {"$sum": "$Streams Count"},
                "Total_Eventos": {"$sum": 1},
                "Total_Minutos": {"$sum": "$Minutes Watched"}
            }
        },

        # 2. Unir con cliente
        {
            "$lookup": {
                "from": "cliente",
                "localField": "_id.Customer Key",
                "foreignField": "Key",
                "as": "cliente"
            }
        },
        {"$unwind": "$cliente"},

        # 3. Unir con geografía
        {
            "$lookup": {
                "from": "geografia",
                "localField": "cliente.Geography Key",
                "foreignField": "Key",
                "as": "geografia"
            }
        },
        {"$unwind": "$geografia"},

        # 4. Unir contenido con categoría
        {
            "$lookup": {
                "from": "contenidocategoria",
                "localField": "_id.Content Key",
                "foreignField": "Content Key",
                "as": "contenido_categoria"
            }
        },
        {"$unwind": "$contenido_categoria"},

        # 5. Unir con categoría
        {
            "$lookup": {
                "from": "categoria",
                "localField": "contenido_categoria.Category Key",
                "foreignField": "Key",
                "as": "categoria"
            }
        },
        {"$unwind": "$categoria"},

        # 6. Agrupar por país y categoría
        {
            "$group": {
                "_id": {
                    "Pais": "$geografia.Country",
                    "Categoria": "$categoria.Name"
                },
                "Total_Visualizaciones": {"$sum": "$Total_Visualizaciones"},
                "Total_Eventos": {"$sum": "$Total_Eventos"},
                "Total_Minutos": {"$sum": "$Total_Minutos"}
            }
        },

        # 7. Mostrar bonito
        {
            "$project": {
                "_id": 0,
                "Pais": "$_id.Pais",
                "Categoria": "$_id.Categoria",
                "Total_Visualizaciones": 1,
                "Total_Eventos": 1,
                "Total_Minutos": 1
            }
        },

        # 8. Ordenar y limitar
        {"$sort": {"Total_Visualizaciones": -1}}
    ]))

    return pd.DataFrame(resultado)

# ---------------------------------------------------------------------------------
# Consulta 4
# ¿Qué suscriptores registran mayor número de eventos de streaming por servidor?
# ---------------------------------------------------------------------------------

def consulta_4():
    resultado = list(eventostreaming.aggregate([

        # 1. Agrupar por cliente y proveedor
        {
            "$group": {
                "_id": {
                    "Customer Key": "$Customer Key",
                    "Provider Key": "$Provider Key"
                },
                "Total_Eventos": {"$sum": 1},
                "Total_Streams": {"$sum": "$Streams Count"}
            }
        },

        # 2. Unir con cliente
        {
            "$lookup": {
                "from": "cliente",
                "localField": "_id.Customer Key",
                "foreignField": "Key",
                "as": "cliente"
            }
        },
        {"$unwind": "$cliente"},

        # 3. Unir con proveedor
        {
            "$lookup": {
                "from": "proveedor",
                "localField": "_id.Provider Key",
                "foreignField": "Key",
                "as": "proveedor"
            }
        },
        {"$unwind": "$proveedor"},

        # 4. Mostrar limpio
        {
            "$project": {
                "_id": 0,
                "Cliente": {
                    "$concat": ["$cliente.First Name", " ", "$cliente.Last Name"]
                },
                "Servidor": "$proveedor.Name",
                "Total_Eventos": 1,
                "Total_Streams": 1
            }
        },

        # 5. Ordenar
        {"$sort": {"Total_Eventos": -1}},

        # 6. Top 20
        {"$limit": 20}
    ]))

    return pd.DataFrame(resultado)

# ---------------------------------------------------------------------------------
# Consulta 5
# ¿Qué ciudades generan mayor ingreso total proveniente de suscriptores?
# ---------------------------------------------------------------------------------

def consulta_5():
    resultado = list(pagos.aggregate([

        # 1. Unir pago con cliente
        {
            "$lookup": {
                "from": "cliente",
                "localField": "Customer Key",
                "foreignField": "Key",
                "as": "cliente"
            }
        },
        {"$unwind": "$cliente"},

        # 2. Unir cliente con geografía
        {
            "$lookup": {
                "from": "geografia",
                "localField": "cliente.Geography Key",
                "foreignField": "Key",
                "as": "geografia"
            }
        },
        {"$unwind": "$geografia"},

        # 3. Agrupar por país y ciudad
        {
            "$group": {
                "_id": {
                    "Pais": "$geografia.Country",
                    "Ciudad": "$geografia.City"
                },
                "Ingreso_Total": {"$sum": "$Amount"},
                "Total_Pagos": {"$sum": 1},
                "Total_Impuestos": {"$sum": "$Tax Amount"},
                "Total_Descuentos": {"$sum": "$Discount Amount"}
            }
        },

        # 4. Mostrar columnas limpias
        {
            "$project": {
                "_id": 0,
                "Pais": "$_id.Pais",
                "Ciudad": "$_id.Ciudad",
                "Ingreso_Total": {"$round": ["$Ingreso_Total", 2]},
                "Total_Pagos": 1,
                "Total_Impuestos": {"$round": ["$Total_Impuestos", 2]},
                "Total_Descuentos": {"$round": ["$Total_Descuentos", 2]}
            }
        },

        # 5. Ordenar por más ingreso
        {"$sort": {"Ingreso_Total": -1}},

        # 6. Top 20
        #{"$limit": 20}
    ]))

    return pd.DataFrame(resultado)


# ---------------------------------------------------------------------------------
# Consulta 6
# ¿Qué categorías de contenido registran mayor número de reproducciones
# para un actor específico en cada país?
# ---------------------------------------------------------------------------------

def consulta_6(actor="Tom Hanks"):
    resultado = list(eventostreaming.aggregate([

        # 1. Evento -> Cliente
        {
            "$lookup": {
                "from": "cliente",
                "localField": "Customer Key",
                "foreignField": "Key",
                "as": "cliente"
            }
        },
        {"$unwind": "$cliente"},

        # 2. Cliente -> Geografía
        {
            "$lookup": {
                "from": "geografia",
                "localField": "cliente.Geography Key",
                "foreignField": "Key",
                "as": "geografia"
            }
        },
        {"$unwind": "$geografia"},

        # 3. Evento -> ContenidoTalento
        {
            "$lookup": {
                "from": "contenidotalento",
                "localField": "Content Key",
                "foreignField": "Content Key",
                "as": "contenido_talento"
            }
        },
        {"$unwind": "$contenido_talento"},

        # 4. ContenidoTalento -> Talento
        {
            "$lookup": {
                "from": "talento",
                "localField": "contenido_talento.Talent Key",
                "foreignField": "Key",
                "as": "talento"
            }
        },
        {"$unwind": "$talento"},

        # 5. Crear nombre completo del actor
        {
            "$addFields": {
                "Actor": {
                    "$concat": [
                        "$talento.First Name",
                        " ",
                        "$talento.Last Name"
                    ]
                }
            }
        },

        # 6. Filtrar solo el actor elegido
        {"$match": {"Actor": actor}},

        # 7. Evento -> ContenidoCategoria
        {
            "$lookup": {
                "from": "contenidocategoria",
                "localField": "Content Key",
                "foreignField": "Content Key",
                "as": "contenido_categoria"
            }
        },
        {"$unwind": "$contenido_categoria"},

        # 8. ContenidoCategoria -> Categoria
        {
            "$lookup": {
                "from": "categoria",
                "localField": "contenido_categoria.Category Key",
                "foreignField": "Key",
                "as": "categoria"
            }
        },
        {"$unwind": "$categoria"},

        # 9. Agrupar por país y categoría
        {
            "$group": {
                "_id": {
                    "Pais": "$geografia.Country",
                    "Categoria": "$categoria.Name"
                },
                "Total_Reproducciones": {"$sum": "$Streams Count"},
                "Total_Eventos": {"$sum": 1},
                "Total_Minutos": {"$sum": "$Minutes Watched"}
            }
        },

        # 10. Mostrar limpio
        {
            "$project": {
                "_id": 0,
                "Actor": actor,
                "Pais": "$_id.Pais",
                "Categoria": "$_id.Categoria",
                "Total_Reproducciones": 1,
                "Total_Eventos": 1,
                "Total_Minutos": 1
            }
        },

        # 11. Ordenar
        {"$sort": {"Pais": 1, "Total_Reproducciones": -1}},

        # 12. Top 20
        {"$limit": 20}
    ]))

    return pd.DataFrame(resultado)