from consultas import *

# ---------------------------------------------------------------------------------
# DATASET CONSULTA 1
# ---------------------------------------------------------------------------------
def dataset_consulta_1():
    df = consulta_1()

    df = df.rename(columns={
        "Key": "Cliente_ID",
        "First Name": "Nombre",
        "Last Name": "Apellido",
        "Email": "Correo",
        "Gender": "Genero",
        "Birth": "Fecha_Nacimiento",
        "Geography Key": "Geografia_ID",
        "Is Active": "Cliente_Activo",
        "Create Date": "Fecha_Registro"
    })

    df = df[[
        "Cliente_ID",
        "Nombre",
        "Apellido",
        "Correo",
        "Genero",
        "Geografia_ID",
        "Cliente_Activo",
        "Fecha_Registro"
    ]]

    df = df.dropna()
    df = df.drop_duplicates()
    df = df.sort_values(by="Cliente_ID")

    df.to_csv(
        "DataSetGenerado/dataset_consulta_1_clientes_activos_sin_streaming.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df


# ---------------------------------------------------------------------------------
# DATASET CONSULTA 2
# ---------------------------------------------------------------------------------

def dataset_consulta_2():
    df = consulta_2()

    df = df.rename(columns={
        "Titulo": "Contenido",
        "Total_Streams": "Total_Reproducciones",
        "Total_Eventos": "Total_Eventos_Streaming",
        "Total_Minutos": "Total_Minutos_Vistos",
        "Revenue": "Ingreso_Total",
        "Ingreso_por_Stream": "Indice_Rentabilidad"
    })

    df = df[[
        "Contenido",
        "Total_Reproducciones",
        "Total_Eventos_Streaming",
        "Total_Minutos_Vistos",
        "Ingreso_Total",
        "Indice_Rentabilidad"
    ]]

    df = df.dropna()
    df = df.sort_values(by="Indice_Rentabilidad", ascending=False)

    df.to_csv(
        "DataSetGenerado/dataset_consulta_2_rentabilidad_contenido.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df

# ---------------------------------------------------------------------------------
# DATASET CONSULTA 3
# ---------------------------------------------------------------------------------

def dataset_consulta_3():
    df = consulta_3()

    df = df.rename(columns={
        "Pais": "Pais",
        "Categoria": "Categoria_Contenido",
        "Total_Visualizaciones": "Total_Visualizaciones",
        "Total_Eventos": "Total_Eventos_Streaming",
        "Total_Minutos": "Total_Minutos_Vistos"
    })

    df = df[[
        "Pais",
        "Categoria_Contenido",
        "Total_Visualizaciones",
        "Total_Eventos_Streaming",
        "Total_Minutos_Vistos"
    ]]

    df = df.dropna()
    df = df.drop_duplicates()
    df = df.sort_values(
        by=["Total_Visualizaciones"],
        ascending=False
    )

    df.to_csv(
        "DataSetGenerado/dataset_consulta_3_categorias_visualizaciones_por_pais.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df


# ---------------------------------------------------------------------------------
# DATASET CONSULTA 4
# ---------------------------------------------------------------------------------

def dataset_consulta_4():
    df = consulta_4()

    df = df.rename(columns={
        "Cliente": "Nombre_Cliente",
        "Servidor": "Plataforma",
        "Total_Eventos": "Total_Eventos_Streaming",
        "Total_Streams": "Total_Reproducciones"
    })

    df = df[[
        "Nombre_Cliente",
        "Plataforma",
        "Total_Eventos_Streaming",
        "Total_Reproducciones"
    ]]

    df = df.dropna()
    df = df.drop_duplicates()
    df = df.sort_values(by="Total_Eventos_Streaming", ascending=False)

    df.to_csv(
        "DataSetGenerado/dataset_consulta_4_eventos_por_cliente_servidor.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df

# ---------------------------------------------------------------------------------
# DATASET CONSULTA 5
# ---------------------------------------------------------------------------------

def dataset_consulta_5():
    df = consulta_5()

    df = df.rename(columns={
        "Pais": "Pais",
        "Ciudad": "Ciudad",
        "Ingreso_Total": "Ingreso_Total_Suscriptores",
        "Total_Pagos": "Total_Pagos_Registrados",
        "Total_Impuestos": "Total_Impuestos",
        "Total_Descuentos": "Total_Descuentos"
    })

    df = df[[
        "Pais",
        "Ciudad",
        "Ingreso_Total_Suscriptores",
        "Total_Pagos_Registrados",
        "Total_Impuestos",
        "Total_Descuentos"
    ]]

    df = df.dropna()
    df = df.drop_duplicates()
    df = df.sort_values(by="Ingreso_Total_Suscriptores", ascending=False)

    df.to_csv(
        "DataSetGenerado/dataset_consulta_5_ingresos_por_ciudad.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df

# ---------------------------------------------------------------------------------
# Dataset Consulta 6 - DATAMART STREAMING
# ---------------------------------------------------------------------------------
def dataset_consulta_6():
    df = consulta_6()

    df = df.dropna()
    df = df.drop_duplicates()

    df.to_csv(
        "DataSetGenerado/dataset_consulta_6_datamart_streaming.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df


# ---------------------------------------------------------------------------------
# Dataset Consulta 7 - DATAMART PAGOS
# ---------------------------------------------------------------------------------
def dataset_consulta_7():
    df = consulta_7()

    df = df.dropna()
    df = df.drop_duplicates()

    df.to_csv(
        "DataSetGenerado/dataset_consulta_7_datamart_pagos.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df


# ---------------------------------------------------------------------------------
# Dataset Consulta 8 - DATAMART SUSCRIPCIONES
# ---------------------------------------------------------------------------------
def dataset_consulta_8():
    df = consulta_8()

    df = df.dropna()
    df = df.drop_duplicates()

    df.to_csv(
        "DataSetGenerado/dataset_consulta_8_datamart_suscripciones.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df


# ---------------------------------------------------------------------------------
# Dataset Consulta 9 - DATAMART CATÁLOGO DISPONIBLE
# ---------------------------------------------------------------------------------
def dataset_consulta_9():
    df = consulta_9()

    df = df.dropna()
    df = df.drop_duplicates()

    df.to_csv(
        "DataSetGenerado/dataset_consulta_9_datamart_catalogo_disponible.csv",
        index=False,
        encoding="utf-8-sig"
    )

    return df