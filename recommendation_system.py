# ============================================================================
# SISTEMA DE RECOMENDACION - SAKILA STREAMING
# ============================================================================
# PROPOSITO: Generar 3 sistemas de recomendacion para el proyecto:
#   1) Usuario a usuario (collaborative filtering)
#   2) Item a item (similitud entre contenidos por comportamiento)
#   3) Contenido por tokenizacion (similitud semantica por texto)
#
# ENTRADA: MongoDB (clientes/customer, catalogo/catalog, eventos_streaming/streamingevent)
# SALIDA: CSV + PNG para cada sistema + tabla comparativa
# ============================================================================

import os
import warnings
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pymongo import MongoClient
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

warnings.filterwarnings("ignore")


MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "SakilaStreaming"

TOP_K_USUARIOS_SIMILARES = 20
TOP_K_RECOMENDACIONES = 10
TOP_K_VECINOS_ITEMS = 15
MIN_MINUTOS_USUARIO = 50
MIN_USUARIOS_POR_CONTENIDO = 3
UMBRAL_VISTO_ALTO = 90


def print_separador() -> None:
    print("=" * 80)


def resolver_coleccion(db, nombres_posibles: List[str]):
    colecciones_disponibles = db.list_collection_names()
    for nombre in nombres_posibles:
        if nombre in colecciones_disponibles:
            return db[nombre], nombre
    return None, None


def normalizar_columnas_eventos(df_eventos: pd.DataFrame) -> pd.DataFrame:
    alias = {
        "Customer_Key": ["Customer Key", "Customer_Key", "customer_key", "customerKey"],
        "Content_Key": ["Content Key", "Content_Key", "content_key", "contentKey"],
        "Minutes_Watched": [
            "Minutes Watched",
            "Minutes_Watched",
            "minutes_watched",
            "minutesWatched",
        ],
    }

    renombrar = {}
    faltantes = []

    for destino, candidatos in alias.items():
        columna = next((c for c in candidatos if c in df_eventos.columns), None)
        if columna is None:
            faltantes.append(destino)
        else:
            renombrar[columna] = destino

    if faltantes:
        raise ValueError(
            "No se encontraron columnas requeridas en eventos: " + ", ".join(faltantes)
        )

    df = df_eventos.rename(columns=renombrar).copy()
    df = df[["Customer_Key", "Content_Key", "Minutes_Watched"]]

    df["Customer_Key"] = pd.to_numeric(df["Customer_Key"], errors="coerce")
    df["Content_Key"] = pd.to_numeric(df["Content_Key"], errors="coerce")
    df["Minutes_Watched"] = pd.to_numeric(df["Minutes_Watched"], errors="coerce")

    df = df.dropna(subset=["Customer_Key", "Content_Key", "Minutes_Watched"])
    df["Customer_Key"] = df["Customer_Key"].astype(int)
    df["Content_Key"] = df["Content_Key"].astype(int)
    df["Minutes_Watched"] = df["Minutes_Watched"].clip(lower=0)

    return df


def extraer_texto(valor) -> str:
    if valor is None:
        return ""
    if isinstance(valor, (str, int, float)):
        return str(valor)
    if isinstance(valor, dict):
        partes = []
        for v in valor.values():
            if isinstance(v, (str, int, float)):
                partes.append(str(v))
        return " ".join(partes)
    if isinstance(valor, list):
        partes = []
        for item in valor:
            texto_item = extraer_texto(item)
            if texto_item:
                partes.append(texto_item)
        return " ".join(partes)
    return ""


def cargar_catalogo_texto(catalogo_collection) -> Tuple[Dict[int, str], pd.DataFrame]:
    contenidos_info: Dict[int, str] = {}
    registros = []

    for doc in catalogo_collection.find({}, {"_id": 0}):
        key = doc.get("Key") or doc.get("Content Key") or doc.get("Content_Key")
        if key is None:
            continue

        try:
            key_int = int(key)
        except (TypeError, ValueError):
            continue

        titulo = doc.get("Title") or doc.get("title") or f"Content {key_int}"
        descripcion = doc.get("Description") or doc.get("description") or ""
        categorias = extraer_texto(doc.get("Categorias_Array"))
        talento = extraer_texto(doc.get("Talento_Array"))
        idioma = extraer_texto(doc.get("Name"))
        rating = extraer_texto(doc.get("Rating"))

        texto_base = " ".join(
            p.strip()
            for p in [str(titulo), str(descripcion), categorias, talento, idioma, rating]
            if str(p).strip()
        )

        if not texto_base:
            texto_base = str(titulo)

        contenidos_info[key_int] = str(titulo)
        registros.append({
            "Content_Key": key_int,
            "Titulo": str(titulo),
            "Texto": texto_base,
        })

    df_catalogo = pd.DataFrame(registros).drop_duplicates(subset=["Content_Key"])
    return contenidos_info, df_catalogo


def calcular_estadisticas_similitud(df_similitud: pd.DataFrame) -> Dict[str, object]:
    if len(df_similitud) < 2:
        return {
            "promedio": 0.0,
            "desv_std": 0.0,
            "min": 0.0,
            "max": 0.0,
            "valores": np.array([]),
        }

    triu_indices = np.triu_indices(len(df_similitud), k=1)
    valores = df_similitud.values[triu_indices]

    return {
        "promedio": float(np.mean(valores)),
        "desv_std": float(np.std(valores)),
        "min": float(np.min(valores)),
        "max": float(np.max(valores)),
        "valores": valores,
    }


def calcular_coseno_ajustado_usuario(matriz_usuario_contenido: pd.DataFrame) -> pd.DataFrame:
    valores = matriz_usuario_contenido.values.astype(float)
    mascara_vistos = valores > 0

    suma_por_usuario = (valores * mascara_vistos).sum(axis=1)
    conteo_por_usuario = mascara_vistos.sum(axis=1)

    media_por_usuario = np.divide(
        suma_por_usuario,
        conteo_por_usuario,
        out=np.zeros_like(suma_por_usuario, dtype=float),
        where=conteo_por_usuario > 0,
    )

    matriz_ajustada = np.where(mascara_vistos, valores - media_por_usuario[:, None], 0.0)
    similitud = cosine_similarity(matriz_ajustada)
    similitud = np.clip(similitud, -1.0, 1.0)
    np.fill_diagonal(similitud, 1.0)

    return pd.DataFrame(
        similitud,
        index=matriz_usuario_contenido.index,
        columns=matriz_usuario_contenido.index,
    )


def calcular_similitud_item_item(matriz_usuario_contenido: pd.DataFrame) -> pd.DataFrame:
    valores = matriz_usuario_contenido.values.astype(float)
    mascara_vistos = valores > 0

    suma_por_usuario = (valores * mascara_vistos).sum(axis=1)
    conteo_por_usuario = mascara_vistos.sum(axis=1)
    media_por_usuario = np.divide(
        suma_por_usuario,
        conteo_por_usuario,
        out=np.zeros_like(suma_por_usuario, dtype=float),
        where=conteo_por_usuario > 0,
    )

    # Ajuste por usuario antes de comparar items (adjusted cosine item-item).
    matriz_ajustada = np.where(mascara_vistos, valores - media_por_usuario[:, None], 0.0)
    similitud_items = cosine_similarity(matriz_ajustada.T)
    similitud_items = np.clip(similitud_items, -1.0, 1.0)
    np.fill_diagonal(similitud_items, 1.0)

    return pd.DataFrame(
        similitud_items,
        index=matriz_usuario_contenido.columns,
        columns=matriz_usuario_contenido.columns,
    )


def calcular_similitud_contenido_tokenizado(
    df_catalogo_ordenado: pd.DataFrame,
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    textos = df_catalogo_ordenado["Texto"].fillna("").astype(str).tolist()

    vectorizer = TfidfVectorizer(
        stop_words="english",
        max_features=5000,
        ngram_range=(1, 2),
    )
    tfidf_matrix = vectorizer.fit_transform(textos)

    similitud = cosine_similarity(tfidf_matrix)
    similitud = np.clip(similitud, 0.0, 1.0)
    np.fill_diagonal(similitud, 1.0)

    df_sim = pd.DataFrame(
        similitud,
        index=df_catalogo_ordenado["Content_Key"].astype(int).tolist(),
        columns=df_catalogo_ordenado["Content_Key"].astype(int).tolist(),
    )

    metadata = {
        "vocabulario": float(len(vectorizer.vocabulary_)),
        "dimension_tfidf": float(tfidf_matrix.shape[1]),
    }

    return df_sim, metadata


def factor_novedad(minutos_vistos_usuario: float) -> float:
    if minutos_vistos_usuario == 0:
        return 1.25
    if minutos_vistos_usuario < 30:
        return 1.12
    if minutos_vistos_usuario <= UMBRAL_VISTO_ALTO:
        return 1.00
    return 0.65


def generar_recomendaciones_usuario_usuario(
    matriz_usuario_contenido: pd.DataFrame,
    df_similitud_usuarios: pd.DataFrame,
) -> Dict[int, List[Tuple[int, float]]]:
    usuarios = list(matriz_usuario_contenido.index)
    contenidos = list(matriz_usuario_contenido.columns)
    popularidad_global = matriz_usuario_contenido.mean(axis=0).sort_values(ascending=False)

    recomendaciones_por_usuario: Dict[int, List[Tuple[int, float]]] = {}

    print("\nProcesando Usuario-Usuario...")

    for idx, usuario_actual in enumerate(usuarios, start=1):
        if idx % 20 == 0 or idx == len(usuarios):
            print(f"  Usuario-Usuario procesados: {idx}/{len(usuarios)}")

        minutos_usuario_vio = matriz_usuario_contenido.loc[usuario_actual]

        similitudes_usuario = df_similitud_usuarios.loc[usuario_actual].drop(usuario_actual)
        similitudes_usuario = similitudes_usuario.clip(lower=0)
        usuarios_similares = similitudes_usuario.nlargest(
            min(TOP_K_USUARIOS_SIMILARES, len(similitudes_usuario))
        )

        puntuaciones_contenido: Dict[int, float] = {}

        if minutos_usuario_vio.sum() >= MIN_MINUTOS_USUARIO and not usuarios_similares.empty:
            for contenido in contenidos:
                minutos_vecinos = matriz_usuario_contenido.loc[usuarios_similares.index, contenido]
                mascara_vieron = minutos_vecinos > 0

                if int(mascara_vieron.sum()) < MIN_USUARIOS_POR_CONTENIDO:
                    continue

                pesos = usuarios_similares[mascara_vieron]
                if float(pesos.sum()) <= 0:
                    continue

                score_base = float(np.dot(minutos_vecinos[mascara_vieron], pesos) / pesos.sum())
                score_final = score_base * factor_novedad(float(minutos_usuario_vio[contenido]))
                puntuaciones_contenido[int(contenido)] = score_final

        if len(puntuaciones_contenido) < TOP_K_RECOMENDACIONES:
            for contenido, score_popularidad in popularidad_global.items():
                contenido = int(contenido)
                if contenido in puntuaciones_contenido:
                    continue

                score_fallback = float(score_popularidad) * factor_novedad(
                    float(minutos_usuario_vio[contenido])
                )
                puntuaciones_contenido[contenido] = score_fallback

                if len(puntuaciones_contenido) >= TOP_K_RECOMENDACIONES * 3:
                    break

        top_10 = sorted(
            puntuaciones_contenido.items(), key=lambda x: x[1], reverse=True
        )[:TOP_K_RECOMENDACIONES]

        recomendaciones_por_usuario[int(usuario_actual)] = top_10

    return recomendaciones_por_usuario


def generar_recomendaciones_item_based(
    matriz_usuario_contenido: pd.DataFrame,
    df_similitud_items: pd.DataFrame,
    nombre_metodo: str,
) -> Dict[int, List[Tuple[int, float]]]:
    usuarios = list(matriz_usuario_contenido.index)
    contenidos = list(matriz_usuario_contenido.columns)
    popularidad_global = matriz_usuario_contenido.mean(axis=0).sort_values(ascending=False)

    recomendaciones_por_usuario: Dict[int, List[Tuple[int, float]]] = {}

    print(f"\nProcesando {nombre_metodo}...")

    for idx, usuario in enumerate(usuarios, start=1):
        if idx % 20 == 0 or idx == len(usuarios):
            print(f"  {nombre_metodo} procesados: {idx}/{len(usuarios)}")

        historial = matriz_usuario_contenido.loc[usuario]
        items_vistos = historial[historial > 0]

        puntuaciones_contenido: Dict[int, float] = {}

        if not items_vistos.empty:
            for item_candidato in contenidos:
                similitudes = df_similitud_items.loc[item_candidato, items_vistos.index]
                if item_candidato in similitudes.index:
                    similitudes = similitudes.drop(item_candidato, errors="ignore")

                similitudes = similitudes[similitudes > 0].nlargest(TOP_K_VECINOS_ITEMS)
                if similitudes.empty:
                    continue

                minutos_items_relacionados = items_vistos.loc[similitudes.index]
                peso_total = float(similitudes.sum())
                if peso_total <= 0:
                    continue

                score_base = float(
                    np.dot(similitudes.values, minutos_items_relacionados.values) / peso_total
                )
                score_final = score_base * factor_novedad(float(historial[item_candidato]))
                puntuaciones_contenido[int(item_candidato)] = score_final

        if len(puntuaciones_contenido) < TOP_K_RECOMENDACIONES:
            for item_popular, score_popularidad in popularidad_global.items():
                item_popular = int(item_popular)
                if item_popular in puntuaciones_contenido:
                    continue

                score_fb = float(score_popularidad) * factor_novedad(float(historial[item_popular]))
                puntuaciones_contenido[item_popular] = score_fb

                if len(puntuaciones_contenido) >= TOP_K_RECOMENDACIONES * 3:
                    break

        top_10 = sorted(
            puntuaciones_contenido.items(), key=lambda x: x[1], reverse=True
        )[:TOP_K_RECOMENDACIONES]
        recomendaciones_por_usuario[int(usuario)] = top_10

    return recomendaciones_por_usuario


def construir_dataframe_recomendaciones(
    recomendaciones_por_usuario: Dict[int, List[Tuple[int, float]]],
    contenidos_info: Dict[int, str],
) -> pd.DataFrame:
    filas = []

    for usuario, recomendaciones in recomendaciones_por_usuario.items():
        fila = {
            "Customer_Key": int(usuario),
            "Num_Recomendaciones": len(recomendaciones),
        }

        for posicion in range(1, TOP_K_RECOMENDACIONES + 1):
            fila[f"Recomendacion_{posicion}_ContentKey"] = np.nan
            fila[f"Recomendacion_{posicion}_Titulo"] = None
            fila[f"Recomendacion_{posicion}_Score"] = np.nan

        for posicion, (content_key, puntuacion) in enumerate(recomendaciones, start=1):
            fila[f"Recomendacion_{posicion}_ContentKey"] = int(content_key)
            fila[f"Recomendacion_{posicion}_Titulo"] = contenidos_info.get(
                int(content_key), f"Content {content_key}"
            )
            fila[f"Recomendacion_{posicion}_Score"] = round(float(puntuacion), 4)

        filas.append(fila)

    return pd.DataFrame(filas)


def calcular_metricas_recomendacion(
    recomendaciones_por_usuario: Dict[int, List[Tuple[int, float]]],
    matriz_usuario_contenido: pd.DataFrame,
) -> Dict[str, object]:
    total_usuarios = len(recomendaciones_por_usuario)
    usuarios_con_recomendaciones = sum(
        1 for recs in recomendaciones_por_usuario.values() if len(recs) > 0
    )

    recomendaciones_totales = sum(len(recs) for recs in recomendaciones_por_usuario.values())
    recomendaciones_promedio = (
        recomendaciones_totales / total_usuarios if total_usuarios > 0 else 0.0
    )

    todos_scores = []
    recomendaciones_nuevas = 0
    frecuencia_items: Dict[int, int] = {}

    for usuario, recs in recomendaciones_por_usuario.items():
        historial_usuario = matriz_usuario_contenido.loc[usuario]

        for content_key, score in recs:
            todos_scores.append(float(score))
            frecuencia_items[int(content_key)] = frecuencia_items.get(int(content_key), 0) + 1

            if float(historial_usuario[int(content_key)]) == 0:
                recomendaciones_nuevas += 1

    diversidad_catalogo = (
        len(frecuencia_items) / matriz_usuario_contenido.shape[1]
        if matriz_usuario_contenido.shape[1] > 0
        else 0.0
    )
    tasa_novedad = (
        recomendaciones_nuevas / recomendaciones_totales
        if recomendaciones_totales > 0
        else 0.0
    )

    return {
        "usuarios_totales": total_usuarios,
        "usuarios_con_recomendaciones": usuarios_con_recomendaciones,
        "recomendaciones_totales": recomendaciones_totales,
        "recomendaciones_promedio": recomendaciones_promedio,
        "scores": todos_scores,
        "frecuencia_items": frecuencia_items,
        "diversidad_catalogo": diversidad_catalogo,
        "tasa_novedad": tasa_novedad,
    }


def crear_visualizacion_usuario_usuario(
    df_similitud_coseno_crudo: pd.DataFrame,
    df_similitud_coseno_ajustado: pd.DataFrame,
    contenidos_info: Dict[int, str],
    metricas: Dict[str, object],
    output_png: str,
) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(17, 12))

    n_plot = min(20, len(df_similitud_coseno_ajustado))
    subset = df_similitud_coseno_ajustado.iloc[:n_plot, :n_plot]

    im = axes[0, 0].imshow(subset, cmap="coolwarm", vmin=-1, vmax=1, aspect="auto")
    axes[0, 0].set_title(
        "Usuario-Usuario: matriz de similitud por coseno ajustado (20 usuarios)",
        fontsize=11,
        fontweight="bold",
    )
    axes[0, 0].set_xlabel("Usuarios")
    axes[0, 0].set_ylabel("Usuarios")
    axes[0, 0].set_xticks(range(n_plot))
    axes[0, 0].set_yticks(range(n_plot))
    axes[0, 0].set_xticklabels(subset.columns, rotation=45, fontsize=8)
    axes[0, 0].set_yticklabels(subset.index, fontsize=8)
    cbar = plt.colorbar(im, ax=axes[0, 0])
    cbar.set_label("Similitud coseno")

    idx_sup = np.triu_indices(len(df_similitud_coseno_crudo), k=1)
    vals_crudo = df_similitud_coseno_crudo.values[idx_sup]
    vals_ajustado = df_similitud_coseno_ajustado.values[idx_sup]

    axes[0, 1].hist(
        vals_crudo,
        bins=35,
        alpha=0.45,
        label="Coseno crudo",
        color="#F58518",
        edgecolor="black",
        linewidth=0.4,
    )
    axes[0, 1].hist(
        vals_ajustado,
        bins=35,
        alpha=0.55,
        label="Coseno ajustado",
        color="#4C78A8",
        edgecolor="black",
        linewidth=0.4,
    )
    axes[0, 1].axvline(np.mean(vals_crudo), color="#F58518", linestyle="--", linewidth=2)
    axes[0, 1].axvline(np.mean(vals_ajustado), color="#4C78A8", linestyle="--", linewidth=2)
    axes[0, 1].set_title("Usuario-Usuario: comparacion coseno crudo vs ajustado", fontsize=11, fontweight="bold")
    axes[0, 1].set_xlabel("Valor de similitud")
    axes[0, 1].set_ylabel("Frecuencia")
    axes[0, 1].grid(True, alpha=0.25)
    axes[0, 1].legend()

    top_items = sorted(
        metricas["frecuencia_items"].items(), key=lambda x: x[1], reverse=True
    )[:10]
    labels = [
        contenidos_info.get(int(content_key), f"Content {content_key}")[:34]
        for content_key, _ in top_items
    ]
    valores = [conteo for _, conteo in top_items]

    axes[1, 0].barh(labels[::-1], valores[::-1], color="#54A24B")
    axes[1, 0].set_title("Usuario-Usuario: Top 10 contenidos recomendados", fontsize=11, fontweight="bold")
    axes[1, 0].set_xlabel("Veces recomendado")
    axes[1, 0].set_ylabel("Contenido")
    axes[1, 0].grid(True, axis="x", alpha=0.25)

    scores = metricas["scores"]
    promedio_scores = float(np.mean(scores)) if scores else 0.0
    minimo_scores = float(np.min(scores)) if scores else 0.0
    maximo_scores = float(np.max(scores)) if scores else 0.0

    kpi_text = (
        "KPI USUARIO-USUARIO\n"
        "------------------------------\n"
        f"Usuarios analizados: {metricas['usuarios_totales']}\n"
        f"Cobertura: {metricas['usuarios_con_recomendaciones']}/{metricas['usuarios_totales']}\n"
        f"Recomendaciones totales: {metricas['recomendaciones_totales']}\n"
        f"Promedio por usuario: {metricas['recomendaciones_promedio']:.2f}\n"
        f"Diversidad del catalogo: {metricas['diversidad_catalogo'] * 100:.1f}%\n"
        f"Novedad (no visto): {metricas['tasa_novedad'] * 100:.1f}%\n"
        "\n"
        "PUNTAJES\n"
        "------------------------------\n"
        f"Media: {promedio_scores:.2f}\n"
        f"Minimo: {minimo_scores:.2f}\n"
        f"Maximo: {maximo_scores:.2f}\n"
    )

    axes[1, 1].text(
        0.03,
        0.98,
        kpi_text,
        transform=axes[1, 1].transAxes,
        va="top",
        ha="left",
        fontsize=10,
        family="monospace",
        bbox={"boxstyle": "round,pad=0.5", "facecolor": "#F7F7F7", "alpha": 0.95},
    )
    axes[1, 1].axis("off")

    plt.tight_layout()
    plt.savefig(output_png, dpi=300, bbox_inches="tight")
    plt.close()


def crear_visualizacion_item_o_contenido(
    df_similitud_items: pd.DataFrame,
    contenidos_info: Dict[int, str],
    metricas: Dict[str, object],
    matriz_usuario_contenido: pd.DataFrame,
    output_png: str,
    titulo_metodo: str,
    descripcion_similitud: str,
    similitud_min: float,
    similitud_max: float,
    cmap: str,
    lineas_extra_kpi: List[str],
) -> None:
    fig, axes = plt.subplots(2, 2, figsize=(17, 12))

    items_populares = (
        matriz_usuario_contenido.mean(axis=0)
        .sort_values(ascending=False)
        .index[: min(20, len(df_similitud_items))]
        .tolist()
    )
    subset = df_similitud_items.loc[items_populares, items_populares]
    labels_items = [contenidos_info.get(int(i), f"Content {i}")[:16] for i in items_populares]

    im = axes[0, 0].imshow(subset, cmap=cmap, vmin=similitud_min, vmax=similitud_max, aspect="auto")
    axes[0, 0].set_title(f"{titulo_metodo}: matriz de similitud (20 contenidos)", fontsize=11, fontweight="bold")
    axes[0, 0].set_xticks(range(len(labels_items)))
    axes[0, 0].set_yticks(range(len(labels_items)))
    axes[0, 0].set_xticklabels(labels_items, rotation=75, fontsize=7)
    axes[0, 0].set_yticklabels(labels_items, fontsize=7)
    cbar = plt.colorbar(im, ax=axes[0, 0])
    cbar.set_label("Similitud")

    stats_sim = calcular_estadisticas_similitud(df_similitud_items)
    vals = stats_sim["valores"]
    axes[0, 1].hist(
        vals,
        bins=35,
        alpha=0.75,
        color="#4C78A8",
        edgecolor="black",
        linewidth=0.4,
    )
    axes[0, 1].axvline(stats_sim["promedio"], color="red", linestyle="--", linewidth=2)
    axes[0, 1].set_title(f"{titulo_metodo}: distribucion de similitud", fontsize=11, fontweight="bold")
    axes[0, 1].set_xlabel(descripcion_similitud)
    axes[0, 1].set_ylabel("Frecuencia")
    axes[0, 1].grid(True, alpha=0.25)

    top_items = sorted(metricas["frecuencia_items"].items(), key=lambda x: x[1], reverse=True)[:10]
    labels_top = [contenidos_info.get(int(k), f"Content {k}")[:34] for k, _ in top_items]
    valores_top = [v for _, v in top_items]

    axes[1, 0].barh(labels_top[::-1], valores_top[::-1], color="#61DDAA")
    axes[1, 0].set_title(f"{titulo_metodo}: Top 10 contenidos recomendados", fontsize=11, fontweight="bold")
    axes[1, 0].set_xlabel("Veces recomendado")
    axes[1, 0].set_ylabel("Contenido")
    axes[1, 0].grid(True, axis="x", alpha=0.25)

    scores = metricas["scores"]
    promedio_scores = float(np.mean(scores)) if scores else 0.0
    minimo_scores = float(np.min(scores)) if scores else 0.0
    maximo_scores = float(np.max(scores)) if scores else 0.0

    lineas_kpi = [
        f"KPI {titulo_metodo.upper()}",
        "------------------------------",
        f"Usuarios: {metricas['usuarios_totales']}",
        f"Cobertura: {metricas['usuarios_con_recomendaciones']}/{metricas['usuarios_totales']}",
        f"Recomendaciones: {metricas['recomendaciones_totales']}",
        f"Promedio usuario: {metricas['recomendaciones_promedio']:.2f}",
        f"Diversidad: {metricas['diversidad_catalogo'] * 100:.1f}%",
        f"Novedad: {metricas['tasa_novedad'] * 100:.1f}%",
        "",
        "PUNTAJES",
        "------------------------------",
        f"Media: {promedio_scores:.2f}",
        f"Minimo: {minimo_scores:.2f}",
        f"Maximo: {maximo_scores:.2f}",
        "",
    ]
    lineas_kpi.extend(lineas_extra_kpi)

    axes[1, 1].text(
        0.03,
        0.98,
        "\n".join(lineas_kpi),
        transform=axes[1, 1].transAxes,
        va="top",
        ha="left",
        fontsize=10,
        family="monospace",
        bbox={"boxstyle": "round,pad=0.5", "facecolor": "#F7F7F7", "alpha": 0.95},
    )
    axes[1, 1].axis("off")

    plt.tight_layout()
    plt.savefig(output_png, dpi=300, bbox_inches="tight")
    plt.close()


def construir_tabla_comparativa(
    metricas_por_metodo: Dict[str, Dict[str, object]],
) -> pd.DataFrame:
    filas = []
    for metodo, metricas in metricas_por_metodo.items():
        scores = metricas["scores"]
        filas.append(
            {
                "Metodo": metodo,
                "Usuarios": metricas["usuarios_totales"],
                "Cobertura": f"{metricas['usuarios_con_recomendaciones']}/{metricas['usuarios_totales']}",
                "Recomendaciones_Totales": metricas["recomendaciones_totales"],
                "Promedio_Usuario": round(float(metricas["recomendaciones_promedio"]), 2),
                "Diversidad_Pct": round(float(metricas["diversidad_catalogo"] * 100), 2),
                "Novedad_Pct": round(float(metricas["tasa_novedad"] * 100), 2),
                "Score_Medio": round(float(np.mean(scores)) if scores else 0.0, 2),
            }
        )
    return pd.DataFrame(filas)


def main() -> None:
    print("\n")
    print_separador()
    print("RECOMMENDATION SYSTEM - SAKILA STREAMING (3 METODOS)")
    print_separador()

    client = None

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[DB_NAME]

        clientes_collection, nombre_clientes = resolver_coleccion(db, ["clientes", "customer"])
        eventos_collection, nombre_eventos = resolver_coleccion(db, ["eventos_streaming", "streamingevent"])
        catalogo_collection, nombre_catalogo = resolver_coleccion(db, ["catalogo", "catalog"])

        if clientes_collection is None or eventos_collection is None or catalogo_collection is None:
            raise RuntimeError("No se encontraron todas las colecciones necesarias")

        print("\nColecciones detectadas:")
        print(f"  - Clientes: {nombre_clientes}")
        print(f"  - Eventos: {nombre_eventos}")
        print(f"  - Catalogo: {nombre_catalogo}")

        print("\nCargando eventos de streaming...")
        eventos_data = list(eventos_collection.find({}, {"_id": 0}))
        print(f"  Eventos cargados: {len(eventos_data)}")

        if not eventos_data:
            raise RuntimeError("La coleccion de eventos esta vacia")

        df_eventos = normalizar_columnas_eventos(pd.DataFrame(eventos_data))

        print("\nCreando matriz usuario-contenido...")
        matriz_usuario_contenido = df_eventos.pivot_table(
            index="Customer_Key",
            columns="Content_Key",
            values="Minutes_Watched",
            aggfunc="sum",
            fill_value=0,
        ).sort_index().sort_index(axis=1)

        usuarios = list(matriz_usuario_contenido.index)
        contenidos = list(matriz_usuario_contenido.columns)

        zeros = int((matriz_usuario_contenido == 0).sum().sum())
        total_celdas = matriz_usuario_contenido.shape[0] * matriz_usuario_contenido.shape[1]
        sparsidad = (zeros / total_celdas) * 100 if total_celdas > 0 else 0

        print(
            f"  Matriz creada: {matriz_usuario_contenido.shape[0]} usuarios x {matriz_usuario_contenido.shape[1]} contenidos"
        )
        print(f"  Sparsidad: {sparsidad:.2f}%")

        contenidos_info, df_catalogo = cargar_catalogo_texto(catalogo_collection)

        # Asegura que todo contenido de la matriz tenga metadatos para tokenizacion.
        faltantes_catalogo = [c for c in contenidos if int(c) not in set(df_catalogo["Content_Key"]) ]
        if faltantes_catalogo:
            filas_faltantes = pd.DataFrame(
                {
                    "Content_Key": [int(c) for c in faltantes_catalogo],
                    "Titulo": [f"Content {int(c)}" for c in faltantes_catalogo],
                    "Texto": [f"content {int(c)}" for c in faltantes_catalogo],
                }
            )
            df_catalogo = pd.concat([df_catalogo, filas_faltantes], ignore_index=True)
            for c in faltantes_catalogo:
                contenidos_info[int(c)] = f"Content {int(c)}"

        df_catalogo = (
            df_catalogo.drop_duplicates(subset=["Content_Key"])
            .set_index("Content_Key")
            .reindex([int(c) for c in contenidos])
            .reset_index()
        )
        df_catalogo["Titulo"] = df_catalogo["Titulo"].fillna(df_catalogo["Content_Key"].apply(lambda x: f"Content {int(x)}"))
        df_catalogo["Texto"] = df_catalogo["Texto"].fillna(df_catalogo["Titulo"])

        os.makedirs("resultados_recomendaciones", exist_ok=True)

        # ------------------------------------------------------------------
        # SISTEMA 1: USUARIO-USUARIO
        # ------------------------------------------------------------------
        print("\nCalculando similitud Usuario-Usuario...")
        similitud_usuario_crudo = cosine_similarity(matriz_usuario_contenido.values.astype(float))
        similitud_usuario_crudo = np.clip(similitud_usuario_crudo, -1.0, 1.0)
        np.fill_diagonal(similitud_usuario_crudo, 1.0)

        df_sim_usuarios_crudo = pd.DataFrame(similitud_usuario_crudo, index=usuarios, columns=usuarios)
        df_sim_usuarios_ajustado = calcular_coseno_ajustado_usuario(matriz_usuario_contenido)

        stats_uu = calcular_estadisticas_similitud(df_sim_usuarios_ajustado)
        print(
            f"  Usuario-Usuario (ajustado): media={stats_uu['promedio']:.4f}, min={stats_uu['min']:.4f}, max={stats_uu['max']:.4f}"
        )

        rec_usuario_usuario = generar_recomendaciones_usuario_usuario(
            matriz_usuario_contenido,
            df_sim_usuarios_ajustado,
        )

        df_rec_uu = construir_dataframe_recomendaciones(rec_usuario_usuario, contenidos_info)
        output_csv_uu = "resultados_recomendaciones/recomendaciones_top10.csv"
        df_rec_uu.to_csv(output_csv_uu, index=False)

        metricas_uu = calcular_metricas_recomendacion(rec_usuario_usuario, matriz_usuario_contenido)
        output_png_uu = "resultados_recomendaciones/recomendacion_analysis.png"
        crear_visualizacion_usuario_usuario(
            df_sim_usuarios_crudo,
            df_sim_usuarios_ajustado,
            contenidos_info,
            metricas_uu,
            output_png_uu,
        )

        # ------------------------------------------------------------------
        # SISTEMA 2: ITEM-ITEM
        # ------------------------------------------------------------------
        print("\nCalculando similitud Item-Item...")
        df_sim_item_item = calcular_similitud_item_item(matriz_usuario_contenido)
        stats_ii = calcular_estadisticas_similitud(df_sim_item_item)
        print(
            f"  Item-Item: media={stats_ii['promedio']:.4f}, min={stats_ii['min']:.4f}, max={stats_ii['max']:.4f}"
        )

        rec_item_item = generar_recomendaciones_item_based(
            matriz_usuario_contenido,
            df_sim_item_item,
            "Item-Item",
        )

        df_rec_ii = construir_dataframe_recomendaciones(rec_item_item, contenidos_info)
        output_csv_ii = "resultados_recomendaciones/recomendaciones_item_item_top10.csv"
        df_rec_ii.to_csv(output_csv_ii, index=False)

        metricas_ii = calcular_metricas_recomendacion(rec_item_item, matriz_usuario_contenido)
        output_png_ii = "resultados_recomendaciones/recomendacion_item_item.png"
        crear_visualizacion_item_o_contenido(
            df_similitud_items=df_sim_item_item,
            contenidos_info=contenidos_info,
            metricas=metricas_ii,
            matriz_usuario_contenido=matriz_usuario_contenido,
            output_png=output_png_ii,
            titulo_metodo="Item-Item",
            descripcion_similitud="Similitud item-item",
            similitud_min=-1.0,
            similitud_max=1.0,
            cmap="coolwarm",
            lineas_extra_kpi=[
                "MODELO",
                "------------------------------",
                "Similitud entre contenidos por",
                "patrones de visualizacion de usuarios.",
            ],
        )

        # ------------------------------------------------------------------
        # SISTEMA 3: CONTENIDO POR TOKENIZACION
        # ------------------------------------------------------------------
        print("\nCalculando similitud por Tokenizacion de contenido...")
        df_sim_contenido, metadata_token = calcular_similitud_contenido_tokenizado(df_catalogo)
        stats_ct = calcular_estadisticas_similitud(df_sim_contenido)
        print(
            f"  Contenido-Tokenizacion: media={stats_ct['promedio']:.4f}, min={stats_ct['min']:.4f}, max={stats_ct['max']:.4f}"
        )

        rec_contenido_token = generar_recomendaciones_item_based(
            matriz_usuario_contenido,
            df_sim_contenido,
            "Contenido-Tokenizacion",
        )

        df_rec_ct = construir_dataframe_recomendaciones(rec_contenido_token, contenidos_info)
        output_csv_ct = "resultados_recomendaciones/recomendaciones_contenido_tokenizacion_top10.csv"
        df_rec_ct.to_csv(output_csv_ct, index=False)

        metricas_ct = calcular_metricas_recomendacion(rec_contenido_token, matriz_usuario_contenido)
        output_png_ct = "resultados_recomendaciones/recomendacion_contenido_tokenizacion.png"
        crear_visualizacion_item_o_contenido(
            df_similitud_items=df_sim_contenido,
            contenidos_info=contenidos_info,
            metricas=metricas_ct,
            matriz_usuario_contenido=matriz_usuario_contenido,
            output_png=output_png_ct,
            titulo_metodo="Contenido-Tokenizacion",
            descripcion_similitud="Similitud textual (TF-IDF + coseno)",
            similitud_min=0.0,
            similitud_max=1.0,
            cmap="YlOrRd",
            lineas_extra_kpi=[
                "MODELO",
                "------------------------------",
                f"Vocabulario TF-IDF: {int(metadata_token['vocabulario'])}",
                f"Dimensiones TF-IDF: {int(metadata_token['dimension_tfidf'])}",
                "Similitud por texto de titulo,",
                "descripcion, categorias y talento.",
            ],
        )

        # ------------------------------------------------------------------
        # TABLA COMPARATIVA
        # ------------------------------------------------------------------
        metricas_por_metodo = {
            "Usuario-Usuario": metricas_uu,
            "Item-Item": metricas_ii,
            "Contenido-Tokenizacion": metricas_ct,
        }
        df_comparacion = construir_tabla_comparativa(metricas_por_metodo)
        output_csv_comp = "resultados_recomendaciones/comparacion_sistemas.csv"
        df_comparacion.to_csv(output_csv_comp, index=False)

        print("\n")
        print_separador()
        print("SISTEMA DE RECOMENDACION COMPLETADO (3 METODOS)")
        print_separador()

        print("\nResumen rapido:")
        for metodo, metricas in metricas_por_metodo.items():
            print(
                f"  - {metodo}: cobertura={metricas['usuarios_con_recomendaciones']}/{metricas['usuarios_totales']}, "
                f"recomendaciones={metricas['recomendaciones_totales']}, "
                f"promedio_usuario={metricas['recomendaciones_promedio']:.2f}"
            )

        print("\nArchivos generados (CSV):")
        print(f"  1. {output_csv_uu}")
        print(f"  2. {output_csv_ii}")
        print(f"  3. {output_csv_ct}")
        print(f"  4. {output_csv_comp}")

        print("\nArchivos generados (graficos):")
        print(f"  1. {output_png_uu}  -> Usuario-Usuario")
        print(f"  2. {output_png_ii}  -> Item-Item")
        print(f"  3. {output_png_ct}  -> Contenido-Tokenizacion")

        print("\n")
        print_separador()
        print("PIPELINE COMPLETADO: Usuario-Usuario + Item-Item + Tokenizacion")
        print_separador()

    except Exception as exc:
        print("\nERROR en recommendation_system.py")
        print(f"Detalle: {exc}")
        raise
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()