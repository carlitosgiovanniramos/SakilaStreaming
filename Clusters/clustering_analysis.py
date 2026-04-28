# ============================================================================
# ANALISIS DE CLUSTERING - SAKILA STREAMING
# ============================================================================
# PROPOSITO: Segmentar usuarios en 3 grupos segun su comportamiento de consumo.
#
# ENTRADA: MongoDB (clientes/customer, eventos_streaming/streamingevent)
# SALIDA: CSV + PNG con dashboard de clustering
# ============================================================================

import os
import warnings
from typing import Dict, List, Tuple

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pymongo import MongoClient
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

warnings.filterwarnings("ignore")


MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "SakilaStreaming"
N_CLUSTERS = 3
RANDOM_STATE = 42


def print_separador() -> None:
    print("=" * 80)


def resolver_coleccion(db, nombres_posibles: List[str]):
    disponibles = db.list_collection_names()
    for nombre in nombres_posibles:
        if nombre in disponibles:
            return db[nombre], nombre
    return None, None


def detectar_campos_eventos(eventos_collection) -> Tuple[str, str, str]:
    muestra = eventos_collection.find_one({}, {"_id": 0})
    if not muestra:
        raise RuntimeError("La coleccion de eventos esta vacia")

    candidatos_customer = ["Customer Key", "Customer_Key", "customer_key", "customerKey"]
    candidatos_minutes = ["Minutes Watched", "Minutes_Watched", "minutesWatched", "minutes_watched"]
    candidatos_streams = ["Streams Count", "Streams_Count", "streamsCount", "streams_count"]

    campo_customer = next((c for c in candidatos_customer if c in muestra), None)
    campo_minutes = next((c for c in candidatos_minutes if c in muestra), None)
    campo_streams = next((c for c in candidatos_streams if c in muestra), None)

    if campo_customer is None or campo_minutes is None:
        raise RuntimeError(
            "No se pudieron detectar campos requeridos (Customer Key / Minutes Watched)"
        )

    return campo_customer, campo_minutes, campo_streams


def obtener_features_por_usuario(eventos_collection) -> pd.DataFrame:
    campo_customer, campo_minutes, campo_streams = detectar_campos_eventos(eventos_collection)

    total_eventos_expr = {"$sum": 1} if campo_streams is None else {"$sum": f"${campo_streams}"}

    pipeline = [
        {
            "$group": {
                "_id": f"${campo_customer}",
                "total_eventos": total_eventos_expr,
                "total_minutos": {"$sum": f"${campo_minutes}"},
                "promedio_minutos": {"$avg": f"${campo_minutes}"},
            }
        }
    ]

    resultados = list(eventos_collection.aggregate(pipeline))
    if not resultados:
        raise RuntimeError("No se obtuvieron resultados de agregacion")

    df = pd.DataFrame(resultados).rename(columns={"_id": "Customer_Key"})
    df = df.fillna(0)

    df["Customer_Key"] = pd.to_numeric(df["Customer_Key"], errors="coerce")
    for col in ["total_eventos", "total_minutos", "promedio_minutos"]:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df = df.dropna(subset=["Customer_Key"])
    df["Customer_Key"] = df["Customer_Key"].astype(int)

    return df


def etiquetar_clusters(df_clustering: pd.DataFrame, features: List[str]) -> Dict[int, str]:
    cluster_stats = df_clustering.groupby("Cluster")[features].mean()

    cluster_mayor_actividad = int(cluster_stats["total_eventos"].idxmax())
    cluster_menor_actividad = int(cluster_stats["total_eventos"].idxmin())
    cluster_medio = [
        c for c in cluster_stats.index.tolist() if c not in [cluster_mayor_actividad, cluster_menor_actividad]
    ]

    etiquetas = {
        cluster_menor_actividad: "Dormant Users",
        cluster_mayor_actividad: "Power Users",
    }

    if cluster_medio:
        etiquetas[int(cluster_medio[0])] = "Regular Users"

    # Seguridad por si existe algun id no cubierto.
    for cluster_id in cluster_stats.index.tolist():
        if int(cluster_id) not in etiquetas:
            etiquetas[int(cluster_id)] = f"Cluster {cluster_id}"

    return etiquetas


def crear_dashboard_clustering(
    df_clustering: pd.DataFrame,
    features: List[str],
    etiquetas_cluster: Dict[int, str],
    cluster_stats: pd.DataFrame,
    centroides_original: pd.DataFrame,
    silhouette: float,
    inertia: float,
    output_path_png: str,
) -> None:
    color_por_etiqueta = {
        "Dormant Users": "#5B8FF9",
        "Regular Users": "#61DDAA",
        "Power Users": "#F6BD16",
    }

    orden_clusters = sorted(cluster_stats.index.tolist(), key=lambda c: cluster_stats.loc[c, "total_eventos"])

    fig, axes = plt.subplots(2, 2, figsize=(17, 12))

    # ---------------------------------------------------------------------
    # Grafico 1: Dispersion principal con centroides y leyenda semantica.
    # ---------------------------------------------------------------------
    for cluster_id in orden_clusters:
        etiqueta = etiquetas_cluster[int(cluster_id)]
        color = color_por_etiqueta.get(etiqueta, "#999999")
        datos = df_clustering[df_clustering["Cluster"] == cluster_id]

        axes[0, 0].scatter(
            datos["total_eventos"],
            datos["total_minutos"],
            s=85,
            alpha=0.78,
            color=color,
            edgecolors="black",
            linewidth=0.5,
            label=f"{etiqueta} ({len(datos)})",
        )

        cx = centroides_original.loc[cluster_id, "total_eventos"]
        cy = centroides_original.loc[cluster_id, "total_minutos"]
        axes[0, 0].scatter(
            cx,
            cy,
            marker="X",
            s=320,
            color=color,
            edgecolors="black",
            linewidth=1.2,
        )
        axes[0, 0].annotate(
            f"Centro {etiqueta}",
            xy=(cx, cy),
            xytext=(8, 8),
            textcoords="offset points",
            fontsize=9,
            fontweight="bold",
        )

    axes[0, 0].set_title("Segmentacion de usuarios: Eventos vs Minutos", fontsize=12, fontweight="bold")
    axes[0, 0].set_xlabel("Total eventos", fontsize=10, fontweight="bold")
    axes[0, 0].set_ylabel("Total minutos visualizados", fontsize=10, fontweight="bold")
    axes[0, 0].grid(True, alpha=0.25)
    axes[0, 0].legend(fontsize=9, loc="best")

    # ---------------------------------------------------------------------
    # Grafico 2: Tamano de segmentos con porcentaje.
    # ---------------------------------------------------------------------
    conteos = df_clustering["Cluster"].value_counts().to_dict()
    total_usuarios = len(df_clustering)
    labels = [etiquetas_cluster[int(c)] for c in orden_clusters]
    valores = [conteos.get(int(c), 0) for c in orden_clusters]
    colores = [color_por_etiqueta.get(etiquetas_cluster[int(c)], "#999999") for c in orden_clusters]

    bars = axes[0, 1].bar(labels, valores, color=colores, alpha=0.88, edgecolor="black", linewidth=1)
    axes[0, 1].set_title("Tamano de cada cluster", fontsize=12, fontweight="bold")
    axes[0, 1].set_ylabel("Cantidad de usuarios", fontsize=10, fontweight="bold")
    axes[0, 1].grid(True, axis="y", alpha=0.25)
    axes[0, 1].tick_params(axis="x", rotation=10)

    for bar, valor in zip(bars, valores):
        pct = (valor / total_usuarios) * 100 if total_usuarios > 0 else 0
        axes[0, 1].text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height(),
            f"{valor}\n({pct:.1f}%)",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    # ---------------------------------------------------------------------
    # Grafico 3: Heatmap de perfil por cluster (z-score por feature).
    # ---------------------------------------------------------------------
    perfil = cluster_stats.loc[orden_clusters, features].copy()
    perfil_z = (perfil - perfil.mean(axis=0)) / perfil.std(axis=0, ddof=0)
    perfil_z = perfil_z.replace([np.inf, -np.inf], 0).fillna(0)

    display_features = ["total_eventos", "total_minutos", "promedio_minutos"]
    display_labels = ["Eventos", "Minutos", "Min/Evento"]
    display_clusters = [etiquetas_cluster[int(c)] for c in orden_clusters]

    im = axes[1, 0].imshow(perfil_z[display_features], cmap="RdYlGn", aspect="auto", vmin=-1.5, vmax=1.5)
    axes[1, 0].set_title("Perfil relativo de clusters (z-score)", fontsize=12, fontweight="bold")
    axes[1, 0].set_xticks(range(len(display_labels)))
    axes[1, 0].set_xticklabels(display_labels, fontsize=10)
    axes[1, 0].set_yticks(range(len(display_clusters)))
    axes[1, 0].set_yticklabels(display_clusters, fontsize=10)

    for i in range(len(display_clusters)):
        for j in range(len(display_labels)):
            valor = perfil_z.iloc[i, j]
            axes[1, 0].text(j, i, f"{valor:+.2f}", ha="center", va="center", fontsize=9, fontweight="bold")

    cbar = plt.colorbar(im, ax=axes[1, 0])
    cbar.set_label("z-score")

    # ---------------------------------------------------------------------
    # Grafico 4: Panel KPI para lectura rapida en presentacion.
    # ---------------------------------------------------------------------
    resumen_lineas = [
        "KPI DEL CLUSTERING",
        "------------------------------",
        f"Usuarios segmentados: {total_usuarios}",
        f"Clusters: {N_CLUSTERS}",
        f"Silhouette score: {silhouette:.4f}",
        f"Inercia K-Means: {inertia:,.2f}",
        "",
        "INTERPRETACION RAPIDA",
        "------------------------------",
        f"- Power Users: {conteos.get(max(orden_clusters, key=lambda c: cluster_stats.loc[c, 'total_eventos']), 0)}",
        f"- Regular Users: {conteos.get(orden_clusters[1], 0) if len(orden_clusters) > 1 else 0}",
        f"- Dormant Users: {conteos.get(min(orden_clusters, key=lambda c: cluster_stats.loc[c, 'total_eventos']), 0)}",
        "",
        "PROMEDIOS POR CLUSTER",
        "------------------------------",
    ]

    for cluster_id in orden_clusters:
        etiqueta = etiquetas_cluster[int(cluster_id)]
        stats = cluster_stats.loc[cluster_id]
        resumen_lineas.append(
            f"{etiqueta}: E={stats['total_eventos']:.0f}, M={stats['total_minutos']:.0f}, MP={stats['promedio_minutos']:.1f}"
        )

    axes[1, 1].text(
        0.03,
        0.98,
        "\n".join(resumen_lineas),
        transform=axes[1, 1].transAxes,
        va="top",
        ha="left",
        fontsize=10,
        family="monospace",
        bbox={"boxstyle": "round,pad=0.5", "facecolor": "#F7F7F7", "alpha": 0.95},
    )
    axes[1, 1].axis("off")

    plt.tight_layout()
    plt.savefig(output_path_png, dpi=300, bbox_inches="tight")
    plt.close()


def main() -> None:
    print("\n")
    print_separador()
    print("CLUSTERING ANALYSIS - SAKILA STREAMING")
    print_separador()

    client = None

    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
        db = client[DB_NAME]

        clientes_collection, nombre_clientes = resolver_coleccion(db, ["clientes", "customer"])
        eventos_collection, nombre_eventos = resolver_coleccion(db, ["eventos_streaming", "streamingevent"])

        if clientes_collection is None or eventos_collection is None:
            raise RuntimeError("No se encontraron las colecciones necesarias para clustering")

        print("\nColecciones detectadas:")
        print(f"  - Clientes: {nombre_clientes}")
        print(f"  - Eventos: {nombre_eventos}")

        print("\nExtrayendo features por usuario...")
        df_clustering = obtener_features_por_usuario(eventos_collection)
        print(f"  Usuarios procesados: {len(df_clustering)}")

        features = ["total_eventos", "total_minutos", "promedio_minutos"]
        print("\nPrimeras filas de features:")
        print(df_clustering[["Customer_Key"] + features].head().to_string(index=False))

        print("\nNormalizando features...")
        X = df_clustering[features].copy()
        scaler = StandardScaler()
        X_normalizado = scaler.fit_transform(X)

        print("\nAplicando K-Means (k=3)...")
        kmeans = KMeans(n_clusters=N_CLUSTERS, random_state=RANDOM_STATE, n_init=10)
        tipo_cluster = kmeans.fit_predict(X_normalizado)
        df_clustering["Cluster"] = tipo_cluster

        cluster_stats = df_clustering.groupby("Cluster")[features].mean()
        etiquetas_cluster = etiquetar_clusters(df_clustering, features)
        df_clustering["Cluster_Etiqueta"] = df_clustering["Cluster"].map(etiquetas_cluster)

        # Centroides en escala original para que la grafica sea interpretable.
        centroides_original = pd.DataFrame(
            scaler.inverse_transform(kmeans.cluster_centers_),
            columns=features,
            index=range(N_CLUSTERS),
        )

        silhouette = silhouette_score(X_normalizado, tipo_cluster)
        inertia = float(kmeans.inertia_)

        print("\nDistribucion por cluster:")
        for cluster_id in sorted(df_clustering["Cluster"].unique()):
            etiqueta = etiquetas_cluster[int(cluster_id)]
            cantidad = int((df_clustering["Cluster"] == cluster_id).sum())
            print(f"  - Cluster {cluster_id} ({etiqueta}): {cantidad} usuarios")

        print("\nMetricas de calidad:")
        print(f"  - Silhouette score: {silhouette:.4f}")
        print(f"  - Inercia: {inertia:,.2f}")

        print("\nGenerando dashboard de clustering...")
        os.makedirs("resultados_clustering", exist_ok=True)
        output_path_png = "resultados_clustering/clustering_analysis.png"
        crear_dashboard_clustering(
            df_clustering=df_clustering,
            features=features,
            etiquetas_cluster=etiquetas_cluster,
            cluster_stats=cluster_stats,
            centroides_original=centroides_original,
            silhouette=silhouette,
            inertia=inertia,
            output_path_png=output_path_png,
        )

        output_csv = "resultados_clustering/clustering_results.csv"
        df_clustering.sort_values(["Cluster", "Customer_Key"]).to_csv(output_csv, index=False)

        print("\n")
        print_separador()
        print("CLUSTERING COMPLETADO")
        print_separador()
        print("\nArchivos generados:")
        print(f"  1. {output_csv}")
        print(f"  2. {output_path_png}")

        print("\nResumen:")
        print(f"  Total usuarios: {len(df_clustering)}")
        print(f"  Total clusters: {N_CLUSTERS}")
        print(f"  Silhouette score: {silhouette:.4f}")

        power_cluster = max(cluster_stats.index.tolist(), key=lambda c: cluster_stats.loc[c, "total_eventos"])
        dormant_cluster = min(cluster_stats.index.tolist(), key=lambda c: cluster_stats.loc[c, "total_eventos"])
        regular_candidates = [c for c in cluster_stats.index.tolist() if c not in [power_cluster, dormant_cluster]]

        print(f"  - Power Users: {(df_clustering['Cluster'] == power_cluster).sum()}")
        print(
            f"  - Regular Users: {(df_clustering['Cluster'] == regular_candidates[0]).sum() if regular_candidates else 0}"
        )
        print(f"  - Dormant Users: {(df_clustering['Cluster'] == dormant_cluster).sum()}")

        print("\nProximo paso: recommendation_system.py")
        print_separador()

    except Exception as exc:
        print("\nERROR en clustering_analysis.py")
        print(f"Detalle: {exc}")
        raise
    finally:
        if client is not None:
            client.close()


if __name__ == "__main__":
    main()