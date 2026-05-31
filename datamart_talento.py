import pandas as pd
import numpy as np
import os

BASE = r"c:\Users\Lenovo LOQ\Desktop\UTA\6 Software\Inteligencia de Negocios\Unidad 4\BD normalizada"
OUT  = os.path.join(BASE, "Datamart_Talento.csv")

print("Leyendo tablas normalizadas...")
talent     = pd.read_csv(os.path.join(BASE, "DimTalent.csv"))
bridge_tal = pd.read_csv(os.path.join(BASE, "BridgeContentTalent.csv"))
content    = pd.read_csv(os.path.join(BASE, "DimContent.csv"))
bridge_cat = pd.read_csv(os.path.join(BASE, "BridgeContentCategory.csv"))
cat        = pd.read_csv(os.path.join(BASE, "DimCategory.csv"))
collection = pd.read_csv(os.path.join(BASE, "DimCollection.csv"))

# ── 1. Enriquecer contenido con colección y categoría principal ───────────────
print("Enriqueciendo contenido con categorias...")

# Categoría principal por contenido
cat_primary = (
    bridge_cat
    .merge(cat[["CategoryKey","Name","GenreGroup"]], on="CategoryKey")
    .groupby("ContentKey")
    .first()
    .reset_index()
    [["ContentKey","Name","GenreGroup"]]
    .rename(columns={"Name":"PrimaryCategory","GenreGroup":"PrimaryGenreGroup"})
)

content_full = (
    content[["ContentKey","Title","ReleaseYear","ContentType","Rating",
             "Popularity","Revenue","Budget","VoteCount","Runtime",
             "OriginCountry","CollectionKey"]]
    .merge(cat_primary, on="ContentKey", how="left")
    .merge(collection[["CollectionKey","Name"]].rename(columns={"Name":"CollectionName"}),
           on="CollectionKey", how="left")
)
content_full["ROI"] = (
    (content_full["Revenue"] - content_full["Budget"])
    / content_full["Budget"].replace(0, np.nan)
).round(4)

# ── 2. Unir bridge con contenido ──────────────────────────────────────────────
print("Uniendo talento con contenido...")
bridge_full = bridge_tal.merge(content_full, on="ContentKey", how="left")

# ── 3. Agregar por talento (grano: 1 fila = 1 actor/talento) ─────────────────
print("Agregando métricas por talento...")

# Mejor contenido por Revenue
best_content = (
    bridge_full.sort_values("Revenue", ascending=False)
    .groupby("TalentKey")
    .first()
    .reset_index()
    [["TalentKey","Title","Revenue","Rating"]]
    .rename(columns={"Title":"BestRevenueTitle",
                     "Revenue":"BestRevenue",
                     "Rating":"BestRevenueRating"})
)

# Mejor contenido por Rating
best_rated = (
    bridge_full.sort_values("Rating", ascending=False)
    .groupby("TalentKey")
    .first()
    .reset_index()
    [["TalentKey","Title","Rating"]]
    .rename(columns={"Title":"BestRatedTitle","Rating":"BestRating"})
)

# Categoría más frecuente del actor
top_category = (
    bridge_full.groupby("TalentKey")["PrimaryCategory"]
    .agg(lambda x: x.value_counts().index[0] if x.notna().any() else "Unknown")
    .reset_index()
    .rename(columns={"PrimaryCategory":"TopCategory"})
)

top_genre = (
    bridge_full.groupby("TalentKey")["PrimaryGenreGroup"]
    .agg(lambda x: x.value_counts().index[0] if x.notna().any() else "Unknown")
    .reset_index()
    .rename(columns={"PrimaryGenreGroup":"TopGenreGroup"})
)

# Títulos más conocidos (top 3 por revenue)
top_titles = (
    bridge_full.sort_values("Revenue", ascending=False)
    .groupby("TalentKey")["Title"]
    .agg(lambda x: " | ".join(x.dropna().head(3).astype(str).values))
    .reset_index()
    .rename(columns={"Title":"TopTitles"})
)

# Agregación principal
tal_agg = (
    bridge_full.groupby("TalentKey")
    .agg(
        # Filmografía
        ContentCount         = ("ContentKey",        "count"),
        UniqueContentTypes   = ("ContentType",        "nunique"),
        MovieCount           = ("ContentType",        lambda x: (x=="Movie").sum()),
        SeriesCount          = ("ContentType",        lambda x: (x=="Series").sum()),

        # Métricas de calidad
        AvgRating            = ("Rating",             "mean"),
        MedianRating         = ("Rating",             "median"),
        MaxRating            = ("Rating",             "max"),

        # Métricas financieras
        TotalRevenue         = ("Revenue",            "sum"),
        AvgRevenue           = ("Revenue",            "mean"),
        TotalBudget          = ("Budget",             "sum"),
        AvgROI               = ("ROI",                "mean"),

        # Popularidad del contenido
        AvgPopularity        = ("Popularity",         "mean"),
        MaxPopularity        = ("Popularity",         "max"),
        TotalVoteCount       = ("VoteCount",          "sum"),

        # Rol en el reparto
        AvgCastOrder         = ("CastOrder",          "mean"),
        LeadRoleCount        = ("CastOrder",          lambda x: (x==1).sum()),
        AvgAllocationFactor  = ("AllocationFactor",   "mean"),

        # Rango temporal
        FirstReleaseYear     = ("ReleaseYear",        "min"),
        LastReleaseYear      = ("ReleaseYear",        "max"),

        # Colecciones
        CollectionCount      = ("CollectionName",     lambda x: x.notna().sum()),
    )
    .reset_index()
)

# Redondear
for col in ["AvgRating","MedianRating","AvgRevenue","AvgROI",
            "AvgPopularity","AvgCastOrder","AvgAllocationFactor"]:
    tal_agg[col] = tal_agg[col].round(3)

# ── 4. JOIN con DimTalent ──────────────────────────────────────────────────────
print("Uniendo con DimTalent...")
talent["FullName"] = talent["FirstName"].astype(str) + " " + talent["LastName"].astype(str)

dm = (
    tal_agg
    .merge(talent[["TalentKey","FullName","FirstName","LastName",
                   "nationality","gender","birth_year","death_year",
                   "is_active","known_for_dept","popularity_score",
                   "awards_count","active_since_year"]],
           on="TalentKey")
    .merge(best_content,  on="TalentKey", how="left")
    .merge(best_rated,    on="TalentKey", how="left")
    .merge(top_category,  on="TalentKey", how="left")
    .merge(top_genre,     on="TalentKey", how="left")
    .merge(top_titles,    on="TalentKey", how="left")
)

# ── 5. Columnas derivadas ──────────────────────────────────────────────────────
dm["CareerSpanYears"]  = dm["LastReleaseYear"] - dm["FirstReleaseYear"]
dm["IsStarActor"]      = (
    (dm["awards_count"] >= 1) | (dm["popularity_score"] >= 50)
).astype(int)
dm["LeadRoleRate"]     = (dm["LeadRoleCount"] / dm["ContentCount"]).round(4)
dm["AvgRevenuePerFilm"]= (dm["TotalRevenue"]  / dm["ContentCount"]).round(0)

dm["TalentTier"] = pd.cut(
    dm["popularity_score"],
    bins=[0, 10, 30, 60, 100, 1000],
    labels=["Emerging", "Known", "Popular", "Star", "Superstar"]
)

dm["ContentProfileROI"] = pd.cut(
    dm["AvgROI"],
    bins=[-np.inf, 0, 0.5, 1.5, np.inf],
    labels=["Losing", "Modest", "Profitable", "Blockbuster"]
)

# ── 6. Orden final ─────────────────────────────────────────────────────────────
COLS = [
    # Identificador
    "TalentKey", "FullName", "FirstName", "LastName",
    # Perfil personal
    "nationality", "gender", "birth_year", "death_year",
    "is_active", "known_for_dept", "TalentTier",
    "popularity_score", "awards_count", "active_since_year", "IsStarActor",
    # Filmografía
    "ContentCount", "MovieCount", "SeriesCount", "UniqueContentTypes",
    "FirstReleaseYear", "LastReleaseYear", "CareerSpanYears",
    "CollectionCount",
    # Calidad
    "AvgRating", "MedianRating", "MaxRating",
    "BestRatedTitle", "BestRating",
    # Financiero
    "TotalRevenue", "AvgRevenue", "AvgRevenuePerFilm",
    "TotalBudget", "AvgROI", "ContentProfileROI",
    "BestRevenueTitle", "BestRevenue", "BestRevenueRating",
    # Popularidad
    "AvgPopularity", "MaxPopularity", "TotalVoteCount",
    # Rol en reparto
    "AvgCastOrder", "LeadRoleCount", "LeadRoleRate", "AvgAllocationFactor",
    # Especialización
    "TopCategory", "TopGenreGroup", "TopTitles",
]
dm = dm[COLS]
dm = dm.sort_values("TotalRevenue", ascending=False)

# ── 7. Guardar ─────────────────────────────────────────────────────────────────
dm.to_csv(OUT, index=False, encoding="utf-8-sig")

# ── 8. Reporte ─────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Datamart_Talento.csv generado")
print(f"{'='*60}")
print(f"Filas    : {len(dm):,}  (1 fila = 1 actor/talento)")
print(f"Columnas : {len(dm.columns)}")
print(f"Ubicacion: {OUT}")

print(f"\n--- Distribucion TalentTier ---")
print(dm["TalentTier"].value_counts().to_string())

print(f"\n--- Distribucion ContentProfileROI ---")
print(dm["ContentProfileROI"].value_counts().to_string())

print(f"\n--- Top 10 actores por TotalRevenue ---")
top = dm.nlargest(10,"TotalRevenue")[
    ["FullName","ContentCount","TotalRevenue","AvgRating","TalentTier","TopCategory"]]
print(top.to_string(index=False))

print(f"\n--- Top 5 actores por AvgRating (min 5 peliculas) ---")
top_r = dm[dm["ContentCount"]>=5].nlargest(5,"AvgRating")[
    ["FullName","ContentCount","AvgRating","TotalRevenue","TopCategory"]]
print(top_r.to_string(index=False))

print(f"\n--- Nulos por columna ---")
nulls = dm.isnull().sum()
nulls = nulls[nulls > 0]
print(nulls.to_string() if len(nulls) else "Sin nulos")
