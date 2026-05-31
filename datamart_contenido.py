import pandas as pd
import numpy as np
import os
from datetime import date

BASE = r"c:\Users\Lenovo LOQ\Desktop\UTA\6 Software\Inteligencia de Negocios\Unidad 4\BD normalizada"
OUT  = os.path.join(BASE, "Datamart_Contenido.csv")

print("Leyendo tablas normalizadas...")
content    = pd.read_csv(os.path.join(BASE, "DimContent.csv"))
lang       = pd.read_csv(os.path.join(BASE, "DimLanguage.csv"))
collection = pd.read_csv(os.path.join(BASE, "DimCollection.csv"))
dim_date   = pd.read_csv(os.path.join(BASE, "DimDate.csv"))
bridge_cat = pd.read_csv(os.path.join(BASE, "BridgeContentCategory.csv"))
cat        = pd.read_csv(os.path.join(BASE, "DimCategory.csv"))
bridge_tal = pd.read_csv(os.path.join(BASE, "BridgeContentTalent.csv"))
talent     = pd.read_csv(os.path.join(BASE, "DimTalent.csv"))

# ── 1. DimLanguage ─────────────────────────────────────────────────────────────
lang_clean = lang.rename(columns={"Name": "Language", "ISO": "LanguageISO"})

# ── 2. DimCollection (nullable) ───────────────────────────────────────────────
coll_clean = collection.rename(columns={"Name": "CollectionName"})

# ── 3. DimDate → atributos de StreamingAddedDate ──────────────────────────────
# Extraemos atributos de fecha directamente desde StreamingAddedDate
content["StreamingAddedDate"] = pd.to_datetime(content["StreamingAddedDate"], errors="coerce")
today = pd.Timestamp(date.today())

content["StreamingYear"]    = content["StreamingAddedDate"].dt.year
content["StreamingMonth"]   = content["StreamingAddedDate"].dt.month
content["StreamingQuarter"] = content["StreamingAddedDate"].dt.quarter
content["DaysSinceStreaming"] = (today - content["StreamingAddedDate"]).dt.days

# Desde DimDate: obtener nombre de mes y si es fin de semana para ReleaseYear
# Usamos el primer día de cada año como proxy del año de estreno
release_dates = (
    dim_date[dim_date["DayNumber"] == 1]
    .groupby("YearNumber")
    .first()
    .reset_index()
    [["YearNumber", "Quarter", "MonthName"]]
    .rename(columns={"YearNumber": "ReleaseYear",
                     "Quarter":   "ReleaseQuarter",
                     "MonthName": "ReleaseMonthRef"})
)
content = content.merge(release_dates, on="ReleaseYear", how="left")

# ── 4. Categorías: M:N → agregar por contenido ────────────────────────────────
print("Agregando categorias por contenido...")
cat_enriched = bridge_cat.merge(
    cat[["CategoryKey", "Name", "GenreGroup"]],
    on="CategoryKey"
)

cat_agg = (
    cat_enriched.groupby("ContentKey")
    .agg(
        CategoryCount    = ("Name",       "count"),
        PrimaryCategory  = ("Name",       "first"),  # primera categoría asignada
        AllCategories    = ("Name",       lambda x: " | ".join(x.values)),
        PrimaryGenreGroup= ("GenreGroup", "first"),
        AllGenreGroups   = ("GenreGroup", lambda x: " | ".join(x.unique())),
    )
    .reset_index()
)

# ── 5. Talento: M:N → agregar por contenido ───────────────────────────────────
print("Agregando talento por contenido...")
tal_enriched = (
    bridge_tal
    .merge(talent[["TalentKey", "FirstName", "LastName",
                   "nationality", "gender", "popularity_score", "awards_count"]],
           on="TalentKey")
)
tal_enriched["FullName"] = tal_enriched["FirstName"] + " " + tal_enriched["LastName"]

# Actor principal = CastOrder más bajo (=1 es el protagonista)
top_actor = (
    tal_enriched.sort_values("CastOrder")
    .groupby("ContentKey")
    .first()
    .reset_index()
    [["ContentKey", "FullName", "nationality", "gender", "popularity_score"]]
    .rename(columns={
        "FullName":         "TopActorName",
        "nationality":      "TopActorNationality",
        "gender":           "TopActorGender",
        "popularity_score": "TopActorPopularity",
    })
)

tal_agg = (
    tal_enriched.groupby("ContentKey")
    .agg(
        CastSize          = ("TalentKey",        "count"),
        AvgCastPopularity = ("popularity_score",  "mean"),
        TotalCastAwards   = ("awards_count",      "sum"),
        AllActors         = ("FullName",          lambda x: " | ".join(x.dropna().head(5).astype(str).values)),
    )
    .reset_index()
)
tal_agg["AvgCastPopularity"] = tal_agg["AvgCastPopularity"].round(2)

tal_final = tal_agg.merge(top_actor, on="ContentKey", how="left")

# ── 6. JOIN principal ──────────────────────────────────────────────────────────
print("Construyendo datamart...")
dm = (
    content
    .merge(lang_clean[["LanguageKey", "Language", "LanguageISO"]],
           on="LanguageKey", how="left")
    .merge(coll_clean, on="CollectionKey", how="left")
    .merge(cat_agg,    on="ContentKey",    how="left")
    .merge(tal_final,  on="ContentKey",    how="left")
)

# ── 7. Medidas derivadas ───────────────────────────────────────────────────────
dm["ROI"]              = ((dm["Revenue"] - dm["Budget"]) / dm["Budget"].replace(0, np.nan)).round(4)
dm["ProfitMargin"]     = ((dm["Revenue"] - dm["Budget"]) / dm["Revenue"].replace(0, np.nan)).round(4)
dm["RevenuePerMinute"] = (dm["Revenue"] / dm["Runtime"].replace(0, np.nan)).round(2)
dm["BudgetCategory"]   = pd.cut(dm["Budget"],
                                 bins=[0, 5e6, 20e6, 60e6, 200e6, 1e10],
                                 labels=["Micro", "Low", "Mid", "High", "Blockbuster"])
dm["RatingCategory"]   = pd.cut(dm["Rating"],
                                 bins=[0, 5, 6.5, 7.5, 8.5, 10],
                                 labels=["Poor", "Average", "Good", "Great", "Masterpiece"])
dm["IsInCollection"]   = dm["CollectionName"].notna().astype(int)
dm["IsAdult"]          = dm["Adult"].astype(int)

# ── 8. Orden final de columnas ─────────────────────────────────────────────────
COLS = [
    # Identificador
    "ContentKey", "FilmID", "Title",
    # Medidas principales
    "Revenue", "Budget", "ROI", "ProfitMargin", "RevenuePerMinute",
    "Rating", "RatingCategory", "VoteCount", "Popularity", "Runtime",
    # Clasificación
    "ContentType", "AgeRating", "IsAdult", "BudgetCategory",
    # Dimensión Idioma
    "Language", "LanguageISO",
    # Dimensión Colección
    "IsInCollection", "CollectionName",
    # Dimensión Fecha
    "ReleaseYear", "ReleaseQuarter",
    "StreamingYear", "StreamingMonth", "StreamingQuarter", "DaysSinceStreaming",
    # Dimensión Categoría
    "CategoryCount", "PrimaryCategory", "AllCategories",
    "PrimaryGenreGroup", "AllGenreGroups",
    # Dimensión Talento
    "CastSize", "AvgCastPopularity", "TotalCastAwards",
    "TopActorName", "TopActorNationality", "TopActorGender", "TopActorPopularity",
    "AllActors",
    # Otros
    "OriginCountry", "ContentSource", "Tagline",
]
dm = dm[COLS]

# ── 9. Guardar ─────────────────────────────────────────────────────────────────
dm.to_csv(OUT, index=False, encoding="utf-8-sig")

# ── 10. Reporte ────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Datamart_Contenido.csv generado")
print(f"{'='*60}")
print(f"Filas    : {len(dm):,}")
print(f"Columnas : {len(dm.columns)}")
print(f"Ubicacion: {OUT}")

print(f"\n--- Medidas principales (estadisticas) ---")
medidas = ["Revenue", "Budget", "ROI", "Rating", "Popularity", "VoteCount", "Runtime"]
print(dm[medidas].describe().round(2).to_string())

print(f"\n--- Distribucion por ContentType ---")
print(dm["ContentType"].value_counts().to_string())

print(f"\n--- Distribucion por RatingCategory ---")
print(dm["RatingCategory"].value_counts().sort_index().to_string())

print(f"\n--- Distribucion por BudgetCategory ---")
print(dm["BudgetCategory"].value_counts().sort_index().to_string())

print(f"\n--- Top 5 contenidos por Revenue ---")
print(dm.nlargest(5, "Revenue")[["Title","Revenue","Budget","ROI","Rating","PrimaryCategory"]].to_string())

print(f"\n--- Nulos por columna ---")
nulls = dm.isnull().sum()
nulls = nulls[nulls > 0]
print(nulls.to_string() if len(nulls) else "Sin nulos criticos")
