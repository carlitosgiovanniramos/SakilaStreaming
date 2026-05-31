import pandas as pd
import os

BASE = r"c:\Users\Lenovo LOQ\Desktop\UTA\6 Software\Inteligencia de Negocios\Unidad 4"
OUT  = os.path.join(BASE, "BD normalizada")
os.makedirs(OUT, exist_ok=True)

def save(df, name):
    path = os.path.join(OUT, f"{name}.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"  [{name}.csv] {len(df):>7,} filas  {list(df.columns)}")

print("Leyendo archivos fuente...\n")

raw_date     = pd.read_excel(os.path.join(BASE, "DimDate.xls"))
raw_lang     = pd.read_excel(os.path.join(BASE, "DimLanguage_v2.xls"))
raw_provider = pd.read_excel(os.path.join(BASE, "dimProvider_v2.xls"))
raw_cat      = pd.read_excel(os.path.join(BASE, "DimCategory_v2.xls"))
raw_talent   = pd.read_excel(os.path.join(BASE, "dimTalent_v2.xls"))
raw_cc       = pd.read_excel(os.path.join(BASE, "dimContentCategory_v2.xls"))
raw_ct       = pd.read_excel(os.path.join(BASE, "dimContentTalent_v2.xls"))
raw_geo      = pd.read_excel(os.path.join(BASE, "DimGeography_v2 (1).xls"))
raw_cust     = pd.read_excel(os.path.join(BASE, "DimCustomer_v2.xls"))
raw_content  = pd.read_excel(os.path.join(BASE, "DimContent_v2.xlsx"))
raw_pm       = pd.read_excel(os.path.join(BASE, "dimPaymentMethod_v2.xls"))

print("="*60)
print("TABLAS SIN CAMBIOS")
print("="*60)

# ── DimDate ──────────────────────────────────────────────────
dim_date = raw_date.rename(columns={
    "Key": "DateKey", "Date": "Date", "Day Number": "DayNumber",
    "Day Name": "DayName", "Week Number": "WeekNumber",
    "Month Number": "MonthNumber", "Month Name": "MonthName",
    "Quarter": "Quarter", "Year Number": "YearNumber", "Is Weekend": "IsWeekend"
})
save(dim_date, "DimDate")

# ── DimLanguage ───────────────────────────────────────────────
dim_lang = raw_lang.rename(columns={"Key": "LanguageKey", "ISO": "ISO", "Name": "Name"})
save(dim_lang, "DimLanguage")

# ── DimProvider ───────────────────────────────────────────────
dim_prov = raw_provider.rename(columns={"Key": "ProviderKey", "ID": "ProviderID"})
save(dim_prov, "DimProvider")

# ── DimTalent ─────────────────────────────────────────────────
dim_talent = raw_talent.rename(columns={"Key": "TalentKey", "Actor ID": "ActorID",
    "First Name": "FirstName", "Last Name": "LastName"})
save(dim_talent, "DimTalent")

# ── dimContentCategory (tabla puente) ─────────────────────────
dim_cc = raw_cc.rename(columns={"Content Key": "ContentKey", "Category Key": "CategoryKey"})
save(dim_cc, "BridgeContentCategory")

# ── dimContentTalent (tabla puente) ───────────────────────────
dim_ct = raw_ct.rename(columns={"Content Key": "ContentKey", "Talent Key": "TalentKey",
    "Allocation Factor": "AllocationFactor", "Cast Order": "CastOrder"})
save(dim_ct, "BridgeContentTalent")

print()
print("="*60)
print("TABLAS NORMALIZADAS (3FN)")
print("="*60)

# ══════════════════════════════════════════════════════════════
# JERARQUÍA GEOGRÁFICA  (DimGeography → 4 tablas)
# ══════════════════════════════════════════════════════════════

# Mapa de continentes por país (de los 8 países detectados)
CONTINENT = {
    "United States": "North America", "Canada": "North America", "Mexico": "North America",
    "Brazil": "South America",
    "United Kingdom": "Europe", "France": "Europe", "Germany": "Europe",
    "Australia": "Oceania",
}

# DimCountry ──────────────────────────────────────────────────
countries_raw = raw_geo[["Country", "Country Slug"]].drop_duplicates().reset_index(drop=True)
countries_raw["CountryKey"] = range(1, len(countries_raw) + 1)
countries_raw["Continent"]  = countries_raw["Country"].map(CONTINENT)
dim_country = countries_raw[["CountryKey", "Country", "Country Slug", "Continent"]].rename(
    columns={"Country": "Name", "Country Slug": "ISOCode"})
save(dim_country, "DimCountry")

# Lookup country → key
country_key = dict(zip(countries_raw["Country"], countries_raw["CountryKey"]))

# DimState ────────────────────────────────────────────────────
states_raw = raw_geo[["State", "Country"]].drop_duplicates().reset_index(drop=True)
states_raw["StateKey"]   = range(1, len(states_raw) + 1)
states_raw["CountryKey"] = states_raw["Country"].map(country_key)
dim_state = states_raw[["StateKey", "State", "CountryKey"]].rename(columns={"State": "Name"})
save(dim_state, "DimState")

# Lookup (state, country) → StateKey
state_key = {(r.State, r.Country): r.StateKey for r in states_raw.itertuples()}

# DimCity ─────────────────────────────────────────────────────
cities_raw = raw_geo[["City", "State", "Country"]].drop_duplicates().reset_index(drop=True)
cities_raw["CityKey"]  = range(1, len(cities_raw) + 1)
cities_raw["StateKey"] = cities_raw.apply(lambda r: state_key.get((r.State, r.Country)), axis=1)
dim_city = cities_raw[["CityKey", "City", "StateKey"]].rename(columns={"City": "Name"})
save(dim_city, "DimCity")

# Lookup (city, state, country) → CityKey
city_key = {(r.City, r.State, r.Country): r.CityKey for r in cities_raw.itertuples()}

# DimAddress (reemplaza DimGeography) ─────────────────────────
raw_geo["CityKey"] = raw_geo.apply(
    lambda r: city_key.get((r["City"], r["State"], r["Country"])), axis=1)
dim_address = raw_geo[["Key", "ID", "Address", "Postal Code",
                        "latitude", "longitude", "timezone", "CityKey"]].rename(columns={
    "Key": "AddressKey", "ID": "AddressID", "Address": "Street",
    "Postal Code": "PostalCode", "latitude": "Latitude",
    "longitude": "Longitude", "timezone": "Timezone"
})
save(dim_address, "DimAddress")

# ══════════════════════════════════════════════════════════════
# DimCustomer  — eliminar campos derivados y métricas
# ══════════════════════════════════════════════════════════════
KEEP_CUST = ["Key", "ID", "First Name", "Last Name", "Email", "Gender", "Birth",
             "Geography Key", "Is Active", "Create Date",
             "preferred_language", "preferred_genre_key",
             "device_type", "registration_source"]
METRICS   = ["age_group", "months_as_customer", "churn_risk_score",
             "lifetime_value", "avg_session_minutes", "payment_reliability",
             "customer_segment"]

dim_cust = raw_cust[KEEP_CUST].rename(columns={
    "Key": "CustomerKey", "ID": "CustomerID",
    "First Name": "FirstName", "Last Name": "LastName",
    "Geography Key": "AddressKey",           # renombrado a AddressKey
    "Is Active": "IsActive", "Create Date": "CreateDate",
    "preferred_language": "LanguageKey",
    "preferred_genre_key": "PreferredCategoryKey",
    "device_type": "DeviceType", "registration_source": "RegistrationSource"
})
save(dim_cust, "DimCustomer")

# FactCustomerMetrics  — los KPIs/métricas separados
fact_metrics = raw_cust[["Key"] + METRICS].rename(columns={
    "Key": "CustomerKey",
    "age_group": "AgeGroup", "months_as_customer": "MonthsAsCustomer",
    "churn_risk_score": "ChurnRiskScore", "lifetime_value": "LifetimeValue",
    "avg_session_minutes": "AvgSessionMinutes",
    "payment_reliability": "PaymentReliability",
    "customer_segment": "CustomerSegment"
})
save(fact_metrics, "FactCustomerMetrics")

# ══════════════════════════════════════════════════════════════
# DimPaymentMethod  — estructura correcta, solo renombrar
# ══════════════════════════════════════════════════════════════
dim_pm = raw_pm.rename(columns={
    "Key": "PaymentMethodKey", "Card ID": "CardID",
    "Customer Key": "CustomerKey", "card_network": "CardNetwork",
    "card_subtype": "CardSubtype", "Card Expiry Date": "CardExpiryDate",
    "issuing_country": "IssuingCountry", "is_active": "IsActive"
})
save(dim_pm, "DimPaymentMethod")

# ══════════════════════════════════════════════════════════════
# DimContent  — extraer colecciones y eliminar revenue_known
# ══════════════════════════════════════════════════════════════

# DimCollection (nueva)
coll_raw = raw_content[raw_content["collection_name"].notna()][["collection_name"]].drop_duplicates().reset_index(drop=True)
coll_raw["CollectionKey"] = range(1, len(coll_raw) + 1)
dim_collection = coll_raw[["CollectionKey", "collection_name"]].rename(columns={"collection_name": "Name"})
save(dim_collection, "DimCollection")

coll_key = dict(zip(coll_raw["collection_name"], coll_raw["CollectionKey"]))

# DimContent limpia
raw_content["CollectionKey"] = raw_content["collection_name"].map(coll_key)
DROP_CONTENT = ["collection_name", "revenue_known"]
dim_content = raw_content.drop(columns=DROP_CONTENT).rename(columns={
    "Key": "ContentKey", "Film ID": "FilmID", "Title": "Title",
    "Description": "Description", "Release Year": "ReleaseYear",
    "Language Key": "LanguageKey", "Rating": "Rating", "Popularity": "Popularity",
    "Revenue": "Revenue", "Runtime": "Runtime", "Adult": "Adult",
    "content_type": "ContentType", "age_rating": "AgeRating", "budget": "Budget",
    "vote_count": "VoteCount", "origin_country": "OriginCountry",
    "content_source": "ContentSource", "streaming_added_date": "StreamingAddedDate",
    "tagline": "Tagline"
})
save(dim_content, "DimContent")

# ══════════════════════════════════════════════════════════════
# DimCategory  — eliminar métricas agregadas
# ══════════════════════════════════════════════════════════════
dim_cat = raw_cat.drop(columns=["content_count", "avg_rating"]).rename(columns={
    "Key": "CategoryKey", "tmdb_genre_id": "TmdbGenreID", "Name": "Name",
    "genre_group": "GenreGroup", "is_active": "IsActive",
    "sort_order": "SortOrder", "description": "Description"
})
save(dim_cat, "DimCategory")

# ══════════════════════════════════════════════════════════════
# RESUMEN FINAL
# ══════════════════════════════════════════════════════════════
print()
print("="*60)
print("RESUMEN")
print("="*60)
csvs = [f for f in os.listdir(OUT) if f.endswith(".csv")]
print(f"Tablas generadas: {len(csvs)}")
for f in sorted(csvs):
    rows = sum(1 for _ in open(os.path.join(OUT, f), encoding="utf-8-sig")) - 1
    print(f"  {f:<40} {rows:>8,} filas")
print(f"\nUbicacion: {OUT}")
