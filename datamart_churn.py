import pandas as pd
import os

BASE = r"c:\Users\Lenovo LOQ\Desktop\UTA\6 Software\Inteligencia de Negocios\Unidad 4\BD normalizada"
OUT  = os.path.join(BASE, "Datamart_Churn.csv")

print("Leyendo tablas normalizadas...")
fact    = pd.read_csv(os.path.join(BASE, "FactCustomerMetrics.csv"))
cust    = pd.read_csv(os.path.join(BASE, "DimCustomer.csv"))
addr    = pd.read_csv(os.path.join(BASE, "DimAddress.csv"))
city    = pd.read_csv(os.path.join(BASE, "DimCity.csv"))
state   = pd.read_csv(os.path.join(BASE, "DimState.csv"))
country = pd.read_csv(os.path.join(BASE, "DimCountry.csv"))
lang    = pd.read_csv(os.path.join(BASE, "DimLanguage.csv"))
cat     = pd.read_csv(os.path.join(BASE, "DimCategory.csv"))
pm      = pd.read_csv(os.path.join(BASE, "DimPaymentMethod.csv"))

# ── 1. Agregar DimPaymentMethod por cliente ────────────────────────────────────
pm_agg = (
    pm.groupby("CustomerKey")
    .agg(
        NumPaymentMethods   = ("PaymentMethodKey", "count"),
        ActivePaymentMethods= ("IsActive", "sum"),
        PrimaryCardNetwork  = ("CardNetwork", lambda x: x.value_counts().index[0]),
        HasCreditCard       = ("CardSubtype", lambda x: int("credit" in x.values)),
        HasDebitCard        = ("CardSubtype", lambda x: int("debit" in x.values or
                                                             "Debit Card" in x.values)),
    )
    .reset_index()
)

# ── 2. Jerarquía geográfica ────────────────────────────────────────────────────
city_full = (
    city
    .merge(state.rename(columns={"Name": "StateName"}),  on="StateKey")
    .merge(country.rename(columns={"Name": "CountryName"}), on="CountryKey")
    [["CityKey", "Name", "StateName", "CountryName", "ISOCode", "Continent"]]
    .rename(columns={"Name": "CityName"})
)

addr_full = (
    addr[["AddressKey", "CityKey", "Latitude", "Longitude", "Timezone"]]
    .merge(city_full, on="CityKey")
)

# ── 3. Dimensiones de lenguaje y categoría ────────────────────────────────────
lang_clean = lang.rename(columns={"Name": "PreferredLanguage", "ISO": "LanguageISO"})
cat_clean  = cat[["CategoryKey", "Name", "GenreGroup"]].rename(columns={
    "Name": "PreferredGenre", "GenreGroup": "PreferredGenreGroup"
})

# ── 4. JOIN principal ──────────────────────────────────────────────────────────
print("Construyendo datamart...")

dm = (
    fact
    # → DimCustomer
    .merge(cust[[
        "CustomerKey", "FirstName", "LastName", "Gender", "Birth",
        "IsActive", "CreateDate", "DeviceType", "RegistrationSource",
        "AddressKey", "LanguageKey", "PreferredCategoryKey"
    ]], on="CustomerKey", how="left")

    # → DimAddress + jerarquía geográfica
    .merge(addr_full[[
        "AddressKey", "Latitude", "Longitude", "Timezone",
        "CityName", "StateName", "CountryName", "ISOCode", "Continent"
    ]], on="AddressKey", how="left")

    # → DimLanguage
    .merge(lang_clean[["LanguageKey", "PreferredLanguage", "LanguageISO"]],
           on="LanguageKey", how="left")

    # → DimCategory
    .merge(cat_clean, left_on="PreferredCategoryKey",
           right_on="CategoryKey", how="left")
    .drop(columns=["CategoryKey"])

    # → DimPaymentMethod agregado
    .merge(pm_agg, on="CustomerKey", how="left")
)

# ── 5. Columnas derivadas útiles ───────────────────────────────────────────────
from datetime import date
today = pd.Timestamp(date.today())

dm["Age"]             = (today - pd.to_datetime(dm["Birth"])).dt.days // 365
dm["AgeGroup"]        = pd.cut(dm["Age"],
                                bins=[0, 25, 35, 50, 65, 120],
                                labels=["Gen_Z", "Millennial", "Gen_X", "Boomer", "Senior"])
dm["TenureYears"]     = (dm["MonthsAsCustomer"] / 12).round(1)
dm["IsChurned"]       = (dm["IsActive"] == 0).astype(int)   # variable objetivo

# ── 6. Orden final de columnas ─────────────────────────────────────────────────
COLS = [
    # Identificador
    "CustomerKey",
    # Variable objetivo
    "IsChurned", "IsActive",
    # Datos demográficos
    "Gender", "Age", "AgeGroup", "Birth",
    # Comportamiento / métricas
    "MonthsAsCustomer", "TenureYears",
    "LifetimeValue", "AvgSessionMinutes",
    "ChurnRiskScore", "PaymentReliability",
    "CustomerSegment",
    # Geografía
    "CityName", "StateName", "CountryName", "ISOCode", "Continent",
    "Latitude", "Longitude", "Timezone",
    # Preferencias
    "PreferredLanguage", "LanguageISO",
    "PreferredGenre", "PreferredGenreGroup",
    # Dispositivo y canal
    "DeviceType", "RegistrationSource",
    # Métodos de pago
    "NumPaymentMethods", "ActivePaymentMethods",
    "PrimaryCardNetwork", "HasCreditCard", "HasDebitCard",
    # Fechas de contexto
    "CreateDate",
]

dm = dm[COLS]

# ── 7. Guardar ─────────────────────────────────────────────────────────────────
dm.to_csv(OUT, index=False, encoding="utf-8-sig")

# ── 8. Reporte ─────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Datamart_Churn.csv generado")
print(f"{'='*60}")
print(f"Filas    : {len(dm):,}")
print(f"Columnas : {len(dm.columns)}")
print(f"Ubicacion: {OUT}")

print(f"\n--- Distribucion variable objetivo ---")
churn = dm["IsChurned"].value_counts()
print(f"Activos  (IsChurned=0): {churn.get(0,0):,}  ({churn.get(0,0)/len(dm)*100:.1f}%)")
print(f"Churned  (IsChurned=1): {churn.get(1,0):,}  ({churn.get(1,0)/len(dm)*100:.1f}%)")

print(f"\n--- Nulos por columna ---")
nulls = dm.isnull().sum()
nulls = nulls[nulls > 0]
if len(nulls):
    print(nulls.to_string())
else:
    print("Sin nulos")

print(f"\n--- Muestra (3 filas) ---")
pd.set_option("display.max_columns", None)
pd.set_option("display.width", 200)
print(dm.head(3).to_string())
