import pandas as pd
import numpy as np
import os

BASE = r"c:\Users\Lenovo LOQ\Desktop\UTA\6 Software\Inteligencia de Negocios\Unidad 4\BD normalizada"
OUT  = os.path.join(BASE, "Datamart_Geografia.csv")

print("Leyendo tablas normalizadas...")
metrics = pd.read_csv(os.path.join(BASE, "FactCustomerMetrics.csv"))
cust    = pd.read_csv(os.path.join(BASE, "DimCustomer.csv"))
pm      = pd.read_csv(os.path.join(BASE, "DimPaymentMethod.csv"))
addr    = pd.read_csv(os.path.join(BASE, "DimAddress.csv"))
city    = pd.read_csv(os.path.join(BASE, "DimCity.csv"))
state   = pd.read_csv(os.path.join(BASE, "DimState.csv"))
country = pd.read_csv(os.path.join(BASE, "DimCountry.csv"))

# ── 1. Construir base plana cliente + métricas + geografía ────────────────────
print("Ensamblando base cliente-geografía...")

# Tarjetas por cliente
pm_count = (
    pm.groupby("CustomerKey")
    .agg(TotalCards=("PaymentMethodKey","count"),
         ActiveCards=("IsActive","sum"))
    .reset_index()
)

# Geo completa
geo = (
    addr[["AddressKey", "CityKey", "Latitude", "Longitude", "Timezone"]]
    .merge(city[["CityKey","Name","StateKey"]].rename(columns={"Name":"CityName"}),
           on="CityKey")
    .merge(state[["StateKey","Name","CountryKey"]].rename(columns={"Name":"StateName"}),
           on="StateKey")
    .merge(country[["CountryKey","Name","ISOCode","Continent"]]
           .rename(columns={"Name":"CountryName"}),
           on="CountryKey")
)

# Base plana 1 fila por cliente
base = (
    cust[["CustomerKey","Gender","Birth","IsActive","CreateDate",
          "DeviceType","RegistrationSource","AddressKey"]]
    .merge(metrics[["CustomerKey","AgeGroup","CustomerSegment","MonthsAsCustomer",
                    "ChurnRiskScore","LifetimeValue","AvgSessionMinutes",
                    "PaymentReliability"]], on="CustomerKey")
    .merge(pm_count, on="CustomerKey", how="left")
    .merge(geo[["AddressKey","CityName","StateName","CountryName",
                "ISOCode","Continent","Latitude","Longitude","Timezone"]],
           on="AddressKey")
)
base["IsChurned"] = (base["IsActive"] == 0).astype(int)

print(f"  Base plana: {len(base):,} clientes")

# ── 2. Función de agregación reutilizable ──────────────────────────────────────
def agg_geo(df, group_cols, level_name):
    agg = (
        df.groupby(group_cols)
        .agg(
            CustomerCount        = ("CustomerKey",        "count"),
            ActiveCustomers      = ("IsActive",           "sum"),
            ChurnedCustomers     = ("IsChurned",          "sum"),

            AvgLifetimeValue     = ("LifetimeValue",      "mean"),
            TotalLifetimeValue   = ("LifetimeValue",      "sum"),
            MedianLifetimeValue  = ("LifetimeValue",      "median"),
            MaxLifetimeValue     = ("LifetimeValue",      "max"),

            AvgChurnRiskScore    = ("ChurnRiskScore",     "mean"),
            HighRiskCustomers    = ("ChurnRiskScore",     lambda x: (x >= 0.7).sum()),

            AvgSessionMinutes    = ("AvgSessionMinutes",  "mean"),
            AvgPaymentReliability= ("PaymentReliability", "mean"),
            AvgMonthsAsCustomer  = ("MonthsAsCustomer",  "mean"),

            # Distribución de género
            MaleCount            = ("Gender",             lambda x: (x == "male").sum()),
            FemaleCount          = ("Gender",             lambda x: (x == "female").sum()),

            # Segmentos
            GoldCount            = ("CustomerSegment",    lambda x: (x == "Gold").sum()),
            SilverCount          = ("CustomerSegment",    lambda x: (x == "Silver").sum()),
            BronzeCount          = ("CustomerSegment",    lambda x: (x == "Bronze").sum()),

            # Dispositivo principal
            PrimaryDevice        = ("DeviceType",         lambda x: x.value_counts().index[0]),

            # Canal de registro principal
            PrimaryRegSource     = ("RegistrationSource", lambda x: x.value_counts().index[0]),

            # Tarjetas
            AvgCardsPerCustomer  = ("TotalCards",         "mean"),
        )
        .reset_index()
    )

    # KPIs derivados
    agg["ChurnRate"]          = (agg["ChurnedCustomers"] / agg["CustomerCount"]).round(4)
    agg["ActiveRate"]         = (agg["ActiveCustomers"]  / agg["CustomerCount"]).round(4)
    agg["HighRiskRate"]       = (agg["HighRiskCustomers"]/ agg["CustomerCount"]).round(4)
    agg["MalePct"]            = (agg["MaleCount"]        / agg["CustomerCount"]).round(4)
    agg["GoldPct"]            = (agg["GoldCount"]        / agg["CustomerCount"]).round(4)
    agg["GeoLevel"]           = level_name

    # Redondear medidas
    for col in ["AvgLifetimeValue","TotalLifetimeValue","MedianLifetimeValue","MaxLifetimeValue",
                "AvgChurnRiskScore","AvgSessionMinutes","AvgPaymentReliability",
                "AvgMonthsAsCustomer","AvgCardsPerCustomer"]:
        agg[col] = agg[col].round(2)

    return agg

# ── 3. Agregaciones por nivel geográfico ──────────────────────────────────────
print("Agregando por Ciudad...")
city_agg = agg_geo(
    base,
    ["CityName","StateName","CountryName","ISOCode","Continent"],
    "City"
)
# Coordenadas promedio de la ciudad (para mapas)
city_coords = (
    base.groupby(["CityName","StateName"])
    [["Latitude","Longitude"]].mean().round(4).reset_index()
)
city_agg = city_agg.merge(city_coords, on=["CityName","StateName"], how="left")
city_agg.insert(0, "GeoKey", city_agg["CityName"] + " | " + city_agg["StateName"])

print("Agregando por Estado...")
state_agg = agg_geo(
    base,
    ["StateName","CountryName","ISOCode","Continent"],
    "State"
)
state_coords = (
    base.groupby("StateName")[["Latitude","Longitude"]].mean().round(4).reset_index()
)
state_agg = state_agg.merge(state_coords, on="StateName", how="left")
state_agg.insert(0, "GeoKey", state_agg["StateName"])
state_agg["CityName"] = ""

print("Agregando por País...")
country_agg = agg_geo(
    base,
    ["CountryName","ISOCode","Continent"],
    "Country"
)
country_coords = (
    base.groupby("CountryName")[["Latitude","Longitude"]].mean().round(4).reset_index()
)
country_agg = country_agg.merge(country_coords, on="CountryName", how="left")
country_agg.insert(0, "GeoKey", country_agg["CountryName"])
country_agg["CityName"]  = ""
country_agg["StateName"] = ""

print("Agregando por Continente...")
cont_agg = agg_geo(base, ["Continent"], "Continent")
cont_coords = (
    base.groupby("Continent")[["Latitude","Longitude"]].mean().round(4).reset_index()
)
cont_agg = cont_agg.merge(cont_coords, on="Continent", how="left")
cont_agg.insert(0, "GeoKey", cont_agg["Continent"])
cont_agg["CityName"]    = ""
cont_agg["StateName"]   = ""
cont_agg["CountryName"] = ""
cont_agg["ISOCode"]     = ""

# ── 4. Unir todos los niveles en un solo datamart multinicel ──────────────────
print("Uniendo niveles geográficos...")
dm = pd.concat([city_agg, state_agg, country_agg, cont_agg],
               ignore_index=True, sort=False)

# ── 5. Orden final de columnas ─────────────────────────────────────────────────
COLS = [
    # Identificador y nivel
    "GeoLevel", "GeoKey",
    # Jerarquía
    "Continent", "CountryName", "ISOCode", "StateName", "CityName",
    "Latitude", "Longitude",
    # Volumen
    "CustomerCount", "ActiveCustomers", "ChurnedCustomers",
    "ChurnRate", "ActiveRate",
    # Riesgo
    "AvgChurnRiskScore", "HighRiskCustomers", "HighRiskRate",
    # Valor económico
    "TotalLifetimeValue", "AvgLifetimeValue",
    "MedianLifetimeValue", "MaxLifetimeValue",
    # Engagement
    "AvgSessionMinutes", "AvgPaymentReliability", "AvgMonthsAsCustomer",
    # Segmentos
    "GoldCount", "SilverCount", "BronzeCount", "GoldPct",
    # Género
    "MaleCount", "FemaleCount", "MalePct",
    # Comportamiento
    "PrimaryDevice", "PrimaryRegSource", "AvgCardsPerCustomer",
]
dm = dm[COLS]
dm = dm.sort_values(["GeoLevel","TotalLifetimeValue"], ascending=[True, False])

# ── 6. Guardar ─────────────────────────────────────────────────────────────────
dm.to_csv(OUT, index=False, encoding="utf-8-sig")

# ── 7. Reporte ─────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Datamart_Geografia.csv generado")
print(f"{'='*60}")
print(f"Filas    : {len(dm):,}  (ciudad + estado + país + continente)")
print(f"Columnas : {len(dm.columns)}")
print(f"Ubicacion: {OUT}")

print(f"\n--- Filas por nivel ---")
print(dm["GeoLevel"].value_counts().to_string())

print(f"\n--- Top 5 Paises por TotalLifetimeValue ---")
paises = dm[dm["GeoLevel"]=="Country"].nlargest(5,"TotalLifetimeValue")
print(paises[["CountryName","CustomerCount","TotalLifetimeValue",
              "AvgChurnRiskScore","ChurnRate"]].to_string(index=False))

print(f"\n--- Top 10 Ciudades por CustomerCount ---")
ciudades = dm[dm["GeoLevel"]=="City"].nlargest(10,"CustomerCount")
print(ciudades[["CityName","StateName","CustomerCount","AvgLifetimeValue",
                "ChurnRate","AvgChurnRiskScore"]].to_string(index=False))

print(f"\n--- Ciudades con mayor riesgo de churn (min 50 clientes) ---")
riesgo = dm[(dm["GeoLevel"]=="City") & (dm["CustomerCount"]>=50)].nlargest(5,"AvgChurnRiskScore")
print(riesgo[["CityName","StateName","CustomerCount","AvgChurnRiskScore","ChurnRate"]].to_string(index=False))

print(f"\n--- Nulos por columna ---")
nulls = dm.isnull().sum()
nulls = nulls[nulls > 0]
print(nulls.to_string() if len(nulls) else "Sin nulos")
