import pandas as pd
import numpy as np
import os

BASE = r"c:\Users\Lenovo LOQ\Desktop\UTA\6 Software\Inteligencia de Negocios\Unidad 4\BD normalizada"
OUT  = os.path.join(BASE, "Datamart_Pagos.csv")

print("Leyendo tablas normalizadas...")
pm      = pd.read_csv(os.path.join(BASE, "DimPaymentMethod.csv"))
cust    = pd.read_csv(os.path.join(BASE, "DimCustomer.csv"))
metrics = pd.read_csv(os.path.join(BASE, "FactCustomerMetrics.csv"))
addr    = pd.read_csv(os.path.join(BASE, "DimAddress.csv"))
city    = pd.read_csv(os.path.join(BASE, "DimCity.csv"))
state   = pd.read_csv(os.path.join(BASE, "DimState.csv"))
country = pd.read_csv(os.path.join(BASE, "DimCountry.csv"))

# ── 1. Jerarquía geográfica ────────────────────────────────────────────────────
geo = (
    addr[["AddressKey", "CityKey"]]
    .merge(city[["CityKey", "Name", "StateKey"]].rename(columns={"Name": "CityName"}),
           on="CityKey")
    .merge(state[["StateKey", "Name", "CountryKey"]].rename(columns={"Name": "StateName"}),
           on="StateKey")
    .merge(country[["CountryKey", "Name", "ISOCode", "Continent"]]
           .rename(columns={"Name": "CountryName"}),
           on="CountryKey")
)

# ── 2. Agregar DimPaymentMethod por cliente ────────────────────────────────────
print("Agregando metodos de pago por cliente...")
pm["CardExpiryDate"] = pd.to_datetime(pm["CardExpiryDate"], errors="coerce")
today = pd.Timestamp("today")

# Flags por red de tarjeta
for network in ["Visa", "Mastercard", "American Express", "PayPal",
                "Discover", "Debit Card", "JCB"]:
    col = "Has_" + network.replace(" ", "_")
    pm[col] = (pm["CardNetwork"] == network).astype(int)

pm_agg = (
    pm.groupby("CustomerKey")
    .agg(
        # Conteos
        TotalCards          = ("PaymentMethodKey", "count"),
        ActiveCards         = ("IsActive",         "sum"),
        InactiveCards       = ("IsActive",         lambda x: (x == 0).sum()),

        # Tipo de tarjeta
        CreditCards         = ("CardSubtype", lambda x: (x == "credit").sum()),
        DebitCards          = ("CardSubtype", lambda x: (x.isin(["debit","Debit Card"])).sum()),

        # Red principal (la más frecuente del cliente)
        PrimaryNetwork      = ("CardNetwork", lambda x: x.value_counts().index[0]),

        # Diversificación
        UniqueNetworks      = ("CardNetwork", "nunique"),

        # Flags por red
        Has_Visa            = ("Has_Visa",            "max"),
        Has_Mastercard      = ("Has_Mastercard",       "max"),
        Has_AmericanExpress = ("Has_American_Express", "max"),
        Has_PayPal          = ("Has_PayPal",           "max"),
        Has_Discover        = ("Has_Discover",         "max"),
        Has_DebitCard       = ("Has_Debit_Card",       "max"),
        Has_JCB             = ("Has_JCB",              "max"),

        # País emisor
        IssuingCountry      = ("IssuingCountry", lambda x: x.value_counts().index[0]),

        # Vencimiento más próximo
        NextExpiry          = ("CardExpiryDate", "min"),
        FurthestExpiry      = ("CardExpiryDate", "max"),
    )
    .reset_index()
)

# Días hasta próximo vencimiento
pm_agg["DaysToNextExpiry"] = (pm_agg["NextExpiry"] - today).dt.days
pm_agg["HasExpiredCard"]   = (pm_agg["DaysToNextExpiry"] < 0).astype(int)
pm_agg["HasCardExpiringSoon"] = (
    (pm_agg["DaysToNextExpiry"] >= 0) & (pm_agg["DaysToNextExpiry"] <= 90)
).astype(int)

# Perfil de diversificación
pm_agg["PaymentDiversification"] = pd.cut(
    pm_agg["UniqueNetworks"],
    bins=[0, 1, 2, 10],
    labels=["Single", "Dual", "Multi"]
)

# ── 3. JOIN principal ──────────────────────────────────────────────────────────
print("Construyendo datamart...")
dm = (
    pm_agg
    .merge(
        cust[["CustomerKey", "Gender", "IsActive", "CreateDate",
              "DeviceType", "RegistrationSource", "AddressKey"]],
        on="CustomerKey", how="left"
    )
    .merge(
        metrics[["CustomerKey", "CustomerSegment", "LifetimeValue",
                 "PaymentReliability", "ChurnRiskScore", "MonthsAsCustomer"]],
        on="CustomerKey", how="left"
    )
    .merge(geo[["AddressKey", "CityName", "StateName",
                "CountryName", "ISOCode", "Continent"]],
           on="AddressKey", how="left"
    )
)

# ── 4. Columna: match entre país del cliente y país emisor ────────────────────
dm["CountryMatchIssuer"] = (dm["ISOCode"] == dm["IssuingCountry"]).astype(int)

# ── 5. Segmento de confianza de pago ──────────────────────────────────────────
conditions = [
    (dm["PaymentReliability"] >= 0.9) & (dm["ActiveCards"] >= 1),
    (dm["PaymentReliability"] >= 0.7) & (dm["ActiveCards"] >= 1),
    (dm["PaymentReliability"] >= 0.5),
]
dm["PaymentTrustSegment"] = np.select(
    conditions,
    ["High Trust", "Medium Trust", "Low Trust"],
    default="At Risk"
)

# ── 6. Orden final ─────────────────────────────────────────────────────────────
COLS = [
    # Identificador
    "CustomerKey",
    # Medidas de tarjetas
    "TotalCards", "ActiveCards", "InactiveCards",
    "CreditCards", "DebitCards",
    "UniqueNetworks", "PaymentDiversification",
    "PrimaryNetwork",
    # Flags por red
    "Has_Visa", "Has_Mastercard", "Has_AmericanExpress",
    "Has_PayPal", "Has_Discover", "Has_DebitCard", "Has_JCB",
    # Vencimientos
    "DaysToNextExpiry", "HasExpiredCard", "HasCardExpiringSoon",
    # País emisor
    "IssuingCountry", "CountryMatchIssuer",
    # Métricas de cliente
    "LifetimeValue", "PaymentReliability", "ChurnRiskScore",
    "MonthsAsCustomer", "CustomerSegment", "PaymentTrustSegment",
    # Demografía
    "Gender", "IsActive", "DeviceType", "RegistrationSource",
    # Geografía del cliente
    "CityName", "StateName", "CountryName", "ISOCode", "Continent",
    # Fecha registro
    "CreateDate",
]
dm = dm[COLS]

# ── 7. Guardar ─────────────────────────────────────────────────────────────────
dm.to_csv(OUT, index=False, encoding="utf-8-sig")

# ── 8. Reporte ─────────────────────────────────────────────────────────────────
print(f"\n{'='*60}")
print(f"Datamart_Pagos.csv generado")
print(f"{'='*60}")
print(f"Filas    : {len(dm):,}")
print(f"Columnas : {len(dm.columns)}")
print(f"Ubicacion: {OUT}")

print(f"\n--- Tarjetas por cliente ---")
print(dm["TotalCards"].value_counts().sort_index().to_string())

print(f"\n--- Red principal mas comun ---")
print(dm["PrimaryNetwork"].value_counts().to_string())

print(f"\n--- Diversificacion de pago ---")
print(dm["PaymentDiversification"].value_counts().to_string())

print(f"\n--- Segmento de confianza ---")
print(dm["PaymentTrustSegment"].value_counts().to_string())

print(f"\n--- Tarjetas por vencer en 90 dias ---")
print(f"  Con tarjeta vencida     : {dm['HasExpiredCard'].sum():,}")
print(f"  Vence proximos 90 dias  : {dm['HasCardExpiringSoon'].sum():,}")

print(f"\n--- Pais emisor vs pais cliente (match) ---")
match = dm["CountryMatchIssuer"].value_counts()
print(f"  Misma region : {match.get(1,0):,} ({match.get(1,0)/len(dm)*100:.1f}%)")
print(f"  Internacional: {match.get(0,0):,} ({match.get(0,0)/len(dm)*100:.1f}%)")

print(f"\n--- Nulos por columna ---")
nulls = dm.isnull().sum()
nulls = nulls[nulls > 0]
print(nulls.to_string() if len(nulls) else "Sin nulos")
