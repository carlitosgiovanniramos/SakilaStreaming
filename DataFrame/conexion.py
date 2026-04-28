from pymongo import MongoClient

# =========================
# 1. CONEXIÓN A MONGODB
# =========================

MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "SakilaStreaming"

client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# =========================
# 2. COLECCIONES
# =========================
categoria = db["categoria"]
cliente = db["cliente"]
contenido = db["contenido"]
contenidocategoria = db["contenidocategoria"]
contenidotalento = db["contenidotalento"]
eventostreaming = db["eventostreaming"]
fecha = db["fecha"]
geografia = db["geografia"]
lenguaje = db["lenguaje"]
metodopago = db["metodopago"]
pagos = db["pagos"]
plansuscripcion = db["plansuscripcion"]
proveedor = db["proveedor"]
suscripcion = db["suscripcion"]
talento = db["talento"]
catalogodisponible = db["catalogodisponible"]

