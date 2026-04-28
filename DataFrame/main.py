from consultas import *
from dataset import *

while True:
    print("\n===== MENÚ PRINCIPAL =====")
    print("1. Ver DataFrames")
    print("2. Generar Datasets")
    print("0. Salir")

    opcion = input("Elige una opción: ")

    # =========================
    # DATAFRAMES
    # =========================
    if opcion == "1":
        while True:
            print("\n--- DATAFRAMES ---")
            for i in range(1, 10):
                print(f"{i}. Consulta {i}")
            print("0. Volver")

            op_df = input("Elige una consulta: ")

            if op_df == "0":
                break

            try:
                funcion = globals()[f"consulta_{op_df}"]
                df = funcion()
                print(df.to_string(index=False))
            except:
                print("❌ Consulta no disponible")

    # =========================
    # DATASETS
    # =========================
    elif opcion == "2":
        while True:
            print("\n--- DATASETS ---")
            for i in range(1, 10):
                print(f"{i}. Dataset Consulta {i}")
            print("10. Generar todos")  
            print("0. Volver")

            op_ds = input("Elige una opción: ")

            if op_ds == "0":
                break

            try:
                
                if op_ds == "10":
                    for i in range(1, 10):
                        try:
                            funcion = globals()[f"dataset_consulta_{i}"]
                            funcion()
                        except:
                            pass
                    print("✅ Todos los datasets generados")

                
                else:
                    funcion = globals()[f"dataset_consulta_{op_ds}"]
                    df = funcion()
                    print(df.to_string(index=False))
                    print(f"\n✅ Dataset consulta {op_ds} generado")

            except:
                print("❌ Dataset no disponible")

    elif opcion == "0":
        print("Saliendo...")
        break

    else:
        print("Opción inválida")