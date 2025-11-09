import psycopg2
from typing import List, Tuple, Dict, Any
import datetime


def get_db_connection():
    return psycopg2.connect(
        host='localhost',
        user='postgres',
        password='1234',
        database='TP_Final'
    )

try:
    Connection = get_db_connection()
    print("Conexion exitosa")
    cursor = Connection.cursor()
except Exception as ex:
    print("Error de conexión:", ex)
    Connection = None
    cursor = None

def get_silo_by_id(silo_id: int) -> List[Tuple]:
    try:
        cursor.execute("SELECT * FROM silos_datos WHERE silo_id = %s", (silo_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error en get_silo_by_id: {e}")
        return []

def get_grano_by_id(silo_id: int) -> List[Tuple]:
    try:
        cursor.execute("SELECT grano FROM silos_datos WHERE silo_id = %s", (silo_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error en get_grano_by_id: {e}")
        return []

def get_Fumigacion_by_id(silo_id: int) -> List[Tuple]:
    try:
        cursor.execute("SELECT fecha_fumigacion FROM silos_datos WHERE silo_id = %s", (silo_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error en get_Fumigacion_by_id: {e}")
        return []

def get_Acopio_by_id(silo_id: int) -> List[Tuple]:
    try:
        cursor.execute("SELECT tiempo_acopio FROM silos_datos WHERE silo_id = %s", (silo_id,))
        return cursor.fetchall()
    except Exception as e:
        print(f"Error en get_Acopio_by_id: {e}")
        return []

def update_silo_processed_data(silo_id: int, humedad: float, requiere_secado: bool, 
                             fuente_datos: str, estado_alerta: str) -> bool:
    """
    Actualiza los datos procesados de un silo en la base de datos
    """
    if not Connection or not cursor:
        print("No hay conexión a la base de datos")
        return False
        
    try:
        sql = """
            UPDATE silos_datos 
            SET humedad_actual = %s,
                requiere_secado = %s,
                fuente_datos = %s,
                ultima_actualizacion = CURRENT_TIMESTAMP,
                estado_alerta = %s
            WHERE silo_id = %s
        """
        cursor.execute(sql, (humedad, requiere_secado, fuente_datos, estado_alerta, silo_id))
        Connection.commit()
        print(f"Datos actualizados correctamente para silo {silo_id}")
        return True
    except Exception as e:
        print(f"Error al actualizar datos del silo {silo_id}: {e}")
        if Connection:
            Connection.rollback()
        return False

def get_all_silos_json() -> List[Dict[str, Any]]:
    """
    Obtiene todos los datos de silos y los devuelve en formato compatible con el JSON original.
    Devuelve una lista de diccionarios con las claves:
    - Silo: número de silo
    - grano: tipo de grano
    - Fecha de fumigacion: [día, mes, año]
    - Tiempo de Acopio: int
    - Cantidad de grano: int
    (La humedad se obtiene de otra base de datos)
    """
    try:
        cursor.execute("""
            SELECT silo_id, grano, fecha_fumigacion, tiempo_acopio, 
                   cantidad_grano
            FROM silos_datos
            ORDER BY silo_id
        """)
        rows = cursor.fetchall()
        result = []
        
        for row in rows:
            silo_data = {
                'Silo': row[0],
                'grano': row[1] if row[1] else "Vacio",
                'Fecha de fumigacion': None,
                'Tiempo de Acopio': row[3] if row[3] is not None else 0,
                'Cantidad de grano': row[4] if row[4] is not None else 0
            }
            
            # Procesar fecha de fumigación
            if row[2] and isinstance(row[2], datetime.date):  # si hay fecha y es un objeto date
                fecha = row[2]
                silo_data['Fecha de fumigacion'] = [fecha.day, fecha.month, fecha.year]
            elif row[2] and isinstance(row[2], (list, tuple)):  # si es una lista/tupla
                if len(row[2]) >= 1 and isinstance(row[2][0], datetime.date):
                    fecha = row[2][0]  # tomar el primer elemento que debería ser la fecha
                    silo_data['Fecha de fumigacion'] = [fecha.day, fecha.month, fecha.year]
                else:
                    today = datetime.date.today()
                    silo_data['Fecha de fumigacion'] = [today.day, today.month, today.year]
            else:  # si no hay fecha o no es reconocible
                today = datetime.date.today()
                silo_data['Fecha de fumigacion'] = [today.day, today.month, today.year]
            
            result.append(silo_data)
        
        return result
    except Exception as ex:
        print("Error al obtener datos:", ex)
        return []

# Test básico
if __name__ == '__main__':
    try:
        # Primero ver la estructura de los datos crudos
        id = 3
        print("=== Datos crudos ===")
        silo_data = get_silo_by_id(id)
        print(f"Estructura de get_silo_by_id({id}):", type(silo_data), silo_data)
        
        fumig_data = get_Fumigacion_by_id(id)
        print(f"Estructura de get_Fumigacion_by_id({id}):", type(fumig_data), fumig_data)
        
        print("\n=== Datos en formato JSON ===")
        silos_json = get_all_silos_json()
        for silo in silos_json:
            print("Silo:", silo)
            print("  Tipo de Fecha de fumigacion:", type(silo['Fecha de fumigacion']))
            print("  Valor:", silo['Fecha de fumigacion'])
            print()
    except Exception as ex:
        print("Error en test:", ex)
        import traceback
        traceback.print_exc()