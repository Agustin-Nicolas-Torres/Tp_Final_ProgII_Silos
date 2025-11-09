from __future__ import annotations
from dataclasses import dataclass
from datetime import date
import daoPOS
import daoSUP
import json
import os
import time
from typing import Dict, Any

# Inicializar conexión con Supabase
SUPABASE_URL = "https://ppsfalgzrzyjvoljiibh.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBwc2ZhbGd6cnp5anZvbGppaWJoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjEyMTMxNjEsImV4cCI6MjA3Njc4OTE2MX0.gXYTPicl6xgO7vFUKFyIZ6lalm84Ut5Q00yS-8UWONQ"

# Crear instancia global del DAO de Supabase
supabase_dao = daoSUP.DaoSUP(SUPABASE_URL, SUPABASE_KEY)


@dataclass
class Acopio:  
    silo: int = 0
    grano: str = ""  # El tipo de grano
    tiempo: date = date.today()
    cantidad: int = 0  # En toneladas
    humedad: float = 0.0  # Porcentaje de humedad
    next: 'Acopio' = None

def mostrar_datos(head):
    current = head
    while current:
        print(f"Silo Nro: {current.silo} # Contenido: {current.grano} # Proxima fumigacion: {current.tiempo} # Cantidad: {current.cantidad}", end="\n"* 2)
        current = current.next
    
def datos_silos(head, Silo, grano, tiempo, cantidad, humedad=0.0):
    silo_datos = Acopio(Silo, grano, tiempo, cantidad, humedad, head)
    silo_datos.next = head
    return silo_datos

def obtener_humedad_supabase() -> Dict[int, float]:
    """
    Obtiene los datos de humedad más recientes de Supabase
    Retorna un diccionario con {id_silo: humedad}
    """
    try:
        # Crear instancia del DAO de Supabase
        dao = daoSUP.DaoSUP(
            "https://ppsfalgzrzyjvoljiibh.supabase.co",
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBwc2ZhbGd6cnp5anZvbGppaWJoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjEyMTMxNjEsImV4cCI6MjA3Njc4OTE2MX0.gXYTPicl6xgO7vFUKFyIZ6lalm84Ut5Q00yS-8UWONQ"
        )
        
        # Obtener últimas lecturas
        latest_readings = dao.get_latest_records("readings", "id")
        if not latest_readings:
            return {}
            
        # Convertir a diccionario {id_silo: humedad}
        humedad_dict = {}
        for reading in latest_readings:
            silo_id = reading.get("silo_id")
            humidity = reading.get("humidity")
            if silo_id is not None and humidity is not None:
                humedad_dict[silo_id] = float(humidity)
        
        return humedad_dict
    except Exception as e:
        print(f"Error al obtener datos de Supabase: {e}")
        return {}

def obtener_datos_supabase():
    """Obtiene los últimos datos de humedad desde Supabase (solo silo 1)"""
    try:
        # Obtener últimos datos directamente de Supabase
        latest_record = supabase_dao.get_latest_records("readings", "id")
        
        if latest_record and isinstance(latest_record, list) and len(latest_record) > 0:
            # Guardar en archivo JSON para respaldo
            supabase_dao.save_to_json(latest_record, "latest_reading.json")
            
            # Convertir el dato de moisture a humedad para el silo 1
            return {
                1: {  # Silo número 1
                    "humidity": float(latest_record[0].get("moisture", 0)),
                    "timestamp": latest_record[0].get("created_at", "")
                }
            }
            
    except Exception as e:
        print(f"Error al obtener datos del sensor: {e}")
        # Intentar leer del archivo de respaldo
        try:
            with open("latest_reading.json", 'r', encoding='utf-8') as f:
                stored_data = json.load(f)
                if stored_data and isinstance(stored_data, list) and len(stored_data) > 0:
                    print("Usando datos de respaldo del último registro")
                    return {
                        1: {
                            "humidity": float(stored_data[0].get("moisture", 0)),
                            "timestamp": stored_data[0].get("created_at", "")
                        }
                    }
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error al leer archivo de respaldo: {e}")
    
    return {}

def humedad_grano(data):
    """Calcula y verifica la humedad de los granos en los silos"""
    # Obtener datos del sensor del silo 1
    sensor_data = obtener_datos_supabase()
    
    for silo_data in data:
        silo_id = silo_data["Silo"]
        grano = silo_data.get("grano", "")
        
        # Determinar la humedad según la fuente de datos
        if silo_id == 1 and 1 in sensor_data:  # Silo 1 usa el sensor
            humedad = sensor_data[1]["humidity"]
            timestamp = sensor_data[1]["timestamp"]
            fuente = "sensor"
        else:  # Otros silos usan datos de PostgreSQL
            humedad = silo_data.get("Humedad", 0)
            tiempo_acopio = silo_data.get("Tiempo de Acopio", 0)
            ton = silo_data.get("Cantidad de grano", 0)
            
            if ton > 0 and tiempo_acopio > 0:
                # Calcular humedad basada en el tiempo de acopio
                liq = tiempo_acopio * (ton/1000)
                humedad_incremento = ((ton + liq - ton) * 100) / (ton + liq)
                humedad = ((humedad * humedad_incremento) / 100) + humedad
            
            timestamp = "N/A (usando datos calculados)"
            fuente = "calculado"
    
            # Verificar límites según tipo de grano
        limite = 14.50 if grano == "Maiz" else 14.00 if grano == "Trigo" else 0
        requiere_secado = humedad >= limite
        
        # Preparar mensaje de estado
        if grano == "Maiz" or grano == "Trigo":
            estado = "ALERTA" if requiere_secado else "OK"
            mensaje = f"{grano} - Humedad: {humedad:.2f}% {'>' if requiere_secado else '<'} {limite}%"
            if requiere_secado:
                mensaje += " - Requiere secado"
            else:
                mensaje += " - Humedad óptima"
            
            # Mostrar en consola
            print(f"{estado} - Silo Nro {silo_id} ({grano}):")
            print(f"  {mensaje}")
            print(f"  Fuente de datos: {fuente}")
            print(f"  Última lectura: {timestamp}")
            
            # Actualizar datos en la base de datos
            daoPOS.update_silo_processed_data(silo_id, humedad, requiere_secado, fuente, mensaje)
def datos_json():
    """
    Obtiene datos de los silos desde la base de datos y los procesa
    """
    try:
        # Obtener datos desde la base de datos PostgreSQL
        data = daoPOS.get_all_silos_json()
        if not data:
            print("No se pudieron obtener datos de los silos")
            return
        
        head = None
        for silo_data in data:
            fecha = silo_data["Fecha de fumigacion"]
            if fecha is None:
                continue
                
            # Calcular próxima fecha de fumigación
            resultado_mes = fecha[1] + silo_data["Tiempo de Acopio"]
            resultado_year = fecha[2]
            if resultado_mes > 12:
                resultado_mes = resultado_mes % 12
                resultado_year += 1
                
            if silo_data["grano"] == "Vacio":
                head = datos_silos(head, silo_data["Silo"], silo_data["grano"], None, None)
            else:
                head = datos_silos(
                    head, 
                    silo_data["Silo"], 
                    silo_data["grano"], 
                    date(resultado_year, resultado_mes, fecha[0]), 
                    silo_data["Cantidad de grano"],
                    silo_data.get("Humedad", 0.0)
                )
        
        mostrar_datos(head)
        humedad_grano(data)
    except Exception as e:
        print(f"Error al procesar datos: {e}")

def monitorear_silos():
    """Función principal que monitorea los silos cada 15 segundos"""
    try:
        while True:
            print("\n=== Actualizando datos de silos ===")
            print(f"Hora: {date.today()}")
            print("=" * 35)
            
            datos_json()
            
            print("\nEsperando 5 segundos para la próxima actualización...")
            time.sleep(5)
            
    except KeyboardInterrupt:
        print("\nMonitoreo detenido por el usuario")
    except Exception as e:
        print(f"\nError en el monitoreo: {e}")

if __name__ == "__main__":
    print("Iniciando monitoreo de silos...")
    print("Presiona Ctrl+C para detener")
    print("=" * 35)
    monitorear_silos()









        