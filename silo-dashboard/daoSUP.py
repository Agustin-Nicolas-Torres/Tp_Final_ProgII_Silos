from supabase import create_client
import os
import json
from datetime import datetime

class DaoSUP:
    def __init__(self, url, key):
        self.supabase = create_client(url, key)
    
    def get_latest_records(self, table_name, id_column, limit=1):
        """
        Obtiene los últimos registros de una tabla, ordenados por fecha de creación
        
        Args:
            table_name (str): Nombre de la tabla
            id_column (str): Nombre de la columna ID
            limit (int): Cantidad de registros a obtener (por defecto 1)
            
        Returns:
            list: Lista de registros encontrados
        """
        try:
            response = (self.supabase
                       .table(table_name)
                       .select("*")
                       .order('created_at', desc=True)
                       .limit(limit)
                       .execute())
            
            if len(response.data) > 0:
                return response.data
            return None
            
        except Exception as e:
            print(f"Error al obtener registros: {str(e)}")
            return None
            
    def save_to_json(self, data, filename):
        """
        Guarda los datos en un archivo JSON
        
        Args:
            data: Datos a guardar
            filename (str): Nombre del archivo donde guardar los datos
            
        Returns:
            bool: True si se guardó correctamente, False en caso contrario
        """
        try:
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
            return True
        except Exception as e:
            print(f"Error al guardar en JSON: {str(e)}")
            return False

# Ejemplo de uso:
if __name__ == "__main__":
    # Reemplaza estos valores con tus credenciales de Supabase
    SUPABASE_URL = "https://ppsfalgzrzyjvoljiibh.supabase.co"
    SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InBwc2ZhbGd6cnp5anZvbGppaWJoIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjEyMTMxNjEsImV4cCI6MjA3Njc4OTE2MX0.gXYTPicl6xgO7vFUKFyIZ6lalm84Ut5Q00yS-8UWONQ"
    
    try:
        # Crear instancia del DAO
        dao = DaoSUP(SUPABASE_URL, SUPABASE_KEY)
        
        # Ejemplo: obtener el último registro de una tabla
        latest_record = dao.get_latest_records("readings", "id")
        
        if latest_record:
            print("Último registro:", latest_record)
            # Guardar el registro en un archivo JSON
            if dao.save_to_json(latest_record, "latest_reading.json"):
                print("Datos guardados exitosamente en latest_reading.json")
            else:
                print("Error al guardar los datos en JSON")
        else:
            print("No se encontraron registros")
            
    except Exception as e:
        print(f"Error: {str(e)}")
