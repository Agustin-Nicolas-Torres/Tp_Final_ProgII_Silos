using System.Data;
using Npgsql;
// Se debe agregar System, ya que Console está en System.
using System; 

namespace DataAccess
{
    public class Program
    {
        public static void Main(string[] args)
        {
            
            Console.WriteLine("Creando la conexión...");
            var builder = new NpgsqlConnectionStringBuilder();
            builder.Host = "localhost";
            builder.Port = 5432;
            builder.Username = "postgres";
            builder.Password = "1234";
            builder.Database = "TP_Final";

            Console.WriteLine(builder.ConnectionString);
            // La conexión debe estar definida DENTRO del método Main
            using NpgsqlConnection connection = new NpgsqlConnection(builder.ConnectionString);

            try
            {
                connection.Open();
                Console.WriteLine("Conexión Válida");
                NpgsqlCommand command = new NpgsqlCommand("SELECT VERSION();", connection);
                using (NpgsqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.HasRows)
                    {
                        while (reader.Read())
                        {
                            Console.WriteLine(reader.GetString(0));
                        }
                    }

                }
                NpgsqlCommand commando = new NpgsqlCommand("SELECT * FROM silos_datos;", connection);
                using (NpgsqlDataReader reader2 = commando.ExecuteReader())
                {
                    if (reader2.HasRows)
                    {
                        while (reader2.Read())
                        {
                            for (int i = 0; i < reader2.FieldCount; i++)
                            {
                                var val = reader2.IsDBNull(i) ? "NULL" : reader2.GetValue(i);
                                Console.Write(val);
                                if (i < reader2.FieldCount - 1) Console.Write("\t");
                            }
                            Console.WriteLine();
                        }
                    }
                    else
                    {
                        Console.WriteLine("No se encontraron filas en silos_datos.");
                    }
                }

            }
            catch (Exception exception)
            {
                Console.WriteLine("⚠️ ERROR: " + exception.Message);
            }
           
        }
    }
}