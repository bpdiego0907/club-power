# -------------------------------------------------------------
# init_db.py — crea la tabla y llena los datos en Railway
# -------------------------------------------------------------
import psycopg
from dotenv import load_dotenv
import os

# Cargar .env
load_dotenv()
DB_URL = os.getenv("DB_URL").replace("postgresql+psycopg://", "postgresql://")

print("Conectando a la base...")
try:
    with psycopg.connect(DB_URL) as conn:
        with conn.cursor() as cur:
            # Crear tabla
            cur.execute("""
                CREATE TABLE IF NOT EXISTS club_power_puntos (
                    id BIGSERIAL PRIMARY KEY,
                    dni VARCHAR(20) NOT NULL,
                    tipo VARCHAR(30),
                    canal VARCHAR(40),
                    puntos_1er_premio INTEGER NOT NULL,
                    puntos_2do_premio INTEGER NOT NULL,
                    CONSTRAINT uk_dni UNIQUE (dni)
                );
            """)
            # Insertar registros
            cur.execute("""
                INSERT INTO club_power_puntos (dni, tipo, canal, puntos_1er_premio, puntos_2do_premio) VALUES
                ('666666','LIDER','PDV',300,400),
                ('123455','EQUIPO','MULTIMARCA',200,220),
                ('12324356','EQUIPO','FULL PREPAGO',234,257),
                ('324355','LIDER','PDV PLUS',346,381),
                ('435467','EQUIPO','HC EMO',600,660)
                ON CONFLICT (dni) DO NOTHING;
            """)
            conn.commit()

            # Leer registros para confirmar
            cur.execute("SELECT * FROM club_power_puntos;")
            rows = cur.fetchall()
            print("\n✅ Datos cargados correctamente:\n")
            for r in rows:
                print(r)

except Exception as e:
    print("❌ Error al conectar o ejecutar SQL:")
    print(e)
