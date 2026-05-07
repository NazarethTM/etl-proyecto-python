# Pipeline ETL — Proyecto Final Python

**2º DAW · Sistema de Ingeniería de Datos**  
**Grupo PyKrew:** Ken, Laura, Naza  

---

## ¿Qué hace este proyecto?

Este pipeline lee ficheros CSV con datos de clientes y tarjetas bancarias, los limpia y transforma (ETL), y los carga en una base de datos MySQL. También genera CSVs con los datos ya procesados en la carpeta `output/`.

El proceso completo se puede lanzar con un solo comando y automatizar para que se ejecute a las 3 AM cada día.

---

## Estructura del proyecto

```
etl-proyecto-python/
├── input/                          ← Ficheros fuente CSV
│   ├── Clientes-YYYY-MM-DD.csv
│   └── Tarjetas-YYYY-MM-DD.csv
├── output/                         ← CSVs transformados
│   ├── Clientes-YYYY-MM-DD.cleaned.csv
│   └── Tarjetas-YYYY-MM-DD.cleaned.csv
├── errors/                         ← Filas rechazadas con motivo
├── logs/                           ← Logs rotativos del pipeline
├── tests/
│   └── test_pipeline.py            ← Tests con pytest
├── etl_pipeline.py                 ← Lógica ETL principal
├── run_pipeline.py                 ← Punto de entrada
├── requirements.txt
└── README.md
```

---

## Instalación

```bash
# 1. Clona el repositorio
git clone <url-del-repositorio>
cd etl-proyecto-python

# 2. Instala dependencias
pip install -r requirements.txt
```

---

## Configuración de MySQL

Antes de lanzar el pipeline necesitas tener MySQL corriendo y crear la base de datos:

```sql
CREATE DATABASE etl_db CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'etl_password';
GRANT ALL PRIVILEGES ON etl_db.* TO 'etl_user'@'localhost';
FLUSH PRIVILEGES;
```

Las tablas (`clientes` y `tarjetas`) se crean automáticamente la primera vez que se ejecuta el pipeline si no existen.

---

## Uso

```bash
# Ejecución básica (MySQL en localhost con valores por defecto)
python run_pipeline.py

# Con variables de entorno (recomendado para no hardcodear credenciales)
DB_HOST=localhost \
DB_PORT=3306 \
DB_USER=etl_user \
DB_PASSWORD=etl_password \
DB_NAME=etl_db \
ETL_SALT="mi_sal_secreta" \
python run_pipeline.py
```

> **Nota:** Si MySQL no está disponible, el pipeline igualmente procesa los ficheros y guarda los CSVs en `output/`. Solo avisa en el log que no pudo conectar a la BD.

---

## Automatización con cron

Para ejecutar el pipeline todos los días a las 03:00 AM, añade esta línea a `crontab -e`:

```
0 3 * * * cd /ruta/al/proyecto && DB_PASSWORD="xxx" ETL_SALT="xxx" python run_pipeline.py >> logs/cron.log 2>&1
```

---

## Ficheros aceptados

El pipeline **solo procesa** ficheros que cumplan exactamente este patrón:

| Tipo | Patrón | Separador | Codificación |
|------|--------|-----------|--------------|
| Clientes | `Clientes-YYYY-MM-DD.csv` | `;` | UTF-8 |
| Tarjetas | `Tarjetas-YYYY-MM-DD.csv` | `;` | UTF-8 |

Cualquier otro fichero en `input/` se ignora y se registra un aviso en el log.

---

## Qué hace la ETL

### Clientes

1. Lee el CSV con `pandas`, manejando errores de codificación
2. Renombra columnas (`Cod cliente` → `cod_cliente`, etc.)
3. Elimina espacios sobrantes en todos los campos
4. Normaliza nombres: capitaliza correctamente respetando guiones (`ANA-MARIA` → `Ana-Maria`)
5. Elimina acentos de nombres y apellidos
6. Pasa correos a minúsculas
7. Limpia DNI (quita espacios/guiones, pasa a mayúsculas)
8. Limpia teléfono (solo dígitos)
9. Añade columnas de validación: `DNI_OK/KO`, `Telefono_OK/KO`, `Correo_OK/KO`
10. Anonimiza el DNI: versión enmascarada (`123****A`) y hash SHA-256 con sal
11. Rechaza filas sin `cod_cliente` o `correo` → las guarda en `errors/`
12. Guarda el CSV limpio en `output/`

### Tarjetas

1. Lee y limpia igual que clientes
2. Enmascara el número de tarjeta (`XXXX-XXXX-XXXX-9012`)
3. Genera hash SHA-256 del número completo
4. Genera hash SHA-256 del CVV — **nunca se almacena el CVV en claro**
5. Elimina las columnas sensibles originales
6. Rechaza filas sin `cod_cliente`
7. Guarda el CSV limpio en `output/`

---

## Anonimización — decisiones tomadas

| Dato | Técnica | Justificación |
|------|---------|---------------|
| Número de tarjeta | Enmascarado (últimos 4 dígitos) + SHA-256 | El enmascarado permite identificación parcial; el hash permite comparaciones sin exponer el dato |
| CVV | SHA-256 + sal | Dato altamente sensible, irreversible. La sal evita ataques de diccionario |
| DNI | Enmascarado parcial + SHA-256 | Misma lógica que la tarjeta |

La sal se pasa como variable de entorno (`ETL_SALT`) para no quedar en el código.

---

## Renombrado de columnas

| CSV original | Columna en BD | Motivo |
|---|---|---|
| `Cod cliente` | `cod_cliente` | Eliminamos el espacio; snake_case |
| `nombre` | `nombre` | Sin cambio |
| `apellido1` / `apellido2` | igual | Sin cambio |
| `dni` | `dni` (+ `dni_masked`, `dni_hash`) | Separamos original de versiones anonimizadas |
| `correo` | `correo` | Sin cambio |
| `telefono` | `telefono` | Sin tilde (consistencia ASCII) |
| `numero_tarjeta` | `numero_tarjeta_masked` + `numero_tarjeta_hash` | Nunca en claro |
| `cvv` | `cvv_hash` | Nunca en claro |

---

## Tests

```bash
pytest tests/test_pipeline.py -v
```

Los tests cubren: validación de DNI, teléfono, correo, enmascarado de tarjeta y normalización de nombres.

---

## Dependencias principales

| Librería | Para qué |
|---|---|
| `pandas` | Lectura y transformación de CSV |
| `sqlalchemy` | Conexión a MySQL y operaciones DDL/DML |
| `mysql-connector-python` | Driver MySQL para SQLAlchemy |
| `pytest` | Tests unitarios |

---

## Aportaciones del equipo

### Ken's POV
Me encargué de todo lo relacionado con la base de datos. Primero estuve investigando cómo conectar SQLAlchemy con MySQL porque al principio no nos salía bien el string de conexión. Una vez que funcionó, escribí las sentencias DDL para crear las tablas `clientes` y `tarjetas` con los tipos de datos correctos para MySQL (VARCHAR, CHAR...). También añadí la lógica para que si las tablas ya existen no las sobreescriba, y que la `fecha_carga` quede como parte de la clave primaria para poder cargar ficheros de distintas fechas sin conflictos. Al final tuve que probar bastante con los tipos de columna porque MySQL es más estricto que SQLite.

### Laura's POV
Me encargué de la parte ETL, que era la más grande. Lo que más me costó fue el tema de la validación del DNI porque tuve que buscar cómo funciona la letra de control (el módulo 23 con la tabla de letras). También me peleé bastante con los nombres compuestos tipo `Ana-María` porque si hacías `.capitalize()` directamente te rompía el guion. Al final lo resolví dividiendo por `-` y capitalizando cada parte por separado. La anonimización la decidimos en grupo: para la tarjeta usamos enmascarado + hash, y para el CVV solo hash porque es un dato que nunca debería salir en ningún formato.

### Naza's POV
Mi parte fue principalmente los tests y la documentación. Escribí los tests con pytest para las funciones de validación y los de enmascarado de tarjeta — quería asegurarme de que los casos raros (teléfonos con prefijo, DNIs con espacios, tarjetas sin separadores) funcionaban bien. También organicé la estructura de carpetas porque al principio teníamos los ficheros bastante mezclados, y redacté el README. Aparte ayudé a depurar un par de cosas cuando el pipeline fallaba silenciosamente sin dar error claro.
