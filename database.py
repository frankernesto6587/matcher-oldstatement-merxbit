import sqlite3
import hashlib
from datetime import datetime
import os

DATABASE_PATH = 'data/match_bancario.db'


def get_db():
    """Obtiene conexión a la base de datos"""
    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    """Inicializa las tablas de la base de datos"""
    os.makedirs('data', exist_ok=True)
    conn = get_db()
    cursor = conn.cursor()

    # Tabla operaciones_banco
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS operaciones_banco (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash_unico TEXT UNIQUE,
            row_original INTEGER,
            fecha DATE,
            codigo_banco TEXT,
            nombre TEXT,
            monto REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Tabla operaciones_ventas
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS operaciones_ventas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hash_unico TEXT UNIQUE,
            row_original INTEGER,
            factura TEXT,
            codigo_venta TEXT,
            fecha DATE,
            nombre TEXT,
            monto REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')

    # Tabla matches
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_code TEXT UNIQUE,
            banco_id INTEGER,
            venta_id INTEGER,
            match_tipo TEXT,
            confianza TEXT,
            estado TEXT DEFAULT 'PENDIENTE',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            confirmed_at TIMESTAMP,
            FOREIGN KEY (banco_id) REFERENCES operaciones_banco(id),
            FOREIGN KEY (venta_id) REFERENCES operaciones_ventas(id)
        )
    ''')

    # Índices para búsqueda rápida
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_banco_monto ON operaciones_banco(monto)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_banco_fecha ON operaciones_banco(fecha)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ventas_monto ON operaciones_ventas(monto)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_ventas_fecha ON operaciones_ventas(fecha)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_matches_estado ON matches(estado)')

    conn.commit()
    conn.close()


def generar_hash_banco(fecha, monto, codigo_banco, nombre):
    """Genera hash único para operación de banco"""
    datos = f"{fecha}|{monto}|{codigo_banco}|{nombre}".lower().strip()
    return hashlib.sha256(datos.encode()).hexdigest()[:16]


def generar_hash_venta(factura, fecha, monto, nombre, codigo_venta):
    """Genera hash único para operación de venta"""
    datos = f"{factura}|{fecha}|{monto}|{nombre}|{codigo_venta}".lower().strip()
    return hashlib.sha256(datos.encode()).hexdigest()[:16]


def generar_match_code():
    """Genera código de match de 6 caracteres"""
    import random
    import string
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))


def determinar_estado_match(match_tipo, confianza):
    """
    Determina el estado del match según las reglas:
    - CONFIRMADO: CODIGO_MONTO_EXACTO+ALTO, CODIGO_EXACTO+ALTO, MEDIO (100%)
    - PENDIENTE: MEDIO (cualquier %), CODIGO_EXACTO+MEDIO
    - SIN_MATCH: BAJO, MUY_BAJO, MONTO_UNICO, SIN_MATCH, SIN_MATCH_BANCO
    """
    if not match_tipo or not confianza:
        return None  # Sin match

    confianza_upper = str(confianza).upper()
    match_tipo_upper = str(match_tipo).upper()

    # SIN MATCH automático
    if 'SIN_MATCH' in match_tipo_upper:
        return None
    if 'MUY_BAJO' in confianza_upper or 'MUY BAJO' in confianza_upper:
        return None
    if confianza_upper == 'BAJO':
        return None
    if 'MONTO_UNICO' in match_tipo_upper or 'MONTO UNICO' in match_tipo_upper:
        return None

    # CONFIRMADO automático
    # V4: CODIGO_MONTO_EXACTO + ALTO
    if match_tipo_upper == 'CODIGO_MONTO_EXACTO' and confianza_upper == 'ALTO':
        return 'CONFIRMADO'
    # V3: CODIGO_EXACTO + ALTO
    if match_tipo_upper == 'CODIGO_EXACTO' and confianza_upper == 'ALTO':
        return 'CONFIRMADO'
    # MEDIO (100%)
    if 'MEDIO (100%)' in confianza_upper or 'MEDIO(100%)' in confianza_upper:
        return 'CONFIRMADO'

    # PENDIENTE - cualquier MEDIO con porcentaje
    if 'MEDIO' in confianza_upper:
        return 'PENDIENTE'

    return None  # Por defecto sin match


def insertar_banco(conn, row_original, fecha, codigo_banco, nombre, monto):
    """Inserta operación de banco si no existe"""
    # Convertir fecha a string si es Timestamp
    fecha_str = str(fecha)[:10] if fecha is not None else None
    hash_unico = generar_hash_banco(fecha_str, monto, codigo_banco, nombre)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO operaciones_banco
            (hash_unico, row_original, fecha, codigo_banco, nombre, monto)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (hash_unico, row_original, fecha_str, codigo_banco, nombre, monto))

        # Obtener el ID (sea nuevo o existente)
        cursor.execute('SELECT id FROM operaciones_banco WHERE hash_unico = ?', (hash_unico,))
        result = cursor.fetchone()
        return result['id'] if result else None
    except Exception as e:
        print(f"Error insertando banco: {e}")
        return None


def insertar_venta(conn, row_original, factura, codigo_venta, fecha, nombre, monto):
    """Inserta operación de venta si no existe"""
    # Convertir fecha a string si es Timestamp
    fecha_str = str(fecha)[:10] if fecha is not None else None
    hash_unico = generar_hash_venta(factura, fecha_str, monto, nombre, codigo_venta)
    cursor = conn.cursor()
    try:
        cursor.execute('''
            INSERT OR IGNORE INTO operaciones_ventas
            (hash_unico, row_original, factura, codigo_venta, fecha, nombre, monto)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (hash_unico, row_original, factura, codigo_venta, fecha_str, nombre, monto))

        # Obtener el ID (sea nuevo o existente)
        cursor.execute('SELECT id FROM operaciones_ventas WHERE hash_unico = ?', (hash_unico,))
        result = cursor.fetchone()
        return result['id'] if result else None
    except Exception as e:
        print(f"Error insertando venta: {e}")
        return None


def insertar_match(conn, banco_id, venta_id, match_tipo, confianza, estado):
    """Inserta match si no existe"""
    cursor = conn.cursor()

    # Verificar si ya existe match para este par
    cursor.execute('''
        SELECT id FROM matches WHERE banco_id = ? AND venta_id = ?
    ''', (banco_id, venta_id))

    if cursor.fetchone():
        return None  # Ya existe

    match_code = generar_match_code()
    confirmed_at = datetime.now().isoformat() if estado == 'CONFIRMADO' else None

    try:
        cursor.execute('''
            INSERT INTO matches (match_code, banco_id, venta_id, match_tipo, confianza, estado, confirmed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (match_code, banco_id, venta_id, match_tipo, confianza, estado, confirmed_at))
        return cursor.lastrowid
    except Exception as e:
        print(f"Error insertando match: {e}")
        return None


def insertar_match_con_codigo(conn, banco_id, venta_id, match_tipo, confianza, estado, match_code):
    """Inserta match con un código existente (para archivos que ya tienen Match_Code)"""
    cursor = conn.cursor()

    # Verificar si ya existe match para este par
    cursor.execute('''
        SELECT id FROM matches WHERE banco_id = ? AND venta_id = ?
    ''', (banco_id, venta_id))

    if cursor.fetchone():
        return None  # Ya existe

    confirmed_at = datetime.now().isoformat() if estado == 'CONFIRMADO' else None

    try:
        cursor.execute('''
            INSERT INTO matches (match_code, banco_id, venta_id, match_tipo, confianza, estado, confirmed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (match_code, banco_id, venta_id, match_tipo, confianza, estado, confirmed_at))
        return cursor.lastrowid
    except Exception as e:
        print(f"Error insertando match con código: {e}")
        return None


def get_stats(fecha_desde=None, fecha_hasta=None):
    """
    Obtiene estadísticas de la base de datos.

    Args:
        fecha_desde: Fecha inicio para filtrar matches (YYYY-MM-DD)
        fecha_hasta: Fecha fin para filtrar matches (YYYY-MM-DD)

    Si se pasan fechas, filtra Confirmados, Pendientes y Sin Match.
    Los totales siempre son globales.
    """
    conn = get_db()
    cursor = conn.cursor()

    stats = {}

    # Totales siempre globales (sin filtro de fecha)
    cursor.execute('SELECT COUNT(*) as count FROM operaciones_banco')
    stats['total_banco'] = cursor.fetchone()['count']

    cursor.execute('SELECT COUNT(*) as count FROM operaciones_ventas')
    stats['total_ventas'] = cursor.fetchone()['count']

    # Construir filtro de fecha para matches
    fecha_filter = ""
    fecha_params = []
    if fecha_desde and fecha_hasta:
        fecha_filter = """
            AND (
                (v.fecha >= ? AND v.fecha <= ?)
                OR (b.fecha >= ? AND b.fecha <= ?)
            )
        """
        fecha_params = [fecha_desde, fecha_hasta, fecha_desde, fecha_hasta]

    # Confirmados (con filtro de fecha si aplica)
    query = f'''
        SELECT COUNT(*) as count FROM matches m
        JOIN operaciones_ventas v ON m.venta_id = v.id
        JOIN operaciones_banco b ON m.banco_id = b.id
        WHERE m.estado = 'CONFIRMADO' {fecha_filter}
    '''
    cursor.execute(query, fecha_params)
    stats['confirmados'] = cursor.fetchone()['count']

    # Pendientes (con filtro de fecha si aplica)
    query = f'''
        SELECT COUNT(*) as count FROM matches m
        JOIN operaciones_ventas v ON m.venta_id = v.id
        JOIN operaciones_banco b ON m.banco_id = b.id
        WHERE m.estado = 'PENDIENTE' {fecha_filter}
    '''
    cursor.execute(query, fecha_params)
    stats['pendientes'] = cursor.fetchone()['count']

    # Sin match banco (con filtro de fecha si aplica)
    if fecha_desde and fecha_hasta:
        cursor.execute('''
            SELECT COUNT(*) as count
            FROM operaciones_banco b
            LEFT JOIN matches m ON b.id = m.banco_id
            WHERE m.id IS NULL
            AND b.fecha >= ? AND b.fecha <= ?
        ''', [fecha_desde, fecha_hasta])
    else:
        cursor.execute('''
            SELECT COUNT(*) as count
            FROM operaciones_banco b
            LEFT JOIN matches m ON b.id = m.banco_id
            WHERE m.id IS NULL
        ''')
    stats['banco_sin_match'] = cursor.fetchone()['count']

    # Sin match ventas (con filtro de fecha si aplica)
    if fecha_desde and fecha_hasta:
        cursor.execute('''
            SELECT COUNT(*) as count
            FROM operaciones_ventas v
            LEFT JOIN matches m ON v.id = m.venta_id
            WHERE m.id IS NULL
            AND v.fecha >= ? AND v.fecha <= ?
        ''', [fecha_desde, fecha_hasta])
    else:
        cursor.execute('''
            SELECT COUNT(*) as count
            FROM operaciones_ventas v
            LEFT JOIN matches m ON v.id = m.venta_id
            WHERE m.id IS NULL
        ''')
    stats['ventas_sin_match'] = cursor.fetchone()['count']

    # Rango de fechas banco (excluyendo nulos) - siempre global
    cursor.execute('''
        SELECT MIN(fecha) as min_fecha, MAX(fecha) as max_fecha
        FROM operaciones_banco
        WHERE fecha IS NOT NULL AND fecha != '' AND fecha NOT LIKE '%NaT%'
    ''')
    row = cursor.fetchone()
    stats['banco_fecha_min'] = row['min_fecha']
    stats['banco_fecha_max'] = row['max_fecha']

    # Rango de fechas ventas (excluyendo nulos) - siempre global
    cursor.execute('''
        SELECT MIN(fecha) as min_fecha, MAX(fecha) as max_fecha
        FROM operaciones_ventas
        WHERE fecha IS NOT NULL AND fecha != '' AND fecha NOT LIKE '%NaT%'
    ''')
    row = cursor.fetchone()
    stats['ventas_fecha_min'] = row['min_fecha']
    stats['ventas_fecha_max'] = row['max_fecha']

    # Guardar filtro aplicado
    stats['filtro_fecha_desde'] = fecha_desde
    stats['filtro_fecha_hasta'] = fecha_hasta

    conn.close()
    return stats


def get_matches_pendientes(limit=50, offset=0):
    """Obtiene matches pendientes de aprobación"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            m.id, m.match_code, m.match_tipo, m.confianza, m.estado,
            v.row_original as row_venta, v.factura, v.codigo_venta, v.fecha as fecha_venta,
            v.nombre as nombre_venta, v.monto as monto_venta,
            b.row_original as row_banco, b.codigo_banco, b.fecha as fecha_banco,
            b.nombre as nombre_banco, b.monto as monto_banco
        FROM matches m
        JOIN operaciones_ventas v ON m.venta_id = v.id
        JOIN operaciones_banco b ON m.banco_id = b.id
        WHERE m.estado = 'PENDIENTE'
        ORDER BY m.id
        LIMIT ? OFFSET ?
    ''', (limit, offset))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_matches_confirmados(limit=50, offset=0):
    """Obtiene matches confirmados"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT
            m.id, m.match_code, m.match_tipo, m.confianza, m.estado,
            v.row_original as row_venta, v.factura, v.codigo_venta, v.fecha as fecha_venta,
            v.nombre as nombre_venta, v.monto as monto_venta,
            b.row_original as row_banco, b.codigo_banco, b.fecha as fecha_banco,
            b.nombre as nombre_banco, b.monto as monto_banco
        FROM matches m
        JOIN operaciones_ventas v ON m.venta_id = v.id
        JOIN operaciones_banco b ON m.banco_id = b.id
        WHERE m.estado = 'CONFIRMADO'
        ORDER BY m.confirmed_at DESC
        LIMIT ? OFFSET ?
    ''', (limit, offset))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_ventas_sin_match(limit=50, offset=0):
    """Obtiene ventas sin match"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT v.*
        FROM operaciones_ventas v
        LEFT JOIN matches m ON v.id = m.venta_id
        WHERE m.id IS NULL
        ORDER BY v.fecha DESC, v.monto DESC
        LIMIT ? OFFSET ?
    ''', (limit, offset))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def get_banco_sin_match(limit=50, offset=0):
    """Obtiene operaciones de banco sin match"""
    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('''
        SELECT b.*
        FROM operaciones_banco b
        LEFT JOIN matches m ON b.id = m.banco_id
        WHERE m.id IS NULL
        ORDER BY b.fecha DESC, b.monto DESC
        LIMIT ? OFFSET ?
    ''', (limit, offset))

    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def aprobar_match(match_id):
    """Aprueba un match pendiente"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE matches
        SET estado = 'CONFIRMADO', confirmed_at = ?
        WHERE id = ? AND estado = 'PENDIENTE'
    ''', (datetime.now().isoformat(), match_id))
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return affected > 0


def rechazar_match(match_id):
    """Rechaza y elimina un match pendiente"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM matches WHERE id = ? AND estado = "PENDIENTE"', (match_id,))
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return affected > 0


def buscar_posibles_matches_para_venta(venta_id, criterios=None, limit=10):
    """
    Busca posibles matches en banco para una venta sin match.

    criterios: dict con opciones de búsqueda
        - monto: 'exacto', '1%', '5%', '10%', 'cualquiera'
        - fecha: 3, 7, 15, 30, None (cualquiera)
        - nombre: True/False (buscar coincidencia parcial)
        - codigo: True/False (buscar coincidencia parcial)
    """
    if criterios is None:
        criterios = {'monto': 'exacto', 'fecha': 7, 'nombre': False, 'codigo': False}

    conn = get_db()
    cursor = conn.cursor()

    # Obtener datos de la venta
    cursor.execute('SELECT * FROM operaciones_ventas WHERE id = ?', (venta_id,))
    venta = cursor.fetchone()

    if not venta:
        conn.close()
        return []

    # Construir query dinámica
    conditions = ["m.id IS NULL"]  # Sin match existente
    params = []

    # Criterio de monto
    monto_criterio = criterios.get('monto', 'exacto')
    if monto_criterio == 'exacto':
        conditions.append("b.monto = ?")
        params.append(venta['monto'])
    elif monto_criterio == '1%':
        conditions.append("b.monto BETWEEN ? AND ?")
        params.extend([venta['monto'] * 0.99, venta['monto'] * 1.01])
    elif monto_criterio == '5%':
        conditions.append("b.monto BETWEEN ? AND ?")
        params.extend([venta['monto'] * 0.95, venta['monto'] * 1.05])
    elif monto_criterio == '10%':
        conditions.append("b.monto BETWEEN ? AND ?")
        params.extend([venta['monto'] * 0.90, venta['monto'] * 1.10])
    # 'cualquiera' no agrega condición de monto

    # Criterio de fecha
    dias_fecha = criterios.get('fecha')
    if dias_fecha:
        conditions.append("ABS(julianday(b.fecha) - julianday(?)) <= ?")
        params.extend([venta['fecha'], dias_fecha])

    # Criterio de nombre (búsqueda parcial)
    if criterios.get('nombre') and venta['nombre']:
        # Buscar coincidencia parcial del nombre
        nombre_partes = venta['nombre'].upper().split()[:2]  # Primeras 2 palabras
        for parte in nombre_partes:
            if len(parte) > 2:
                conditions.append("UPPER(b.nombre) LIKE ?")
                params.append(f"%{parte}%")

    # Criterio de código (búsqueda parcial)
    if criterios.get('codigo') and venta['codigo_venta']:
        # Buscar coincidencia parcial del código
        codigo_limpio = venta['codigo_venta'].upper().replace('-', '').replace('_', '')[:8]
        conditions.append("UPPER(REPLACE(REPLACE(b.codigo_banco, '-', ''), '_', '')) LIKE ?")
        params.append(f"%{codigo_limpio}%")

    # Construir query
    where_clause = " AND ".join(conditions)

    query = f'''
        SELECT b.*,
               ABS(julianday(b.fecha) - julianday(?)) as dias_diferencia,
               ABS(b.monto - ?) as diferencia_monto
        FROM operaciones_banco b
        LEFT JOIN matches m ON b.id = m.banco_id
        WHERE {where_clause}
        ORDER BY diferencia_monto ASC, dias_diferencia ASC
        LIMIT ?
    '''
    params = [venta['fecha'], venta['monto']] + params + [limit]

    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def buscar_posibles_matches_para_banco(banco_id, criterios=None, limit=10):
    """
    Busca posibles matches en ventas para una operación de banco sin match.
    """
    if criterios is None:
        criterios = {'monto': 'exacto', 'fecha': 7, 'nombre': False, 'codigo': False}

    conn = get_db()
    cursor = conn.cursor()

    cursor.execute('SELECT * FROM operaciones_banco WHERE id = ?', (banco_id,))
    banco = cursor.fetchone()

    if not banco:
        conn.close()
        return []

    conditions = ["m.id IS NULL"]
    params = []

    # Criterio de monto
    monto_criterio = criterios.get('monto', 'exacto')
    if monto_criterio == 'exacto':
        conditions.append("v.monto = ?")
        params.append(banco['monto'])
    elif monto_criterio == '1%':
        conditions.append("v.monto BETWEEN ? AND ?")
        params.extend([banco['monto'] * 0.99, banco['monto'] * 1.01])
    elif monto_criterio == '5%':
        conditions.append("v.monto BETWEEN ? AND ?")
        params.extend([banco['monto'] * 0.95, banco['monto'] * 1.05])
    elif monto_criterio == '10%':
        conditions.append("v.monto BETWEEN ? AND ?")
        params.extend([banco['monto'] * 0.90, banco['monto'] * 1.10])

    # Criterio de fecha
    dias_fecha = criterios.get('fecha')
    if dias_fecha:
        conditions.append("ABS(julianday(v.fecha) - julianday(?)) <= ?")
        params.extend([banco['fecha'], dias_fecha])

    # Criterio de nombre
    if criterios.get('nombre') and banco['nombre']:
        nombre_partes = banco['nombre'].upper().split()[:2]
        for parte in nombre_partes:
            if len(parte) > 2:
                conditions.append("UPPER(v.nombre) LIKE ?")
                params.append(f"%{parte}%")

    # Criterio de código
    if criterios.get('codigo') and banco['codigo_banco']:
        codigo_limpio = banco['codigo_banco'].upper().replace('-', '').replace('_', '')[:8]
        conditions.append("UPPER(REPLACE(REPLACE(v.codigo_venta, '-', ''), '_', '')) LIKE ?")
        params.append(f"%{codigo_limpio}%")

    where_clause = " AND ".join(conditions)

    query = f'''
        SELECT v.*,
               ABS(julianday(v.fecha) - julianday(?)) as dias_diferencia,
               ABS(v.monto - ?) as diferencia_monto
        FROM operaciones_ventas v
        LEFT JOIN matches m ON v.id = m.venta_id
        WHERE {where_clause}
        ORDER BY diferencia_monto ASC, dias_diferencia ASC
        LIMIT ?
    '''
    params = [banco['fecha'], banco['monto']] + params + [limit]

    cursor.execute(query, params)
    results = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return results


def crear_match_manual(banco_id, venta_id):
    """Crea un match manual (confirmado)"""
    conn = get_db()

    # Verificar que no existan matches previos
    cursor = conn.cursor()
    cursor.execute('SELECT id FROM matches WHERE banco_id = ? OR venta_id = ?', (banco_id, venta_id))
    if cursor.fetchone():
        conn.close()
        return None, "Ya existe un match para esta operación"

    match_code = generar_match_code()
    cursor.execute('''
        INSERT INTO matches (match_code, banco_id, venta_id, match_tipo, confianza, estado, confirmed_at)
        VALUES (?, ?, ?, 'MANUAL', 'MANUAL', 'CONFIRMADO', ?)
    ''', (match_code, banco_id, venta_id, datetime.now().isoformat()))

    conn.commit()
    match_id = cursor.lastrowid
    conn.close()
    return match_id, None


def reset_database():
    """Limpia todas las tablas"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('DELETE FROM matches')
    cursor.execute('DELETE FROM operaciones_banco')
    cursor.execute('DELETE FROM operaciones_ventas')
    conn.commit()
    conn.close()


# Inicializar base de datos al importar
init_db()
