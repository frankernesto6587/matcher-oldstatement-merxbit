from flask import Flask, render_template, request, redirect, url_for, flash, jsonify, send_file
import pandas as pd
from datetime import datetime
import os
import io
from database import (
    init_db, get_db, get_stats,
    insertar_banco, insertar_venta, insertar_match, insertar_match_con_codigo,
    determinar_estado_match,
    get_matches_pendientes, get_matches_confirmados,
    get_ventas_sin_match, get_banco_sin_match,
    aprobar_match, rechazar_match,
    buscar_posibles_matches_para_venta, crear_match_manual,
    reset_database
)

app = Flask(__name__)
app.secret_key = 'match_bancario_secret_key_2026'

UPLOAD_FOLDER = 'uploads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)


@app.route('/')
def index():
    """Dashboard principal"""
    fecha_desde = request.args.get('fecha_desde')
    fecha_hasta = request.args.get('fecha_hasta')
    stats = get_stats(fecha_desde=fecha_desde, fecha_hasta=fecha_hasta)
    return render_template('index.html', stats=stats)


@app.route('/upload', methods=['GET', 'POST'])
def upload():
    """Subir archivo fusionado"""
    # Verificar si ya hay datos en la DB
    stats = get_stats()
    if stats['total_banco'] > 0 or stats['total_ventas'] > 0:
        flash('Ya hay datos en la base de datos. Resetea la DB primero si quieres subir un nuevo archivo.', 'error')
        return redirect(url_for('index'))

    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No se seleccionó archivo', 'error')
            return redirect(url_for('upload'))

        file = request.files['file']
        if file.filename == '':
            flash('No se seleccionó archivo', 'error')
            return redirect(url_for('upload'))

        if not file.filename.endswith('.xlsx'):
            flash('El archivo debe ser .xlsx', 'error')
            return redirect(url_for('upload'))

        try:
            # Guardar archivo
            filepath = os.path.join(UPLOAD_FOLDER, f'upload_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
            file.save(filepath)

            # Procesar archivo
            result = procesar_archivo(filepath)

            flash(f'Archivo procesado: {result["nuevos_banco"]} banco, {result["nuevos_ventas"]} ventas, '
                  f'{result["confirmados"]} confirmados, {result["pendientes"]} pendientes', 'success')
            return redirect(url_for('index'))

        except Exception as e:
            flash(f'Error procesando archivo: {str(e)}', 'error')
            return redirect(url_for('upload'))

    return render_template('upload.html')


def procesar_archivo(filepath):
    """Procesa el archivo fusionado e importa a SQLite"""
    df = pd.read_excel(filepath)

    conn = get_db()
    result = {
        'nuevos_banco': 0,
        'nuevos_ventas': 0,
        'confirmados': 0,
        'pendientes': 0,
        'sin_match': 0
    }

    for _, row in df.iterrows():
        banco_id = None
        venta_id = None

        # Insertar banco si tiene datos
        if pd.notna(row.get('row_banco')) and pd.notna(row.get('Monto_Banco')):
            banco_id = insertar_banco(
                conn,
                row_original=int(row['row_banco']) if pd.notna(row['row_banco']) else None,
                fecha=row.get('Fecha_Banco'),
                codigo_banco=row.get('codigo_banco'),
                nombre=row.get('Nombre_Banco'),
                monto=row.get('Monto_Banco')
            )
            if banco_id:
                result['nuevos_banco'] += 1

        # Insertar venta si tiene datos
        if pd.notna(row.get('row_venta')) and pd.notna(row.get('Monto_Venta')):
            venta_id = insertar_venta(
                conn,
                row_original=int(row['row_venta']) if pd.notna(row['row_venta']) else None,
                factura=row.get('Factura'),
                codigo_venta=row.get('Codigo_venta'),
                fecha=row.get('Fecha_Venta'),
                nombre=row.get('Nombre_Venta'),
                monto=row.get('Monto_Venta')
            )
            if venta_id:
                result['nuevos_ventas'] += 1

        # Crear match si hay ambos
        if banco_id and venta_id:
            match_tipo = row.get('Match_Tipo')
            confianza = row.get('Confianza')
            match_code_existente = row.get('Match_Code')

            # Si ya tiene Match_Code, es CONFIRMADO automáticamente
            if pd.notna(match_code_existente) and str(match_code_existente).strip():
                estado = 'CONFIRMADO'
                match_id = insertar_match_con_codigo(
                    conn, banco_id, venta_id, match_tipo, confianza,
                    estado, str(match_code_existente).strip()
                )
                if match_id:
                    result['confirmados'] += 1
            else:
                # Sin Match_Code, evaluar según reglas
                estado = determinar_estado_match(match_tipo, confianza)
                if estado:
                    match_id = insertar_match(conn, banco_id, venta_id, match_tipo, confianza, estado)
                    if match_id:
                        if estado == 'CONFIRMADO':
                            result['confirmados'] += 1
                        else:
                            result['pendientes'] += 1
                else:
                    result['sin_match'] += 1

    conn.commit()
    conn.close()
    return result


@app.route('/pendientes')
def pendientes():
    """Lista de matches pendientes de aprobación"""
    page = request.args.get('page', 1, type=int)
    limit = 20
    offset = (page - 1) * limit

    matches = get_matches_pendientes(limit=limit, offset=offset)
    stats = get_stats()

    return render_template('pendientes.html', matches=matches, stats=stats, page=page)


@app.route('/confirmados')
def confirmados():
    """Lista de matches confirmados"""
    page = request.args.get('page', 1, type=int)
    limit = 20
    offset = (page - 1) * limit

    matches = get_matches_confirmados(limit=limit, offset=offset)
    stats = get_stats()

    return render_template('confirmados.html', matches=matches, stats=stats, page=page)


@app.route('/sin-match/ventas')
def sin_match_ventas():
    """Lista de ventas sin match"""
    page = request.args.get('page', 1, type=int)
    limit = 20
    offset = (page - 1) * limit

    ventas = get_ventas_sin_match(limit=limit, offset=offset)
    stats = get_stats()

    return render_template('sin_match_ventas.html', ventas=ventas, stats=stats, page=page)


@app.route('/sin-match/banco')
def sin_match_banco():
    """Lista de operaciones de banco sin match"""
    page = request.args.get('page', 1, type=int)
    limit = 20
    offset = (page - 1) * limit

    banco = get_banco_sin_match(limit=limit, offset=offset)
    stats = get_stats()

    return render_template('sin_match_banco.html', banco=banco, stats=stats, page=page)


@app.route('/api/aprobar/<int:match_id>', methods=['POST'])
def api_aprobar(match_id):
    """API para aprobar un match"""
    if aprobar_match(match_id):
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'No se pudo aprobar'}), 400


@app.route('/api/rechazar/<int:match_id>', methods=['POST'])
def api_rechazar(match_id):
    """API para rechazar un match"""
    if rechazar_match(match_id):
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': 'No se pudo rechazar'}), 400


@app.route('/api/buscar-matches/<int:venta_id>')
def api_buscar_matches(venta_id):
    """API para buscar posibles matches para una venta"""
    # Obtener criterios de query params
    criterios = {
        'monto': request.args.get('monto', 'exacto'),
        'fecha': int(request.args.get('fecha', 7)) if request.args.get('fecha') else None,
        'nombre': request.args.get('nombre', 'false').lower() == 'true',
        'codigo': request.args.get('codigo', 'false').lower() == 'true'
    }
    posibles = buscar_posibles_matches_para_venta(venta_id, criterios)
    return jsonify(posibles)


@app.route('/api/buscar-matches-banco/<int:banco_id>')
def api_buscar_matches_banco(banco_id):
    """API para buscar posibles matches para una operación de banco"""
    from database import buscar_posibles_matches_para_banco
    criterios = {
        'monto': request.args.get('monto', 'exacto'),
        'fecha': int(request.args.get('fecha', 7)) if request.args.get('fecha') else None,
        'nombre': request.args.get('nombre', 'false').lower() == 'true',
        'codigo': request.args.get('codigo', 'false').lower() == 'true'
    }
    posibles = buscar_posibles_matches_para_banco(banco_id, criterios)
    return jsonify(posibles)


@app.route('/api/crear-match-manual', methods=['POST'])
def api_crear_match_manual():
    """API para crear match manual"""
    data = request.json
    banco_id = data.get('banco_id')
    venta_id = data.get('venta_id')

    if not banco_id or not venta_id:
        return jsonify({'success': False, 'error': 'Faltan parámetros'}), 400

    match_id, error = crear_match_manual(banco_id, venta_id)
    if error:
        return jsonify({'success': False, 'error': error}), 400

    return jsonify({'success': True, 'match_id': match_id})


@app.route('/api/aprobar-todos', methods=['POST'])
def api_aprobar_todos():
    """API para aprobar todos los matches pendientes"""
    conn = get_db()
    cursor = conn.cursor()
    cursor.execute('''
        UPDATE matches
        SET estado = 'CONFIRMADO', confirmed_at = ?
        WHERE estado = 'PENDIENTE'
    ''', (datetime.now().isoformat(),))
    conn.commit()
    affected = cursor.rowcount
    conn.close()
    return jsonify({'success': True, 'aprobados': affected})


@app.route('/reset', methods=['POST'])
def reset():
    """Resetea la base de datos"""
    reset_database()
    flash('Base de datos reseteada', 'success')
    return redirect(url_for('index'))


@app.route('/descargar-fusionado')
def descargar_fusionado():
    """Descarga archivo fusionado con los matches actuales"""
    conn = get_db()
    cursor = conn.cursor()

    fusionado = []

    # 1. Obtener matches confirmados
    cursor.execute('''
        SELECT
            v.row_original as row_venta, v.factura, v.codigo_venta, v.fecha as fecha_venta,
            v.nombre as nombre_venta, v.monto as monto_venta,
            b.row_original as row_banco, b.fecha as fecha_banco, b.codigo_banco,
            b.nombre as nombre_banco, b.monto as monto_banco,
            m.match_tipo, m.confianza, m.match_code
        FROM matches m
        JOIN operaciones_ventas v ON m.venta_id = v.id
        JOIN operaciones_banco b ON m.banco_id = b.id
        WHERE m.estado = 'CONFIRMADO'
    ''')
    for row in cursor.fetchall():
        fusionado.append({
            'row_venta': row['row_venta'],
            'Factura': row['factura'],
            'Codigo_venta': row['codigo_venta'],
            'Fecha_Venta': row['fecha_venta'],
            'Nombre_Venta': row['nombre_venta'],
            'Monto_Venta': row['monto_venta'],
            'row_banco': row['row_banco'],
            'Fecha_Banco': row['fecha_banco'],
            'codigo_banco': row['codigo_banco'],
            'Nombre_Banco': row['nombre_banco'],
            'Monto_Banco': row['monto_banco'],
            'Match_Tipo': row['match_tipo'],
            'Confianza': row['confianza'],
            'Match_Code': row['match_code']
        })

    # 2. Obtener matches pendientes
    cursor.execute('''
        SELECT
            v.row_original as row_venta, v.factura, v.codigo_venta, v.fecha as fecha_venta,
            v.nombre as nombre_venta, v.monto as monto_venta,
            b.row_original as row_banco, b.fecha as fecha_banco, b.codigo_banco,
            b.nombre as nombre_banco, b.monto as monto_banco,
            m.match_tipo, m.confianza
        FROM matches m
        JOIN operaciones_ventas v ON m.venta_id = v.id
        JOIN operaciones_banco b ON m.banco_id = b.id
        WHERE m.estado = 'PENDIENTE'
    ''')
    for row in cursor.fetchall():
        fusionado.append({
            'row_venta': row['row_venta'],
            'Factura': row['factura'],
            'Codigo_venta': row['codigo_venta'],
            'Fecha_Venta': row['fecha_venta'],
            'Nombre_Venta': row['nombre_venta'],
            'Monto_Venta': row['monto_venta'],
            'row_banco': row['row_banco'],
            'Fecha_Banco': row['fecha_banco'],
            'codigo_banco': row['codigo_banco'],
            'Nombre_Banco': row['nombre_banco'],
            'Monto_Banco': row['monto_banco'],
            'Match_Tipo': row['match_tipo'],
            'Confianza': row['confianza'],
            'Match_Code': None  # Pendientes no tienen código aún
        })

    # 3. Obtener ventas sin match
    cursor.execute('''
        SELECT v.*
        FROM operaciones_ventas v
        LEFT JOIN matches m ON v.id = m.venta_id
        WHERE m.id IS NULL
    ''')
    for row in cursor.fetchall():
        fusionado.append({
            'row_venta': row['row_original'],
            'Factura': row['factura'],
            'Codigo_venta': row['codigo_venta'],
            'Fecha_Venta': row['fecha'],
            'Nombre_Venta': row['nombre'],
            'Monto_Venta': row['monto'],
            'row_banco': None,
            'Fecha_Banco': None,
            'codigo_banco': None,
            'Nombre_Banco': None,
            'Monto_Banco': None,
            'Match_Tipo': 'SIN_MATCH',
            'Confianza': None,
            'Match_Code': None
        })

    # 4. Obtener banco sin match
    cursor.execute('''
        SELECT b.*
        FROM operaciones_banco b
        LEFT JOIN matches m ON b.id = m.banco_id
        WHERE m.id IS NULL
    ''')
    for row in cursor.fetchall():
        fusionado.append({
            'row_venta': None,
            'Factura': None,
            'Codigo_venta': None,
            'Fecha_Venta': None,
            'Nombre_Venta': None,
            'Monto_Venta': None,
            'row_banco': row['row_original'],
            'Fecha_Banco': row['fecha'],
            'codigo_banco': row['codigo_banco'],
            'Nombre_Banco': row['nombre'],
            'Monto_Banco': row['monto'],
            'Match_Tipo': 'SIN_MATCH',
            'Confianza': None,
            'Match_Code': None
        })

    conn.close()

    # Crear DataFrame y exportar
    df = pd.DataFrame(fusionado)

    # Ordenar: confirmados primero, luego pendientes, luego sin match
    df['_orden'] = df['Match_Code'].apply(lambda x: 0 if pd.notna(x) else (1 if x is None else 2))
    df = df.sort_values('_orden').drop('_orden', axis=1)

    # Crear archivo Excel en memoria
    output = io.BytesIO()
    df.to_excel(output, index=False)
    output.seek(0)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return send_file(
        output,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        as_attachment=True,
        download_name=f'fusionado_matches_{timestamp}.xlsx'
    )


if __name__ == '__main__':
    init_db()
    app.run(debug=True, host='0.0.0.0', port=5000)
