-- ============================================================================
-- Trabajo Práctico Especial - Base de Datos I
-- Implementación completa utilizando PostgreSQL / PLpgSQL
-- ============================================================================

-- ============================================================================
-- Punto (0): Eliminación previa para garantizar re-ejecución
-- ============================================================================

DROP TABLE IF EXISTS PAGO CASCADE;
DROP TABLE IF EXISTS SUSCRIPCION CASCADE;
DROP FUNCTION IF EXISTS procesar_pago() CASCADE;
DROP FUNCTION IF EXISTS consolidar_cliente(TEXT) CASCADE;

-- ============================================================================
-- Punto (a): creación de tablas PAGO y SUSCRIPCION
-- ============================================================================

CREATE TABLE SUSCRIPCION (
    id              INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    cliente_email   TEXT NOT NULL,
    tipo            TEXT NOT NULL CHECK (tipo IN ('nueva', 'renovacion')),
    modalidad       TEXT NOT NULL CHECK (modalidad IN ('mensual', 'anual')),
    fecha_inicio    DATE NOT NULL,
    fecha_fin       DATE NOT NULL,
    CHECK (fecha_fin >= fecha_inicio)
);

CREATE TABLE PAGO (
    fecha           DATE NOT NULL,
    medio_pago      TEXT NOT NULL CHECK (medio_pago IN (
                        'tarjeta_credito', 'tarjeta_debito', 'transferencia',
                        'efectivo', 'mercadopago')),
    id_transaccion  TEXT PRIMARY KEY,
    cliente_email   TEXT NOT NULL,
    modalidad       TEXT NOT NULL CHECK (modalidad IN ('mensual', 'anual')),
    monto           NUMERIC(12,2) NOT NULL CHECK (monto > 0),
    suscripcion_id  INTEGER NOT NULL REFERENCES SUSCRIPCION(id)
);

-- ============================================================================
-- Punto (b): función y trigger procesar_pago()
-- ============================================================================

CREATE OR REPLACE FUNCTION procesar_pago()
RETURNS TRIGGER
AS $$
DECLARE
    v_ultima_misma_modalidad   SUSCRIPCION%ROWTYPE;
    v_inicio                   DATE;
    v_fin                      DATE;
    v_tipo                     TEXT;
    v_superpuesta_id           INTEGER;
    v_superpuesta_inicio       DATE;
    v_superpuesta_fin          DATE;
BEGIN
    SELECT *
      INTO v_ultima_misma_modalidad
      FROM SUSCRIPCION
     WHERE cliente_email = NEW.cliente_email
       AND modalidad = NEW.modalidad
     ORDER BY fecha_fin DESC
     LIMIT 1;

    IF NOT FOUND THEN
        v_tipo := 'nueva';
        v_inicio := NEW.fecha;
    ELSE
        IF NEW.fecha > v_ultima_misma_modalidad.fecha_fin THEN
            v_tipo := 'nueva';
            v_inicio := NEW.fecha;
        ELSE
            IF NEW.fecha < v_ultima_misma_modalidad.fecha_fin - INTERVAL '30 days' THEN
                RAISE EXCEPTION 'Pago % rechazado: las renovaciones solo se admiten dentro de los 30 dias previos al vencimiento % (cliente %).',
                    NEW.id_transaccion, v_ultima_misma_modalidad.fecha_fin, NEW.cliente_email;
            END IF;
            v_tipo := 'renovacion';
            v_inicio := v_ultima_misma_modalidad.fecha_fin + 1;
        END IF;
    END IF;

    IF NEW.modalidad = 'mensual' THEN
        v_fin := ((v_inicio + INTERVAL '1 month') - INTERVAL '1 day')::date;
    ELSE
        v_fin := ((v_inicio + INTERVAL '1 year') - INTERVAL '1 day')::date;
    END IF;

    SELECT s.id, s.fecha_inicio, s.fecha_fin
      INTO v_superpuesta_id, v_superpuesta_inicio, v_superpuesta_fin
      FROM SUSCRIPCION s
     WHERE s.cliente_email = NEW.cliente_email
       AND s.fecha_inicio <= v_fin
       AND v_inicio <= s.fecha_fin
     ORDER BY s.fecha_inicio
     LIMIT 1;

    IF FOUND THEN
        RAISE EXCEPTION 'Pago % rechazado: la cobertura % a % se superpone con la suscripcion % (% a %).',
            NEW.id_transaccion, v_inicio, v_fin, v_superpuesta_id, v_superpuesta_inicio, v_superpuesta_fin;
    END IF;

    INSERT INTO SUSCRIPCION(cliente_email, tipo, modalidad, fecha_inicio, fecha_fin)
    VALUES (NEW.cliente_email, v_tipo, NEW.modalidad, v_inicio, v_fin)
    RETURNING id INTO NEW.suscripcion_id;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_procesar_pago
BEFORE INSERT ON PAGO
FOR EACH ROW
EXECUTE PROCEDURE procesar_pago();

-- ============================================================================
-- Punto (c): Importación de los datos desde pagos.csv
-- ============================================================================

-- Ajustar la ruta si el archivo pagos.csv se encuentra en otra ubicación.
COPY PAGO(fecha, medio_pago, id_transaccion, cliente_email, modalidad, monto)
FROM '/home/eugemigliaro/Documents/facultad/tercero/bd_tpe/pagos.csv'
WITH (FORMAT csv, HEADER true, DELIMITER ',');

-- ============================================================================
-- Punto (d): función de consolidación consolidar_cliente(cliente_email)
-- ============================================================================

CREATE OR REPLACE FUNCTION consolidar_cliente(p_email TEXT)
RETURNS VOID
AS $$
DECLARE
    rec                 RECORD;
    v_periodo           INTEGER := 0;
    v_periodo_inicio    DATE;
    v_periodo_fin       DATE;
    v_total_periodo     INTEGER := 0;
    v_total_general     INTEGER := 0;
    v_meses_actual      INTEGER;
BEGIN
    FOR rec IN
        SELECT s.id, s.tipo, s.modalidad, s.fecha_inicio, s.fecha_fin,
               p.fecha AS pago_fecha, p.medio_pago
          FROM SUSCRIPCION s
          JOIN PAGO p ON p.suscripcion_id = s.id
         WHERE s.cliente_email = p_email
         ORDER BY s.fecha_inicio
    LOOP
        IF v_periodo = 0 THEN
            RAISE NOTICE '== Cliente: % ==', p_email;
            v_periodo := 1;
            RAISE NOTICE 'Periodo #%s', v_periodo;
            v_periodo_inicio := rec.fecha_inicio;
            v_total_periodo := 0;
        ELSE
            IF v_periodo_fin IS NOT NULL AND rec.fecha_inicio > v_periodo_fin + 1 THEN
                RAISE NOTICE '(Fin del periodo #%s: %s a %s) | Total periodo: %s',
                    v_periodo,
                    v_periodo_inicio,
                    v_periodo_fin,
                    CASE WHEN v_total_periodo = 1 THEN '1 mes' ELSE v_total_periodo || ' meses' END;
                v_total_general := v_total_general + v_total_periodo;
                RAISE NOTICE '--- PERIODO DE BAJA ---';
                v_periodo := v_periodo + 1;
                RAISE NOTICE 'Periodo #%s', v_periodo;
                v_periodo_inicio := rec.fecha_inicio;
                v_total_periodo := 0;
            END IF;
        END IF;

        v_periodo_fin := rec.fecha_fin;
        v_meses_actual := CASE WHEN rec.modalidad = 'mensual' THEN 1 ELSE 12 END;
        v_total_periodo := v_total_periodo + v_meses_actual;

        RAISE NOTICE '%', format('%s %s (%s) | pago=%s medio=%s | cobertura=%s a %s',
            upper(rec.tipo),
            upper(rec.modalidad),
            CASE WHEN rec.modalidad = 'mensual' THEN '1 mes' ELSE '12 meses' END,
            rec.pago_fecha,
            rec.medio_pago,
            rec.fecha_inicio,
            rec.fecha_fin);
    END LOOP;

    IF v_periodo = 0 THEN
        RAISE NOTICE '== Cliente: % ==', p_email;
        RAISE NOTICE 'No posee suscripciones registradas.';
    ELSE
        RAISE NOTICE '(Fin del periodo #%s: %s a %s) | Total periodo: %s',
            v_periodo,
            v_periodo_inicio,
            v_periodo_fin,
            CASE WHEN v_total_periodo = 1 THEN '1 mes' ELSE v_total_periodo || ' meses' END;
        v_total_general := v_total_general + v_total_periodo;
        RAISE NOTICE '== Total acumulado: %s ==',
            CASE WHEN v_total_general = 1 THEN '1 mes' ELSE v_total_general || ' meses' END;
    END IF;
END;
$$ LANGUAGE plpgsql;
