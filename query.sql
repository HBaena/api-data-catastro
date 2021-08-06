WITH vulnerables AS 
(
    SELECT DISTINCT
                        T1.ncta,
                        T1.efectos,
                        T1.iduser,
                        T1.clave_mvto,
                        T1.nombre,
                        T1.descr,
                        T1.efectos AS efecto,
                        T1.status,
                        T1.imported,
                        T1.cve_cat,
                        T1.aprobado,
                        T1.aplicado,
                        T1.creationdate,
                        T1.deleted
    FROM            cip_grupo_vulnerable AS T1
    WHERE       status = 1
    ORDER BY    T1.ncta  -- 85,685
)
, padrones AS 
(
    SELECT DISTINCT 
                        T1.padron_id,
                        T1.preclavecatant,
                        T1.preubicacion,
                        T1.prevalcat,
                        T2.ncta,
                        T2.iduser,
                        T2.clave_mvto,
                        T2.nombre,
                        T2.descr,
                        T2.efecto,
                        T2.status,
                        T2.imported,
                        T2.cve_cat,
                        T2.aprobado,
                        T2.aplicado,
                        T2.creationdate,
                        T2.deleted
    FROM            padron AS T1
    INNER JOIN          vulnerables AS T2 ON T1.preclavecatant::text = T2.ncta::text
    ORDER BY    T2.efecto  -- 85,596
)
, usuarios AS (
    SELECT DISTINCT 
                        T1.admusuario_usuario,
                        T1.admusuario_id
    FROM            adm_usuario AS T1
    LEFT JOIN padrones AS T2 ON T1.admusuario_id = T2.iduser
    ORDER BY    T1.admusuario_usuario
)
, pagos AS (
    SELECT DISTINCT 
                        t1.padron_id,
                        t1.ejercicioinicial,
                        t1.periodoinicial,
                        t1.ejerciciofinal,
                        t1.periodofinal,
                        t1.montoapagar,
                        t1.fechacaptura,
                        t1.serie_recibo,
                        t1.serie_folio,
                        t1.catestatuspase_id,
                        padrones.efecto
    FROM            cip_pasecajapredial t1
    LEFT JOIN padrones ON t1.padron_id::text = padrones.padron_id::text AND  t1.ejerciciofinal::text = padrones.efecto::text
    WHERE       t1.catestatuspase_id = 2 AND serie_recibo > 0
    ORDER BY    t1.serie_recibo, t1.serie_folio
)
, fechaspagos AS (
    Select distinct padron_id, catcaja_id, fechapago::date as fechap
    FROM cip_cortepredial   AS T0 
    left join pagos AS T3 using(padron_id)
    WHERE 
    T0.serie_recibo = T3.serie_recibo AND T0.serie_folio = T3.serie_folio AND catestatuspago_id = 1 and fechapago::date >= '2021-01-01'::date
--      order by fechapago::date  desc 

)
, bancos AS (
select catcaja_id, serierecibo from cip_cajaaperturacierre where estatus is true
)
--select * from fechaspagos

SELECT DISTINCT 
        --t2.iduser,
        t2.ncta                                 AS Cuenta,
        t2.clave_mvto                   AS Movimiento,
        t2.nombre                               AS Nombre,
        t2.descr                                AS Comentarios,
        t2.efecto                           AS "Año Efecto",
        CASE t2.status      
        WHEN 1 THEN 'Activo'
        ELSE        'No Activo'
        END                                         AS Situacion,
    --  CASE t2.imported    
    --  WHEN false THEN 'NO'
    --  ELSE 'SI'
    --  END                                         AS Importado,
        t2.cve_cat                          AS Clave,
        CASE t2.aprobado
        WHEN true THEN 'SI'
        ELSE 'NO'
        END                                         AS Aprobado,
        CASE t2.aplicado
        WHEN true THEN 'SI'
        ELSE 'NO'
        END                                         AS Aplicado,
        (t2.creationdate)::date AS fecha_asignacion,
--      t2.padron_id                        AS Padron,
        t2.preubicacion                 AS Ubicacion,
        t2.prevalcat                        AS Valor_Catastral,
        T5.descripcion,
        usuarios.admusuario_usuario AS Usuario,
        t3.montoapagar                  AS Importe,
        t3.serie_recibo                 AS Recibo,
        t3.serie_folio                  AS Folio,
        
        T7.descripcion                  AS caja,

    --  (t3.fechacaptura)::date AS fecha_Captura,
        CASE t3.fechacaptura::date >=  T4.fechap
        WHEN true THEN  fechap::date  
        ELSE t3.fechacaptura::date
        END  AS Fecha_Pago
--      t3.catestatuspase_id        AS Vigencia
    
FROM padrones t2
LEFT JOIN cip_catgrupovulnerable    AS T5   ON t2.clave_mvto::text = T5.abreviacion::text
LEFT JOIN usuarios                                              ON usuarios.admusuario_id = t2.iduser
LEFT JOIN pagos                                     AS t3   ON t3.padron_id = t2.padron_id AND t2.efecto::text = t3.efecto::text
LEFT JOIN fechaspagos                       AS t4   ON t3.padron_id = t4.padron_id
--inner join bancos                                 AS T6   ON t6.serierecibo = t3.serie_folio
left join cip_catcaja                       AS T7   ON T4.catcaja_id = T7.catcaja_id
WHERE t2.deleted = false
ORDER BY t2.efecto, t3.serie_folio, t3.serie_recibo
OFFSET %s