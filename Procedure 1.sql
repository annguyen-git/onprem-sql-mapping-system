CREATE OR REPLACE EDITIONABLE PROCEDURE "ETL"."MAPPING_PROC_STEP1" (DATE_ID varchar2) AS
DECLARE
    v_sql            VARCHAR2(4000);
    v_insert_table   VARCHAR2(4000);
    v_from           VARCHAR2(4000);
    v_key            VARCHAR2(4000);
    v_data_field     VARCHAR2(4000);
    v_select         VARCHAR2(4000);
    v_where          VARCHAR2(4000);
    v_date           VARCHAR2(100);
BEGIN
    v_date := DATE_ID;
    COMMIT;
    FOR i IN (
        SELECT DISTINCT 
               a.mapping_id, a.partner_id, a.src_name,
               b.p1_key, b.p2_key, b.p3_key
        FROM metadata a
        JOIN fold_data b ON a.mapping_id = b.mapping_id
        WHERE 1=1
        GROUP BY a.mapping_id, a.partner_id, a.src_name
        ORDER BY a.mapping_id, a.partner_id, a.src_name
    ) LOOP
		FOR l in 1..2 LOOP
			SELECT i.p1_key || i.p2_key || i.p3_key
			INTO v_key
			FROM metadata a
			WHERE 1 = 1
				AND a.mapping_id = i.mapping_id
				AND a.partner_id = i.partner_id
				AND a.src_name = i.src_name;
                
			SELECT  
				'select ''DEMO_MD'' mapping_module, '''|| i.mapping_id || ''' mapping_id, ''' || i.partner_id ||''' partner_id,'
				|| 'to_date(''' || v_date || ''',''yyyyMMdd'')' || ' mapping_date,''' || v_key || ''' mapping_key'
			INTO v_sql
			FROM metadata a
			WHERE 1 = 1
				AND a.partner_id = i.partner_id
				AND a.mapping_id = i.mapping_id
				AND a.src_name = i.src_name;
				
			IF l = 1 THEN
				v_sql := v_sql || ' id src1_id ';
				v_from := ' FROM  stg.src1 PARTITION FOR(TO_DATE('''|| v_date || ''',''yyyymmdd''))' || i.src_name;
				v_where := ' WHERE 1=1 and ' || i.src_name || '.mapping_date = to_date(''' || v_date || ''',''yyyyMMdd'')';
				v_select := v_sql || CHR(10) || v_from || CHR(10) || v_where;
				v_sql := 'insert into src1_table ( mapping_module, mapping_id, partner_id, mapping_date, mapping_key, src1_id )'
				
			ELSE
				v_sql := v_sql || ' id src2_id '
                v_from := ' FROM  stg.src2 PARTITION FOR(TO_DATE('''|| v_date || ''',''yyyymmdd''))' || i.src_name;
				v_where := ' WHERE 1=1 and ' || i.src_name || '.mapping_date = to_date(''' || v_date || ''',''yyyyMMdd'')';
				v_select := v_sql || CHR(10) || v_from || CHR(10) || v_where;
				v_sql := 'insert into src2_table ( mapping_module, mapping_id, partner_id, mapping_date, mapping_key, src2_id )'
			END IF;
            
				v_insert_table := v_sql || CHR(10) || v_select;
				dbms_output.put_line(v_insert_table);
				INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
				COMMIT;
				EXECUTE IMMEDIATE v_insert_table;
				COMMIT;
			COMMIT;
		END LOOP
    END LOOP;
END;