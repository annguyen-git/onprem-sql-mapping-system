CREATE OR REPLACE EDITIONABLE PROCEDURE "ETL"."MAPPING_PROC_STEP2" (DATE_ID VARCHAR2) AS
DECLARE 
    v_sql            VARCHAR2(4000);
    v_insert_table   VARCHAR2(4000);
    v_from           VARCHAR2(4000);
    v_select         VARCHAR2(4000);
    v_where          VARCHAR2(4000);
    v_date           VARCHAR2(20);
BEGIN 
    v_date := DATE_ID;
	FOR i in (
		SELECT a.mapping_id, a.partner_id
		FROM metadata a
		) LOOP
    
--------------------Matched data--------------------
		v_sql := 'select a.mapping_module, a.mapping_id, a.partner_id, a.mapping_id, a.mapping_key, a.is_fold, a.fold_num, a.src1_id, b.src2_id, ''MATCHED'' mapping_result_in_day,  mapping_date'
		v_from := ' FROM  src1_table  a
					JOIN  src2_table  b  
					ON a.mapping_date = b.mapping_date
					AND a.mapping_key = b.mapping_key
					AND a.mapping_id = b.mapping_id';
		v_where := '    WHERE 1=1 
					AND a.mapping_id = ''' || i.mapping_id || ''' 
					AND a.mapping_id = to_date('''|| v_date || ''',''yyyyMMdd'')';
		v_select := v_sql || CHR(10) || v_from || CHR(10) || v_where;

		v_sql := 'insert into mapping_temp_tb (mapping_module, mapping_id, partner_id, mapping_id, mapping_key, is_fold, fold_num, src1_id, src2_id, mapping_result_in_day)'
		v_insert_table := v_sql || CHR(10) || v_select;
		DBMS_OUTPUT.PUT_LINE(v_insert_table);
		INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
		COMMIT;
		EXECUTE IMMEDIATE v_insert_table;
		COMMIT;

--------------------Unmatched data--------------------
		FOR loop_var IN 1..2 LOOP
			v_sql := 'select a.mapping_module, a.mapping_id, a.partner_id, a.mapping_id, a.mapping_key, a.is_fold, a.fold_num, a.src1_id, b.src2_id, mapping_date'; 
			v_where := 'WHERE 1=1
						AND a.mapping_id = ''' || i.mapping_id || ''' 
						AND a.mapping_date = TO_DATE(''' || v_date || ''',''yyyymmdd'') 
						AND NOT EXISTS(
						SELECT 1 FROM mapping_temp_tb tmp
						WHERE 1=1
						AND tmp.mapping_date= a.mapping_date
						AND tmp.mapping_key = a.mapping_key)';
			IF loop_var = 1 THEN
				v_sql := v_sql || '''UNMATCH SRC1'' mapping_result_in_day';
				v_from := ' from src1_table a';
			ELSE
				v_sql := v_sql || '''UNMATCH SRC2'' mapping_result_in_day';
				v_from := ' from src2_table a';
			END IF;
			v_select := v_sql || CHR(10) || v_from || CHR(10) || v_where;
			v_sql := 'insert into mapping_temp_tb (mapping_module, mapping_id, partner_id, mapping_id, mapping_key, is_fold, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_date)'
			v_insert_table := v_sql || CHR(10) || v_select;
			dbms_output.put_line(v_insert_table);
			INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
			COMMIT;
			EXECUTE IMMEDIATE v_insert_table;
			COMMIT;
		END LOOP;      
	END LOOP;
END;