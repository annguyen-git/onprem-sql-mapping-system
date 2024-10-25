 CREATE OR REPLACE EDITIONABLE PROCEDURE "ETL"."MAPPING_PROC_STEP3" ( DATE_ID VARCHAR2) AS
DECLARE
    v_select        VARCHAR2(4000);
    v_from          VARCHAR2(4000);
    v_sql           VARCHAR2(4000);
    v_where         VARCHAR2(4000);
    v_insert_table  VARCHAR2(4000);
    v_select_t1     VARCHAR2(4000);
BEGIN 
    v_date := DATE_ID;
	FOR i in (
		SELECT a.mapping_id, a.partner_id, a.is_foled, a.fold_num
		FROM metadata a
		) LOOP 

----------No fold----------
		IF i.is_foled = 0 OR i.is_foled IS NULL OR  i.fold_num = 0 OR i.fold_num IS NULL THEN dbms_output.put_line(i.mapping_id || 'Concluded');
			v_select := 'SELECT mapping_module, mapping_id, partner_id, mapping_id, mapping_key, '''|| i.fold_num ||''' fold_num, src2_id, src1_id, src2_id, mapping_result_in_day, ''MATCHED'' mapping_result, mapping_date, first_mapping_date';
			v_from := '
			FROM    mapping_temp_tb
			WHERE   mapping_date = TO_DATE('|| v_prd_id||', ''yyyymmdd'')
					AND mapping_id = '''|| i.mapping_id ||'''
			';
			v_insert_table := v_select || v_from
			;
			v_sql := 'INSERT INTO DATA.RC_LOYALTY_RSLT (mapping_module, mapping_id, partner_id, mapping_id, mapping_key, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_result, mapping_date, first_mapping_date) 
			';
			v_insert_table := v_sql||v_insert_table
			;
			INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
			COMMIT;
			EXECUTE IMMEDIATE v_insert_table ;
			COMMIT;
			dbms_output.put_line(v_insert_table);

----------------------------------------Folded mapping----------------------------------------
		ELSIF I.reconcile_type = 1 THEN  
			dbms_output.put_line( '--------------------Matched data within today--------------------
			');
			v_select := 'SELECT mapping_module, mapping_id, partner_id, mapping_id, mapping_key, '''|| i.fold_num ||''' fold_num, src1_id, src2_id, mapping_result_in_day, ''MATCHED'' mapping_result, mapping_date, first_mapping_date';
			v_from := '
			FROM    mapping_temp_tb
			WHERE   mapping_date = TO_DATE('|| v_prd_id||', ''yyyymmdd'')
					AND mapping_id = '''|| i.mapping_id ||'''
			';
			v_insert_table := v_select || v_from
			;
			v_sql := 'INSERT INTO DATA.RC_LOYALTY_RSLT (mapping_module, mapping_id, partner_id, mapping_id, mapping_key, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_result, mapping_date, first_mapping_date) 
			';
			v_insert_table := v_sql||v_insert_table
			;
			INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
			COMMIT;
			EXECUTE IMMEDIATE v_insert_table ;
			COMMIT;
			dbms_output.put_line(v_insert_table);

			dbms_output.put_line( '--------------------Matched data of today and yesterday--------------------
			');
			FOR loop_var IN 1..2 LOOP
				v_select_t1 :=
				'SELECT tmp.mapping_module, tmp.mapping_id, tmp.partner_id, tmp.mapping_id, tmp.mapping_key, '''|| i.fold_num ||''' fold_num, tmp.src1_id, tmp.src2_id, tmp.mapping_result_in_day, ''MATCHED'' mapping_result, tmp.mapping_date, t1.first_mapping_date,
						CASE WHEN t1.fold_num >= 0 THEN t1.fold_num + 1 ELSE 1 END fold_num
				FROM    mapping_temp_tb tmp
				JOIN    mapping_result_unmatch  t1
				ON      tmp.mapping_key = t1.mapping_key
				AND     tmp.mapping_id = t1.mapping_id
				WHERE   t1.mapping_date = TO_DATE('|| v_prd_id||', ''yyyymmdd'') - 1
						AND tmp.mapping_date = TO_DATE('|| v_prd_id||', ''yyyymmdd'')
						AND tmp.mapping_id = ''' || i.mapping_id ||'''';
				IF loop_var = 1 THEN
					v_select_t1 := v_select_t1 || '
						AND tmp.mapping_result = ''UNMATCH SRC1''
						AND t1.mapping_result = ''UNMATCH SRC2''';
				ELSE
					v_select_t1 := v_select_t1 || '
						AND tmp.mapping_result = ''UNMATCH SRC2''
						AND t1.mapping_result = ''UNMATCH SRC1''';
				END IF;
				v_sql := 'INSERT INTO mapping_result (mapping_module, mapping_id, partner_id, mapping_id, mapping_key, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_result, mapping_date, first_mapping_date, fold_num)
				'; 
				v_insert_table := v_sql || CHR(10) || v_select_t1;
				dbms_output.put_line(v_insert_table);
				INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
				COMMIT;
				EXECUTE IMMEDIATE v_insert_table ;
				COMMIT;
			END LOOP;
    
			dbms_output.put_line( '--------------------Unmatched data today vs unmacthed data yesterday --> unmatched data today--------------------
			');
			FOR loop_var IN 1..2 LOOP
				v_select_t1 :=  
				'select tmp.mapping_module, tmp.mapping_id, tmp.partner_id, tmp.mapping_id, tmp.mapping_key, '''|| i.fold_num ||''' fold_num, tmp.src1_id, tmp.src2_id, tmp.mapping_result_in_day, tmp.mapping_date, t1.first_mapping_date,
				0 RC_NUMBER, ';
				v_from :=
				'FROM   mapping_temp_tb tmp
				LEFT JOIN mapping_result_unmatch partition for(TO_DATE('''||  v_prd_id || ''',''yyyyMMdd'') -1) t1
				ON      tmp.mapping_key_02 = t1.mapping_key_02
				AND     tmp.mapping_id = t1.mapping_id
				AND     t1.mapping_date = TO_DATE('|| v_prd_id||',''yyyymmdd'') - 1 
				AND     tmp.mapping_date = TO_DATE('|| v_prd_id||',''yyyymmdd'')';
				v_where :=
				'WHERE  t1.mapping_key is null
						AND tmp.mapping_id = '''|| i.mapping_id||'''
						AND tmp.mapping_date = TO_DATE('|| v_prd_id||',''yyyymmdd'')';
				IF loop_var = 1 THEN
					v_select_t1 := v_select_t1 ||'            
						''UNMATCHED SRC1'' mapping_result,
					';
					v_from := v_from ||'   
						AND t1.mapping_result = ''UNMATCHED SRC2''
					';
					v_where := v_where ||'
						AND tmp.mapping_result = ''UNMATCHED SRC1''
					';
				ELSE
					v_select_t1 := v_select_t1 ||'            
						''UNMATCHED SRC2'' mapping_result_02,
					';
					v_from := v_from ||'   
						AND t1.mapping_result = ''UNMATCHED SRC1''
					';
					v_where := v_where ||'
						AND tmp.mapping_result = ''UNMATCHED SRC2''
					';
				END IF;
				v_sql := ' insert into mapping_result mapping_module, mapping_id, partner_id, mapping_id, mapping_key, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_date, first_mapping_date, fold_num, mapping_result';
				v_insert_table := v_sql|| CHR(10) || v_select_t1 || CHR(10) || v_from || v_where;
				dbms_output.put_line(v_insert_table);
				INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
				COMMIT;
				EXECUTE IMMEDIATE v_insert_table ;
				COMMIT;
				END LOOP;
				v_sql := ' insert into mapping_result mapping_module, mapping_id, partner_id, mapping_id, mapping_key, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_date, first_mapping_date, fold_num, mapping_result, max_fold_num ';
				v_insert_table := v_sql|| CHR(10) || v_select_t1 ||' tmp.max_fold_num, ' CHR(10) || v_from || v_where;
				dbms_output.put_line(v_insert_table);
				INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
				COMMIT;
				EXECUTE IMMEDIATE v_insert_table ;
				COMMIT;
			END LOOP;
    
			dbms_output.put_line( '--------------------Unmatched data today vs unmacthed data yesterday --> unmatched data yesterday--------------------
			');    
			FOR loop_var IN 1..2 LOOP
				v_select_t1 :=  '
				SELECT tmp.mapping_module, tmp.mapping_id, tmp.partner_id, tmp.mapping_id, tmp.mapping_key, '''|| i.fold_num ||''' fold_num, tmp.src1_id, tmp.src2_id, tmp.mapping_result_in_day, tmp.mapping_date, t1.first_mapping_date,
						NULL  mapping_result_01, t1.MAPPING_FIRST_DATE, t1.rc_number +1 RC_NUMBER, ';
				v_from := 
				'FROM    mapping_temp_tb tmp
				RIGHT JOIN mapping_result_unmatch partition for(TO_DATE('''|| v_prd_id || ''',''yyyyMMdd'') -1) t1
				ON      tmp.mapping_key = t1.mapping_key
				AND     tmp.mapping_id = t1.mapping_id
				AND     tmp.is_folded = 1
				AND     tmp.mapping_date = TO_DATE('|| v_prd_id||',''yyyymmdd'')
				AND     t1.mapping_date = TO_DATE('|| v_prd_id||',''yyyymmdd'') - 1';
				v_where :=
				'WHERE  tmp.mapping_key_02 is null
						AND t1.mapping_id = '''|| I.mapping_id||'''
						AND t1.mapping_date = TO_DATE('|| v_prd_id||',''yyyymmdd'')-1';
				IF loop_var = 1 THEN
					v_select_t1 := v_select_t1 ||'            
						CASE WHEN t1.max_fold_num = t1.fold_num +1 then ''CONFLICT SRC1'' ELSE ''UNMATCHED SRC1'' END mapping_result,
				';
					v_from := v_from ||'   
						AND  tmp.mapping_result = ''UNMATCHED SRC2''
				';
					v_where := v_where ||'
						AND t1.mapping_result = ''UNMATCHED SRC1''
				';
				ELSE
					v_select_t1 := v_select_t1 ||'            
						CASE WHEN t1.max_fold_num = t1.fold_num +1 then ''CONFLICT SRC2'' ELSE ''UNMATCHED SRC2'' END mapping_result,
				';
					v_from := v_from ||'   
						AND  tmp.mapping_result = ''UNMATCHED SRC1''
				';
					v_where := v_where ||'
						AND t1.mapping_result = ''UNMATCHED SRC2''
				';
				END IF;
				v_sql := ' insert into mapping_result mapping_module, mapping_id, partner_id, mapping_id, mapping_key, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_date, first_mapping_date, fold_num, mapping_result';
				v_insert_table := v_sql|| CHR(10) || v_select_t1 || CHR(10) || v_from || v_where;
				dbms_output.put_line(v_insert_table);
				INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
				COMMIT;
				EXECUTE IMMEDIATE v_insert_table ;
				COMMIT;
				END LOOP;
				v_sql := ' insert into mapping_result mapping_module, mapping_id, partner_id, mapping_id, mapping_key, fold_num, src1_id, src2_id, mapping_result_in_day, mapping_date, first_mapping_date, fold_num, mapping_result, max_fold_num ';
				v_insert_table := v_sql|| CHR(10) || v_select_t1 ||' tmp.max_fold_num, ' CHR(10) || v_from || v_where;
				dbms_output.put_line(v_insert_table);
				INSERT INTO log_table VALUES ( i.mapping_id, i.partner_id, i.src_name,  i.fold_num, v_insert_table, SYSDATE );
				COMMIT;
				EXECUTE IMMEDIATE v_insert_table ;
				COMMIT;
			END LOOP;
		END IF;
	END LOOP;
END;