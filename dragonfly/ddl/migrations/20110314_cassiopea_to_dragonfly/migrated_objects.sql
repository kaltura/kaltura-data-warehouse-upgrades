use kalturadw;

alter table `dwh_dim_entries`
add index `created_at` (`created_at`),
add index `updated_at` (`updated_at`),
add index `modified_at` (`modified_at`);

ALTER TABLE kalturadw.dwh_dim_kusers MODIFY screen_name VARCHAR(127) DEFAULT 'missing value';
ALTER TABLE kalturadw.dwh_dim_kusers MODIFY email VARCHAR(100) DEFAULT 'missing value';
ALTER TABLE kalturadw.dwh_dim_kusers MODIFY puser_id VARCHAR(100) DEFAULT 'missing value';
ALTER TABLE kalturadw.dwh_dim_kusers ADD first_name VARCHAR(40);
ALTER TABLE kalturadw.dwh_dim_kusers ADD last_name VARCHAR(40);

ALTER TABLE kalturadw.dwh_dim_partners MODIFY admin_name VARCHAR(50) CHARACTER SET utf8 DEFAULT 'missing value';
ALTER TABLE kalturadw.dwh_dim_partners MODIFY admin_email VARCHAR(50) CHARACTER SET utf8 DEFAULT 'missing value' ;
ALTER TABLE kalturadw.dwh_dim_partners MODIFY description VARCHAR(1024) CHARACTER SET utf8 DEFAULT 'missing value' ;
ALTER TABLE kalturadw.dwh_dim_ui_conf CONVERT TO CHARACTER SET utf8;

ALTER TABLE kalturadw.dwh_fact_events ADD COLUMN referrer_id INT(11);

DELIMITER $$

DROP PROCEDURE IF EXISTS `tmp_update_referrer_id`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `tmp_update_referrer_id`()
BEGIN
	DECLARE v_start_date_id INT;
	DECLARE v_end_date_id INT;
	DECLARE done INT DEFAULT 0;	
	DECLARE update_fact_cursor CURSOR FOR SELECT day_id start_date_id, (DATE(day_id) + INTERVAL 1 MONTH)*1 end_date_id FROM kalturadw.dwh_dim_time WHERE day_of_month = 1;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	OPEN update_fact_cursor;
	
	read_loop: LOOP
		FETCH update_fact_cursor INTO v_start_date_id, v_end_date_id;
		IF done THEN
			LEAVE read_loop;
		END IF;
		
		UPDATE kalturadw.dwh_fact_events e, kalturadw.dwh_dim_referrer r SET e.referrer_id = r.referrer_id
		WHERE IFNULL(e.referrer,'') = r.referrer
		and e.event_time >= DATE(v_start_date_id) AND event_time < DATE(v_end_date_id);
	
	END LOOP;
	CLOSE update_fact_cursor;
    END$$

DELIMITER ;

call tmp_update_referrer_id();
DROP PROCEDURE IF EXISTS `tmp_update_referrer_id`;

use kalturadw_ds;

ALTER TABLE `aggr_name_resolver` ADD COLUMN `hourly_aggr_table` VARCHAR(100) DEFAULT NULL;
UPDATE `aggr_name_resolver` SET `hourly_aggr_table` = CONCAT(SUBSTR(aggr_table,1,4),'hourly',SUBSTR(aggr_table,9));
ALTER TABLE kalturadw_ds.ds_events MODIFY COLUMN event_id int(11) NOT NULL;
ALTER TABLE kalturadw_ds.ds_events DROP PRIMARY KEY;
ALTER TABLE kalturadw_ds.ds_events ADD COLUMN referrer_id INT(11);
ALTER TABLE kalturadw_ds.files ADD COLUMN cycle_id INT(11) DEFAULT NULL;

/*Since no unique key existed ealier we prevent the adding of the unique to fail by differing between files with the same name */
UPDATE kalturadw_ds.files f INNER JOIN (SELECT file_name, process_id  FROM kalturadw_ds.files
GROUP BY file_name, process_id
HAVING COUNT(*) > 1) dup_files
ON (f.file_name = dup_files.file_name
AND f.process_id = dup_files.process_id)
SET f.file_name = CONCAT(f.file_id, "_", f.file_name);

ALTER TABLE kalturadw_ds.files ADD UNIQUE KEY file_name_process_id (file_name, process_id);
ALTER TABLE kalturadw_ds.staging_areas ADD post_transfer_aggregations VARCHAR(255);
UPDATE kalturadw_ds.staging_areas
SET post_transfer_aggregations = CASE 	WHEN (id = 1) THEN ('(\'country\',\'domain\',\'entry\',\'partner\',\'plays_views\',\'uid\',\'widget\',\'domain_referrer\')')
										WHEN (id IN (2,4,5,6,7)) THEN ('(''partner_usage'')') 
										ELSE (post_transfer_aggregations)
								 END,
	aggr_date_field = CASE 	WHEN (id in (1,2)) THEN ('event_date_id')
							WHEN (id IN (4,5,6,7)) THEN ('activity_date_id') 
							ELSE (aggr_date_field)
	END;

DROP FUNCTION IF EXISTS `kalturadw`.`calc_partner_storage_data_last_month`;

INSERT INTO kalturadw_ds.aggr_name_resolver(aggr_name, aggr_table, aggr_id_field, hourly_aggr_table) VALUES('domain_referrer', '', 'domain_id, referrer_id', 'dwh_hourly_events_domain_referrer');
UPDATE kalturadw_ds.aggr_name_resolver SET aggr_table = hourly_aggr_table;
alter table kalturadw_ds.aggr_name_resolver drop column hourly_aggr_table;

insert into kalturadw.aggr_managment (aggr_name, aggr_day, aggr_day_int, is_calculated) select distinct 'domain_referrer', date(aggr_day_int), aggr_day_int, 0 from kalturadw.aggr_managment;
insert into kalturadw.aggr_managment (aggr_name, aggr_day, aggr_day_int, is_calculated) select distinct 'partner_usage', date(aggr_day_int), aggr_day_int, if(aggr_day_int > date(now())*1, 0, 1) from kalturadw.aggr_managment;
INSERT INTO kalturadw.aggr_managment (aggr_name, aggr_day, aggr_day_int, is_calculated) SELECT DISTINCT 'storage_usage', DATE(aggr_day_int), aggr_day_int, IF(aggr_day_int > DATE(NOW())*1, 0, 1) FROM kalturadw.aggr_managment;
INSERT INTO kalturadw.aggr_managment (aggr_name, aggr_day, aggr_day_int, is_calculated) SELECT DISTINCT 'storage_usage_kuser_sync', DATE(aggr_day_int), aggr_day_int, IF(aggr_day_int > DATE(NOW())*1, 0, 1) FROM kalturadw.aggr_managment;

use kalturadw;

DROP TABLE dwh_aggr_events_country;
DROP TABLE dwh_aggr_events_domain;
DROP TABLE dwh_aggr_events_entry;
DROP TABLE dwh_aggr_events_uid;
DROP TABLE dwh_aggr_events_widget;
DROP TABLE dwh_aggr_partner;
DROP table dwh_aggr_partner_daily_usage;
DROP table dwh_aggr_monthly_partner;

DROP PROCEDURE IF EXISTS `daily_procedure_dwh_aggr_partner`;
DROP PROCEDURE IF EXISTS `daily_procedure_dwh_aggr_events_widget`;
DROP PROCEDURE IF EXISTS `daily_procedure_dwh_aggr_partner_daily_usage`;
DROP PROCEDURE IF EXISTS `daily_procedure_dwh_aggr_partner_daily_usage_loop`;

INSERT INTO kalturadw.dwh_fact_bandwidth_usage (file_id, partner_id, activity_date_id, activity_hour_id, bandwidth_source_id, bandwidth_bytes)
SELECT -1, IFNULL(partner_id, -1), activity_date_id, activity_hour_id, partner_sub_activity_id, SUM(amount)*1024 FROM kalturadw.dwh_fact_partner_activities
WHERE partner_activity_id  = 1 
AND partner_sub_activity_id IN (1,2,3,4)
GROUP BY partner_id, activity_date, activity_date_id, activity_hour_id, partner_sub_activity_id;

DELETE FROM kalturadw.ri_mapping WHERE table_name = 'dwh_aggr_partner_daily_usage';