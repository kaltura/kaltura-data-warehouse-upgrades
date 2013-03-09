USE kalturalog;

ALTER TABLE etl_log
CONVERT TO CHARACTER SET utf8;

USE kalturadw_ds;

UPDATE aggr_name_resolver
SET aggr_join_stmt = 'USE INDEX (event_hour_id_event_date_id_partner_id) inner join kalturadw.dwh_dim_entries as entry on(ev.entry_id = entry.entry_id)'
WHERE aggr_name = 'uid';

INSERT INTO aggr_name_resolver (aggr_name, aggr_table, aggr_id_field, aggr_join_stmt)
VALUES ('devices', 'dwh_hourly_events_devices', 'country_id,location_id,os_id,browser_id,ui_conf_id, entry_id','');

ALTER TABLE cycles
ADD COLUMN `assigned_server_id` int(11) DEFAULT NULL;

DROP TABLE IF EXISTS `ds_bandwidth_usage`;
CREATE TABLE `ds_bandwidth_usage` (
  `line_number` int(10) DEFAULT NULL,
  `cycle_id` int(11) NOT NULL,
  `file_id` int(11) NOT NULL,
  `partner_id` int(11) NOT NULL DEFAULT '-1',
  `activity_date_id` int(11) DEFAULT '-1',
  `activity_hour_id` tinyint(4) DEFAULT '-1',
  `bandwidth_source_id` bigint(20) DEFAULT NULL,
  `url` varchar(2000) DEFAULT NULL,
  `bandwidth_bytes` bigint(20) DEFAULT '0',
  `user_ip` varchar(15) DEFAULT NULL,
  `user_ip_number` int(10) unsigned DEFAULT NULL,
  `country_id` int(11) DEFAULT NULL,
  `location_id` int(11) DEFAULT NULL,
  `os_id` int(11) DEFAULT NULL,
  `browser_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
PARTITION BY LIST (cycle_id)
(PARTITION p_0 VALUES IN (0) ENGINE = InnoDB);

DROP TABLE IF EXISTS `ds_events`;
CREATE TABLE `ds_events` (
  `cycle_id` int(11) NOT NULL,
  `file_id` int(11) NOT NULL,
  `event_id` int(11) NOT NULL,
  `event_type_id` smallint(6) NOT NULL,
  `client_version` varchar(31) DEFAULT NULL,
  `event_time` datetime DEFAULT NULL,
  `event_date_id` int(11) DEFAULT NULL,
  `event_hour_id` tinyint(4) DEFAULT NULL,
  `session_id` varchar(50) DEFAULT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `entry_id` varchar(20) DEFAULT NULL,
  `unique_viewer` varchar(40) DEFAULT NULL,
  `widget_id` varchar(31) DEFAULT NULL,
  `ui_conf_id` int(11) DEFAULT NULL,
  `uid` varchar(64) DEFAULT NULL,
  `current_point` int(11) DEFAULT NULL,
  `duration` int(11) DEFAULT NULL,
  `user_ip` varchar(15) DEFAULT NULL,
  `user_ip_number` int(10) unsigned DEFAULT NULL,
  `country_id` int(11) DEFAULT NULL,
  `location_id` int(11) DEFAULT NULL,
  `process_duration` int(11) DEFAULT NULL,
  `control_id` varchar(15) DEFAULT NULL,
  `seek` int(11) DEFAULT NULL,
  `new_point` int(11) DEFAULT NULL,
  `domain_id` int(11) DEFAULT NULL,
  `entry_media_type_id` int(11) DEFAULT NULL,
  `entry_partner_id` int(11) DEFAULT NULL,
  `referrer_id` int(11) DEFAULT NULL,
  `os_id` int(11) DEFAULT NULL,
  `browser_id` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
PARTITION BY LIST (cycle_id)
(PARTITION p_0 VALUES IN (0) ENGINE = InnoDB);

CREATE TABLE `ds_fms_session_events` (
  `line_number` int(10) DEFAULT NULL,
  `cycle_id` int(11) NOT NULL,
  `file_id` int(11) unsigned NOT NULL,
  `event_type_id` tinyint(3) unsigned NOT NULL,
  `event_category_id` tinyint(3) unsigned NOT NULL,
  `event_time` datetime NOT NULL,
  `event_time_tz` varchar(3) NOT NULL,
  `event_date_id` int(11) NOT NULL,
  `event_hour_id` tinyint(3) NOT NULL,
  `context` varchar(1024) DEFAULT NULL,
  `entry_id` varchar(20) DEFAULT NULL,
  `partner_id` int(10) DEFAULT NULL,
  `external_id` varchar(50) DEFAULT NULL,
  `server_ip` varchar(15) DEFAULT NULL,
  `server_ip_number` int(10) unsigned DEFAULT NULL,
  `server_process_id` int(10) unsigned NOT NULL,
  `server_cpu_load` smallint(5) unsigned NOT NULL,
  `server_memory_load` smallint(5) unsigned NOT NULL,
  `adaptor_id` smallint(5) unsigned NOT NULL,
  `virtual_host_id` smallint(5) unsigned NOT NULL,
  `fms_app_id` tinyint(3) unsigned NOT NULL,
  `app_instance_id` tinyint(3) unsigned NOT NULL,
  `duration_secs` int(10) unsigned NOT NULL,
  `status_id` smallint(3) unsigned DEFAULT NULL,
  `status_desc_id` tinyint(3) unsigned NOT NULL,
  `client_ip` varchar(15) NOT NULL,
  `client_ip_number` int(10) unsigned NOT NULL,
  `client_country_id` int(10) unsigned DEFAULT '0',
  `client_location_id` int(10) unsigned DEFAULT '0',
  `client_protocol_id` tinyint(3) unsigned NOT NULL,
  `uri` varchar(4000) NOT NULL,
  `uri_stem` varchar(2000) DEFAULT NULL,
  `uri_query` varchar(2000) DEFAULT NULL,
  `referrer` varchar(4000) DEFAULT NULL,
  `user_agent` varchar(2000) DEFAULT NULL,
  `session_id` varchar(20) NOT NULL,
  `client_to_server_bytes` bigint(20) unsigned NOT NULL,
  `server_to_client_bytes` bigint(20) unsigned NOT NULL,
  `stream_name` varchar(1024) DEFAULT NULL,
  `stream_query` varchar(1024) DEFAULT NULL,
  `stream_file_name` varchar(4000) DEFAULT NULL,
  `stream_type_id` tinyint(3) unsigned DEFAULT NULL,
  `stream_size_bytes` int(11) DEFAULT NULL,
  `stream_length_secs` int(11) DEFAULT NULL,
  `stream_position` int(11) DEFAULT NULL,
  `client_to_server_stream_bytes` int(10) unsigned DEFAULT NULL,
  `server_to_client_stream_bytes` int(10) unsigned DEFAULT NULL,
  `server_to_client_qos_bytes` int(10) unsigned DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8
PARTITION BY LIST (cycle_id)
(PARTITION p_0 VALUES IN (0) ENGINE = InnoDB);

CREATE TABLE `etl_servers` (
  `etl_server_id` int(11) NOT NULL AUTO_INCREMENT,
  `etl_server_name` varchar(64) NOT NULL,
  PRIMARY KEY (`etl_server_id`),
  UNIQUE KEY `etl_server_name` (`etl_server_name`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

ALTER TABLE files
CHANGE `file_size` `file_size_kb` int(20) DEFAULT NULL,
ADD COLUMN `compression_suffix` varchar(10) NOT NULL DEFAULT '',
ADD COLUMN `subdir` varchar(1024) NOT NULL DEFAULT '',
DROP KEY file_name_process_id,
ADD UNIQUE KEY `file_name_process_id_compression_suffix` (`file_name`,`process_id`,`compression_suffix`),
ADD KEY `cycle_id` (`cycle_id`);

ALTER TABLE `fms_incomplete_sessions`
ALTER `session_id` SET DEFAULT '',
ADD COLUMN `session_client_ip` varchar(15) DEFAULT NULL AFTER `session_date_id`,
ADD COLUMN `session_client_ip_number` int(10) unsigned DEFAULT NULL AFTER `session_client_ip`,
ADD COLUMN `session_client_country_id` int(11) DEFAULT NULL AFTER `session_client_ip_number`,
ADD COLUMN `session_client_location_id` int(11) DEFAULT NULL AFTER `session_client_country_id`,
ADD COLUMN `bandwidth_source_id` int(11) NOT NULL DEFAULT '5',
ADD COLUMN `is_connected_ind` int(11) DEFAULT NULL,
ADD COLUMN `is_disconnected_ind` int(11) DEFAULT NULL,
ADD PRIMARY KEY (`session_id`);

DROP TABLE IF EXISTS `fms_stale_sessions`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8 */;
CREATE TABLE `fms_stale_sessions` (
  `session_id` varchar(20) DEFAULT NULL,
  `session_time` datetime DEFAULT NULL,
  `last_update_time` datetime DEFAULT NULL,
  `purge_time` datetime DEFAULT NULL,
  `bandwidth_source_id` int(11) NOT NULL DEFAULT '5',
  `session_date_id` int(11) unsigned DEFAULT NULL,
  `session_client_ip` varchar(15) DEFAULT NULL,
  `session_client_ip_number` int(10) unsigned DEFAULT NULL,
  `session_client_country_id` int(11) DEFAULT NULL,
  `session_client_location_id` int(11) DEFAULT NULL,
  `con_cs_bytes` bigint(20) unsigned DEFAULT NULL,
  `con_sc_bytes` bigint(20) unsigned DEFAULT NULL,
  `dis_cs_bytes` bigint(20) unsigned DEFAULT NULL,
  `dis_sc_bytes` bigint(20) unsigned DEFAULT NULL,
  `partner_id` int(10) unsigned DEFAULT NULL,
  `is_connected_ind` int(11) DEFAULT NULL,
  `is_disconnected_ind` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

ALTER TABLE invalid_ds_lines
ENGINE = InnoDB;

ALTER TABLE invalid_ds_lines_error_codes
ENGINE = InnoDB;

ALTER TABLE invalid_event_lines
ENGINE = InnoDB;

ALTER TABLE invalid_fms_event_lines
ENGINE = InnoDB;

ALTER TABLE locks
CHANGE `lock_id` `lock_id` int(11) NOT NULL AUTO_INCREMENT;

DROP TABLE IF EXISTS `ods_fms_session_events`;

ALTER TABLE `parameters`
ADD COLUMN `date_value` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;

DELETE FROM `parameters`;
INSERT INTO `parameters` VALUES (2,0,'dim_sync_last_update',-1,'2011-09-17 20:04:30'),(3,2,'fms_stale_session_days_limit',3,'2011-09-18 20:04:30'),(8,0,'referencial_integrity_last_update',0,'2010-12-31 22:00:00');

INSERT INTO pentaho_sequences VALUES (9,'dimensions/update_batch_job.ktr',1,1);

ALTER TABLE processes
ADD COLUMN `max_files_per_cycle` int(11) DEFAULT NULL;

DELETE FROM processes;
INSERT INTO `processes` VALUES (1,'events',20),(2,'fms_live_streaming',50),(7,'fms_ondemand_streaming',50),(4,'bandwidth_usage_AKAMAI',50),(5,'bandwidth_usage_LLN',50),(6,'bandwidth_usage_LEVEL3',1000);

CREATE TABLE `retention_policy` (
  `table_name` varchar(256) NOT NULL,
  `archive_start_days_back` int(11) DEFAULT '180',
  `archive_delete_days_back` int(11) DEFAULT '365',
  `archive_last_partition` date DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `retention_policy` VALUES ('dwh_fact_events',180,365,'2011-01-01'),('dwh_fact_bandwidth_usage',180,365,'2011-01-01'),('dwh_fact_fms_session_events',180,365,'2011-01-01'),('dwh_fact_fms_sessions',180,365,'2011-01-01');

DROP TABLE IF EXISTS `staging_areas`;

CREATE TABLE `staging_areas` (
  `id` int(10) unsigned NOT NULL,
  `process_id` int(10) unsigned NOT NULL,
  `source_table` varchar(45) NOT NULL,
  `target_table` varchar(45) NOT NULL,
  `on_duplicate_clause` varchar(4000) DEFAULT NULL,
  `staging_partition_field` varchar(45) DEFAULT NULL,
  `post_transfer_sp` varchar(500) DEFAULT NULL,
  `aggr_date_field` varchar(45),
  `hour_id_field` VARCHAR(45),
  `post_transfer_aggregations` VARCHAR(255),
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

INSERT INTO `staging_areas` VALUES (1,1,'ds_events','kalturadw.dwh_fact_events','ON DUPLICATE KEY UPDATE kalturadw.dwh_fact_events.file_id = kalturadw.dwh_fact_events.file_id','cycle_id',NULL,'event_date_id','event_hour_id','(\'country\',\'domain\',\'entry\',\'partner\',\'plays_views\',\'uid\',\'widget\',\'domain_referrer\',\'devices\')'),
(2,2,'ds_fms_session_events','kalturadw.dwh_fact_fms_session_events',NULL,'cycle_id','fms_sessionize','event_date_id','event_hour_id','(\'bandwidth_usage\',\'devices\'))'),
(8,7,'ds_fms_session_events','kalturadw.dwh_fact_fms_session_events',NULL,'cycle_id','fms_sessionize','event_date_id','event_hour_id','(\'bandwidth_usage\',\'devices\')'),
(4,4,'ds_bandwidth_usage','kalturadw.dwh_fact_bandwidth_usage',NULL,'cycle_id',NULL,'activity_date_id','activity_hour_id','(\'bandwidth_usage\',\'devices\')'),
(5,5,'ds_bandwidth_usage','kalturadw.dwh_fact_bandwidth_usage',NULL,'cycle_id',NULL,'activity_date_id','activity_hour_id','(\'bandwidth_usage\',\'devices\')'),
(6,6,'ds_bandwidth_usage','kalturadw.dwh_fact_bandwidth_usage',NULL,'cycle_id',NULL,'activity_date_id','activity_hour_id','(\'bandwidth_usage\',\'devices\')'),
(7,1,'ds_bandwidth_usage','kalturadw.dwh_fact_bandwidth_usage',NULL,'cycle_id',NULL,'activity_date_id','activity_hour_id','(\'bandwidth_usage\',\'devices\')');

CREATE TABLE `updated_kusers_storage_usage` (
  `kuser_id` int(11) NOT NULL,
  `storage_kb` int(11) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

CREATE TABLE `version_management` (
  `version` int(11) DEFAULT NULL,
  `filename` varchar(250) DEFAULT NULL,
  `created_at` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `version_management` VALUES (6086,'version_table.sql','2011-09-18 20:04:31');

--
-- Dumping routines for database 'kalturadw_ds'
--
/*!50003 DROP FUNCTION IF EXISTS `get_error_code` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 FUNCTION `get_error_code`(error_code_reason_param varchar(255)) RETURNS smallint(6)
    NO SQL
BEGIN
	DECLARE error_code smallint(6);
	INSERT IGNORE kalturadw_ds.invalid_ds_lines_error_codes (error_code_reason) VALUES(error_code_reason_param);
	SELECT error_code_id 
		INTO error_code
		FROM kalturadw_ds.invalid_ds_lines_error_codes
		WHERE error_code_reason = error_code_reason_param;
	return error_code;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `get_ip_country_location` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 FUNCTION `get_ip_country_location`(ip BIGINT) RETURNS varchar(30) CHARSET utf8
    READS SQL DATA
    DETERMINISTIC
BEGIN
	DECLARE res VARCHAR(30);
	SELECT CONCAT(country_id,",",location_id)
	INTO res
	FROM kalturadw.dwh_dim_ip_ranges
	WHERE ip_from = (
	SELECT MAX(ip_from) 
	FROM kalturadw.dwh_dim_ip_ranges
	WHERE ip >= ip_from
	) ;
	RETURN res;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `add_cycle_partition` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `add_cycle_partition`(p_cycle_id VARCHAR(10))
BEGIN
	DECLARE table_name VARCHAR(32);
	DECLARE done INT DEFAULT 0;
	DECLARE staging_areas_cursor CURSOR FOR SELECT source_table 
						FROM kalturadw_ds.staging_areas, kalturadw_ds.cycles 
						WHERE staging_areas.process_id = cycles.process_id AND cycles.cycle_id = p_cycle_id;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	OPEN staging_areas_cursor;	
	read_loop: LOOP
		FETCH staging_areas_cursor INTO table_name;
		IF done THEN
			LEAVE read_loop;
		END IF;
		SET @s = CONCAT('alter table kalturadw_ds.',table_name,' ADD PARTITION (partition p_' ,	p_cycle_id ,' values in (', p_cycle_id ,'))');
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	END LOOP;
	CLOSE staging_areas_cursor;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `create_updated_entries` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `create_updated_entries`(max_date DATE)
BEGIN
	TRUNCATE TABLE kalturadw_ds.updated_entries;
	
	UPDATE kalturadw.aggr_managment SET start_time = NOW() WHERE is_calculated = 0 AND aggr_day < max_date AND aggr_name = 'plays_views';
	
	INSERT INTO kalturadw_ds.updated_entries SELECT entries.entry_id, SUM(IFNULL(count_loads, 0))+IFNULL(old_entries.views,0) views, SUM(IFNULL(count_plays, 0))+IFNULL(old_entries.plays,0) plays FROM 
	(SELECT DISTINCT entry_id 
		FROM kalturadw.dwh_hourly_events_entry e
		INNER JOIN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment WHERE is_calculated = 0 AND aggr_day < max_date AND aggr_name = 'plays_views') aggr_managment
		ON (e.date_id = aggr_managment.aggr_day_int)) entries
	INNER JOIN
	kalturadw.dwh_hourly_events_entry
	ON (dwh_hourly_events_entry.entry_id = entries.entry_id)
	LEFT OUTER JOIN
	kalturadw.entry_plays_views_before_08_2009 AS old_entries
	ON (entries.entry_id = old_entries.entry_id)
	GROUP BY entries.entry_id;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `create_updated_kusers_storage_usage` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `create_updated_kusers_storage_usage`(max_date DATE)
BEGIN
	TRUNCATE TABLE kalturadw_ds.updated_kusers_storage_usage;
	
	UPDATE kalturadw.aggr_managment SET start_time = NOW() WHERE is_calculated = 0 AND aggr_day < DATE(max_date) AND aggr_name = 'storage_usage_kuser_sync';
	
	INSERT INTO kalturadw_ds.updated_kusers_storage_usage 
	SELECT u.kuser_id , SUM(s.entry_additional_size_kb) storage_kb FROM 
		(SELECT DISTINCT entry_id FROM kalturadw.dwh_fact_entries_sizes s 
				INNER JOIN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment WHERE is_calculated = 0 AND aggr_day < max_date AND aggr_name = 'storage_usage_kuser_sync') aggr_managment
				ON (s.entry_size_date_id = aggr_managment.aggr_day_int)) updated_entries, 
		kalturadw.dwh_fact_entries_sizes s, 
		kalturadw.dwh_dim_entries u
	WHERE s.entry_id = u.entry_id 
	AND u.entry_id = updated_entries.entry_id
	GROUP BY u.kuser_id;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `drop_cycle_partition` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `drop_cycle_partition`(p_cycle_id VARCHAR(10))
BEGIN
	DECLARE table_name VARCHAR(32);
	DECLARE p_exists INT;
	DECLARE done INT DEFAULT 0;
	DECLARE staging_areas_cursor CURSOR FOR SELECT source_table 
						FROM kalturadw_ds.staging_areas, kalturadw_ds.cycles 
						WHERE staging_areas.process_id = cycles.process_id AND cycles.cycle_id = p_cycle_id;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	OPEN staging_areas_cursor;	
	read_loop: LOOP
		FETCH staging_areas_cursor INTO table_name;
		IF done THEN
			LEAVE read_loop;
		END IF;
		
		SELECT COUNT(*) INTO p_exists
		FROM information_schema.PARTITIONS 
		WHERE partition_name = CONCAT('p_',p_cycle_id)
		AND table_name = table_name
		AND table_schema = 'kalturadw_ds';
		
		IF(p_exists>0) THEN
			SET @s = CONCAT('alter table kalturadw_ds.',table_name,' drop PARTITION  p_' ,p_cycle_id);
			PREPARE stmt FROM  @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		END IF;
	END LOOP;
	CLOSE staging_areas_cursor;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `empty_cycle_partition` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `empty_cycle_partition`(p_cycle_id VARCHAR(10))
BEGIN
	CALL drop_cycle_partition(p_cycle_id);
	CALL add_cycle_partition(p_cycle_id);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `fms_sessionize` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `fms_sessionize`(
  partition_id INTEGER)
BEGIN
  DECLARE FMS_STALE_SESSION_PURGE DATETIME;
 
  SELECT SUBDATE(NOW(),INTERVAL int_value DAY) INTO FMS_STALE_SESSION_PURGE FROM parameters WHERE id=3; 
 
  DROP TABLE IF EXISTS ds_temp_fms_session_aggr;
  DROP TABLE IF EXISTS ds_temp_fms_sessions; 
 
  CREATE TEMPORARY TABLE ds_temp_fms_session_aggr (
    agg_session_id       	VARCHAR(20) NOT NULL,
    agg_session_time     	DATETIME    NOT NULL,
    agg_client_ip	 	VARCHAR(15),
    agg_client_ip_number 	INT(10),
    agg_client_country_id 	INT(10),
    agg_client_location_id 	INT(10),
    agg_session_date_id  	INT(11),
    agg_con_cs_bytes     	BIGINT,
    agg_con_sc_bytes     	BIGINT,
    agg_dis_cs_bytes     	BIGINT,
    agg_dis_sc_bytes     	BIGINT,
    agg_partner_id       	INT(10),
    agg_bandwidth_source_id     INT(11),
    agg_is_connected_ind	INT(11),
    agg_is_disconnected_ind	INT(11)
  ) ENGINE = MEMORY;
 
  CREATE TEMPORARY TABLE ds_temp_fms_sessions (
    session_id         		VARCHAR(20) NOT NULL,
    session_time       		DATETIME    NOT NULL,
    session_date_id    		INT(11),
    session_client_ip	 	VARCHAR(15),
    session_client_ip_number 	INT(10),
    session_client_country_id 	INT(10),
    session_client_location_id 	INT(10),
    session_partner_id 		INT(10),
    bandwidth_source_id		INT(11),
    total_bytes        		BIGINT
   ) ENGINE = MEMORY;
    
  INSERT INTO ds_temp_fms_session_aggr (agg_session_id,agg_session_time,agg_session_date_id, agg_client_ip, agg_client_ip_number, agg_client_country_id, agg_client_location_id,
              agg_con_cs_bytes,agg_con_sc_bytes,agg_dis_cs_bytes,agg_dis_sc_bytes,agg_partner_id, agg_bandwidth_source_id, agg_is_connected_ind, agg_is_disconnected_ind)
SELECT session_id, MAX(event_time), MAX(event_date_id), MAX(client_ip), MAX(client_ip_number), MAX(client_country_id), MAX(client_location_id),  	
    SUM(IF(t.event_type='connect',client_to_server_bytes,0)) con_cs_bytes,
    SUM(IF(t.event_type='connect',server_to_client_bytes,0)) con_sc_bytes,
    SUM(IF(t.event_type='disconnect',client_to_server_bytes,0)) dis_cs_bytes,
    SUM(IF(t.event_type='disconnect',server_to_client_bytes,0)) dis_sc_bytes,
    MAX(partner_id) partner_id, MAX(bandwidth_source_id),
    MAX(IF(t.event_type='connect',1,0)) is_connected_ind,
    MAX(IF(t.event_type='disconnect',1,0)) is_disconnected_ind
  FROM ds_fms_session_events e 
 INNER JOIN kalturadw.dwh_dim_fms_event_type t ON e.event_type_id = t.event_type_id
 INNER JOIN files f ON e.file_id = f.file_id
  LEFT OUTER JOIN kalturadw.dwh_dim_fms_bandwidth_source fbs ON (e.fms_app_id = fbs.fms_app_id AND f.process_id = fbs.process_id AND f.file_name REGEXP fbs.file_regex)
  WHERE e.cycle_id = partition_id
  GROUP BY session_id
  HAVING MAX(bandwidth_source_id) IS NOT NULL;
 
  INSERT INTO ds_temp_fms_sessions (session_id,session_time,session_date_id, session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id, session_partner_id, bandwidth_source_id, total_bytes)
  SELECT agg_session_id,agg_session_time,agg_session_date_id,agg_client_ip, agg_client_ip_number, agg_client_country_id, agg_client_location_id, agg_partner_id,agg_bandwidth_source_id,
  GREATEST(agg_dis_sc_bytes - agg_con_sc_bytes + agg_dis_cs_bytes - agg_con_cs_bytes, 0)
  FROM ds_temp_fms_session_aggr
  WHERE agg_partner_id IS NOT NULL AND agg_partner_id NOT IN (100  , -1  , -2  , 0 , 99 )  AND agg_is_connected_ind = 1 AND agg_is_disconnected_ind = 1;
  
  
  INSERT INTO fms_incomplete_sessions (session_id,session_time,updated_time,session_date_id, session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id, con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,partner_id, is_connected_ind, is_disconnected_ind)
  SELECT agg_session_id,agg_session_time,NOW() AS agg_update_time,agg_session_date_id,agg_client_ip, agg_client_ip_number, agg_client_country_id, agg_client_location_id,
         agg_con_cs_bytes,agg_con_sc_bytes,agg_dis_cs_bytes,agg_dis_sc_bytes,agg_partner_id, agg_is_connected_ind, agg_is_disconnected_ind
  FROM ds_temp_fms_session_aggr
  WHERE agg_partner_id IS NULL OR agg_is_connected_ind = 0 AND agg_is_disconnected_ind = 0
  ON DUPLICATE KEY UPDATE
    session_time=GREATEST(session_time,VALUES(session_time)),
    session_date_id=GREATEST(session_date_id,VALUES(session_date_id)),
    session_client_ip=VALUES(session_client_ip),
    session_client_ip_number=VALUES(session_client_ip_number), 
    session_client_location_id=VALUES(session_client_location_id),
    session_client_country_id=VALUES(session_client_country_id),
    con_cs_bytes=con_cs_bytes+VALUES(con_cs_bytes),
    con_sc_bytes=con_sc_bytes+VALUES(con_sc_bytes),
    dis_cs_bytes=dis_cs_bytes+VALUES(dis_cs_bytes),
    dis_sc_bytes=dis_sc_bytes+VALUES(dis_sc_bytes),
    partner_id=IF(partner_id IS NULL,VALUES(partner_id),partner_id),
    bandwidth_source_id=VALUES(bandwidth_source_id),
    updated_time=GREATEST(updated_time,VALUES(updated_time)),
    is_connected_ind = GREATEST(is_connected_ind, VALUES(is_connected_ind)),
    is_disconnected_ind = GREATEST(is_disconnected_ind, VALUES(is_disconnected_ind));
  
  INSERT INTO ds_temp_fms_sessions (session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,session_partner_id,bandwidth_source_id,total_bytes)
  SELECT session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,partner_id,bandwidth_source_id,
  GREATEST(dis_sc_bytes - con_sc_bytes + dis_cs_bytes -con_cs_bytes, 0)
  FROM fms_incomplete_sessions
  WHERE partner_id IS NOT NULL AND partner_id NOT IN (100  , -1  , -2  , 0 , 99 ) AND is_connected_ind = 1 AND is_disconnected_ind = 1;
    
  INSERT INTO fms_stale_sessions (partner_id, bandwidth_source_id, session_id, session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,last_update_time,purge_time)
  SELECT partner_id,bandwidth_source_id, session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,updated_time,NOW()
  FROM fms_incomplete_sessions
  WHERE GREATEST(session_time,updated_time) < FMS_STALE_SESSION_PURGE AND (partner_id IS NULL OR is_connected_ind = 0 AND is_disconnected_ind = 0);
  
  DELETE FROM fms_incomplete_sessions
  WHERE (partner_id IS NOT NULL AND is_connected_ind = 1 AND is_disconnected_ind = 1) OR
        GREATEST(session_time,updated_time) < FMS_STALE_SESSION_PURGE;
  
  INSERT INTO kalturadw.dwh_fact_fms_sessions (session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,session_partner_id,bandwidth_source_id,total_bytes)
  SELECT session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,session_partner_id,bandwidth_source_id,total_bytes
  FROM ds_temp_fms_sessions
  ON DUPLICATE KEY UPDATE
	total_bytes=VALUES(total_bytes),
	session_partner_id=VALUES(session_partner_id),
	session_time=VALUES(session_time),
	session_client_ip=VALUES(session_client_ip),
	session_client_ip_number=VALUES(session_client_ip_number),
	session_client_country_id=VALUES(session_client_country_id),
	session_client_location_id=VALUES(session_client_location_id),
	bandwidth_source_id=VALUES(bandwidth_source_id);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `fms_sessionize_by_date_id` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `fms_sessionize_by_date_id`(p_event_date_id INT)
BEGIN
	DROP TABLE IF EXISTS ds_temp_fms_session_aggr;
	DROP TABLE IF EXISTS ds_temp_fms_sessions;
 
	CREATE TEMPORARY TABLE ds_temp_fms_session_aggr (
	    agg_session_id       	VARCHAR(20) NOT NULL,
	    agg_session_time     	DATETIME    NOT NULL,
	    agg_client_ip	 	VARCHAR(15),
	    agg_client_ip_number 	INT(10),
	    agg_client_country_id 	INT(10),
	    agg_client_location_id 	INT(10),
	    agg_session_date_id  	INT(11),
	    agg_con_cs_bytes     	BIGINT,
	    agg_con_sc_bytes     	BIGINT,
	    agg_dis_cs_bytes     	BIGINT,
	    agg_dis_sc_bytes     	BIGINT,
	    agg_partner_id       	INT(10),
	    agg_bandwidth_source_id     INT(11)
	  ) ENGINE = MEMORY;
	 
	  CREATE TABLE ds_temp_fms_sessions (
	    session_id         		VARCHAR(20) NOT NULL,
	    session_time       		DATETIME    NOT NULL,
	    session_date_id    		INT(11),
	    session_client_ip	 	VARCHAR(15),
	    session_client_ip_number 	INT(10),
	    session_client_country_id 	INT(10),
	    session_client_location_id 	INT(10),
	    session_partner_id 		INT(10),
	    bandwidth_source_id		INT(11),
	    total_bytes        		BIGINT      
	   ) ENGINE = MEMORY;
	    
	
	INSERT INTO ds_temp_fms_session_aggr (agg_session_id,agg_session_time,agg_session_date_id, agg_client_ip, agg_client_ip_number, agg_client_country_id, agg_client_location_id,
		agg_con_cs_bytes,agg_con_sc_bytes,agg_dis_cs_bytes,agg_dis_sc_bytes,agg_partner_id, agg_bandwidth_source_id)
		SELECT session_id, MAX(event_time), MAX(event_date_id), MAX(client_ip), MAX(client_ip_number), MAX(client_country_id), MAX(client_location_id),  	
			SUM(IF(t.event_type='connect',client_to_server_bytes,0)) con_cs_bytes,
			SUM(IF(t.event_type='connect',server_to_client_bytes,0)) con_sc_bytes,
			SUM(IF(t.event_type='disconnect',client_to_server_bytes,0)) dis_cs_bytes,
			SUM(IF(t.event_type='disconnect',server_to_client_bytes,0)) dis_sc_bytes,
			MAX(partner_id) partner_id, MAX(bandwidth_source_id) bandwidth_source_id
		FROM kalturadw.dwh_fact_fms_session_events e 
		INNER JOIN kalturadw.dwh_dim_fms_event_type t ON e.event_type_id = t.event_type_id
		INNER JOIN files f ON e.file_id = f.file_id
		LEFT OUTER JOIN kalturadw.dwh_dim_fms_bandwidth_source fbs ON (e.fms_app_id = fbs.fms_app_id AND f.process_id = fbs.process_id)
		WHERE e.event_date_id = p_event_date_id 
		GROUP BY session_id
		HAVING MAX(bandwidth_source_id) IS NOT NULL;
	 
	INSERT INTO ds_temp_fms_sessions (session_id,session_time,session_date_id, session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id, session_partner_id, bandwidth_source_id, total_bytes)
		SELECT agg_session_id,agg_session_time,agg_session_date_id,agg_client_ip, agg_client_ip_number, agg_client_country_id, agg_client_location_id, agg_partner_id,agg_bandwidth_source_id,
		GREATEST(agg_dis_sc_bytes - agg_con_sc_bytes + agg_dis_cs_bytes -agg_con_cs_bytes, 0)
		FROM ds_temp_fms_session_aggr
		WHERE agg_partner_id IS NOT NULL AND agg_partner_id NOT IN (100  , -1  , -2  , 0 , 99 ) AND agg_dis_cs_bytes >0 AND agg_con_cs_bytes > 0;
	
	INSERT INTO kalturadw.dwh_fact_fms_sessions (session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,session_partner_id,bandwidth_source_id,total_bytes)
	SELECT session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, session_client_country_id, session_client_location_id,session_partner_id,bandwidth_source_id,total_bytes
	FROM ds_temp_fms_sessions
	ON DUPLICATE KEY UPDATE
		total_bytes=VALUES(total_bytes),
		session_partner_id=VALUES(session_partner_id),
		session_time=VALUES(session_time),
		session_client_ip=VALUES(session_client_ip),
		session_client_ip_number=VALUES(session_client_ip_number),
		session_client_country_id=VALUES(session_client_country_id),
		session_client_location_id=VALUES(session_client_location_id),
		bandwidth_source_id=VALUES(bandwidth_source_id);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `insert_invalid_ds_line` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `insert_invalid_ds_line`(line_number_param INT(11), 
									file_id_param INT(11), 
									error_reason_param VARCHAR(255), 
									ds_line_param VARCHAR(4096), 
									date_id_param INT(11),
									partner_id_param INT(11), 
									cycle_id_param INT(11), 
									process_id_param INT(11))
BEGIN
	INSERT IGNORE INTO invalid_ds_lines (line_number, file_id, error_reason_code, ds_line, date_id, partner_id, cycle_id, process_id)
	VALUES (line_number_param, file_id_param, get_error_code(error_reason_param), ds_line_param, date_id_param, partner_id_param, cycle_id_param, process_id_param);
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `register_file` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `register_file`(p_file_name VARCHAR(750), p_process_id INT, p_file_size_kb INT(11), p_compression_suffix VARCHAR(10), p_subdir VARCHAR(1024))
BEGIN
	DECLARE v_assigned_server_id INT;
	DECLARE v_cycle_id INT;
	DECLARE v_file_id INT;
	
	SELECT file_id INTO v_file_id FROM kalturadw_ds.files WHERE file_name = p_file_name AND process_id = p_process_id AND compression_suffix = p_compression_suffix;
	
	IF (v_file_id IS NULL) THEN
		SELECT etl_server_id INTO v_assigned_server_id
			FROM kalturadw_ds.etl_servers s  
				LEFT OUTER JOIN kalturadw_ds.cycles c 
					ON (s.etl_server_id = c.assigned_server_id AND c.STATUS = 'REGISTERED' AND c.process_id = p_process_id)
				LEFT OUTER JOIN kalturadw_ds.files f
					ON (c.cycle_id = f.cycle_id)		
			GROUP BY etl_server_id
			ORDER BY SUM(IF(file_id IS NULL,0,1)) LIMIT 1;
		
		IF (v_assigned_server_id IS NOT NULL) THEN
			SELECT c.cycle_id INTO v_cycle_id 
				FROM kalturadw_ds.processes p INNER JOIN kalturadw_ds.cycles c ON (c.process_id = p.id) LEFT OUTER JOIN kalturadw_ds.files f ON (c.cycle_id = f.cycle_id)
				WHERE 	p.id = p_process_id AND
					c.STATUS='REGISTERED' AND
					c.assigned_server_id = v_assigned_server_id
				GROUP BY c.cycle_id
				HAVING COUNT(file_id)<MAX(max_files_per_cycle) LIMIT 1;
			
			
			IF (v_cycle_id IS NOT NULL) THEN 
				INSERT INTO kalturadw_ds.files 	(file_name, file_status, insert_time, file_size_kb, process_id, cycle_id, compression_suffix, subdir)
							VALUES	(p_file_name, 'IN_CYCLE', NOW(), p_file_size_kb, p_process_id, v_cycle_id, p_compression_suffix, p_subdir);
			ELSE
				INSERT INTO kalturadw_ds.cycles (STATUS, insert_time, process_id, assigned_server_id)
							VALUES	('REGISTERED', NOW(), p_process_id, v_assigned_server_id);
				SET v_cycle_id = LAST_INSERT_ID();
				CALL add_cycle_partition(v_cycle_id);
				INSERT INTO kalturadw_ds.files 	(file_name, file_status, insert_time, file_size_kb, process_id, cycle_id, compression_suffix, subdir)
							VALUES	(p_file_name, 'IN_CYCLE', NOW(), p_file_size_kb, p_process_id, v_cycle_id, p_compression_suffix, p_subdir);
			END IF;
		END IF;
	END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `restore_file_status` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `restore_file_status`(
	pfile_id INT(20)
    )
BEGIN
	UPDATE kalturadw_ds.files f
	SET f.file_status = f.prev_status,
	    f.prev_status = f.file_status
	WHERE f.file_id = pfile_id;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `set_cycle_status` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `set_cycle_status`(
	p_cycle_id INT(20),
	new_cycle_status VARCHAR(20)
    )
BEGIN
	UPDATE kalturadw_ds.cycles c
	SET c.prev_status = c.status
	    ,c.status = new_cycle_status
	WHERE c.cycle_id = p_cycle_id;
	
	IF new_cycle_status = 'RUNNING'
	THEN 
		UPDATE kalturadw_ds.cycles c
		SET c.run_time = NOW()
		WHERE c.cycle_id = p_cycle_id;
	ELSEIF new_cycle_status = 'TRANSFERING'
	THEN 
		UPDATE kalturadw_ds.cycles c
		SET c.transfer_time = NOW()
		WHERE c.cycle_id = p_cycle_id;
	END IF;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `set_file_status` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `set_file_status`(
	pfile_id INT(20),
	new_file_status VARCHAR(20)
    )
BEGIN
	CALL set_file_status_full(pfile_id,new_file_status,1);
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `set_file_status_full` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `set_file_status_full`(
	pfile_id INT(20),
	new_file_status VARCHAR(20),
	override_safety_check INT
    )
BEGIN
	DECLARE cur_status VARCHAR(20);
	IF override_safety_check = 1 THEN
		SELECT f.file_status
		INTO cur_status
		FROM kalturadw_ds.files f
		WHERE f.file_id = pfile_id;
		IF  new_file_status NOT IN ('WAITING','RUNNING','PROCESSED','TRANSFERING','DONE','FAILED')
		 OR new_file_status = 'RUNNING' AND cur_status <> 'WAITING'
		 OR new_file_status = 'PROCESSED' AND cur_status <> 'RUNNING'
		 OR new_file_status = 'TRANSFERING' AND cur_status <> 'PROCESSED'
		 OR new_file_status = 'DONE' AND cur_status <> 'TRANSFERING'
		THEN
			SET @s = CONCAT('call Illegal_state_trying_to_set_',
					new_file_status,'_to_', cur_status,'_file_',pfile_id);
			PREPARE stmt FROM  @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;		
		END IF;
	END IF;
	
	UPDATE kalturadw_ds.files f
	SET f.prev_status = f.file_status
	    ,f.file_status = new_file_status
	WHERE f.file_id = pfile_id;
	IF new_file_status = 'RUNNING'
	THEN 
		UPDATE kalturadw_ds.files f
		SET f.run_time = NOW()
		WHERE f.file_id = pfile_id;
	ELSEIF new_file_status = 'TRANSFERING'
	THEN 
		UPDATE kalturadw_ds.files f
		SET f.transfer_time = NOW()
		WHERE f.file_id = pfile_id;
	END IF;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `transfer_cycle_partition` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `transfer_cycle_partition`(p_cycle_id VARCHAR(10))
BEGIN
	DECLARE src_table VARCHAR(45);
	DECLARE tgt_table VARCHAR(45);
	DECLARE dup_clause VARCHAR(4000);
	DECLARE partition_field VARCHAR(45);
	DECLARE select_fields VARCHAR(4000);
	DECLARE post_transfer_sp_val VARCHAR(4000);
	DECLARE aggr_date VARCHAR(400);
	DECLARE aggr_hour VARCHAR(400);
	DECLARE aggr_names VARCHAR(4000);
	
	DECLARE done INT DEFAULT 0;
	DECLARE staging_areas_cursor CURSOR FOR SELECT 	source_table, target_table, IFNULL(on_duplicate_clause,''),	staging_partition_field, post_transfer_sp, aggr_date_field, hour_id_field, post_transfer_aggregations
											FROM staging_areas s, cycles c
											WHERE s.process_id=c.process_id AND c.cycle_id = p_cycle_id;
											
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	OPEN staging_areas_cursor;
	
	read_loop: LOOP
		FETCH staging_areas_cursor INTO src_table, tgt_table, dup_clause, partition_field, post_transfer_sp_val, aggr_date, aggr_hour, aggr_names;
		IF done THEN
			LEAVE read_loop;
		END IF;
		
		IF ((LENGTH(AGGR_DATE) > 0) && (LENGTH(aggr_names) > 0)) THEN
		
			SET @s = CONCAT(
				'INSERT INTO kalturadw.aggr_managment(aggr_name, aggr_day, aggr_day_int, hour_id, is_calculated)
				SELECT aggr_name, date(aggr_date), aggr_date, aggr_hour, 0
				FROM (SELECT DISTINCT aggr_name FROM kalturadw.aggr_managment) a, 
					(select distinct ',aggr_date, ' aggr_date,' ,aggr_hour,' aggr_hour 
					 from ',src_table,
					' where ',partition_field,' = ',p_cycle_id,') ds
				WHERE aggr_name in ', aggr_names,'
				ON DUPLICATE KEY UPDATE is_calculated = 0');
			
			PREPARE stmt FROM @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		END IF;

		SELECT 	GROUP_CONCAT(column_name ORDER BY ordinal_position)
		INTO 	select_fields
		FROM information_schema.COLUMNS
		WHERE CONCAT(table_schema,'.',table_name) = tgt_table;
			
		SET @s = CONCAT(	'insert into ',tgt_table, ' (',select_fields,') ',
						' select ',select_fields,
						' from ',src_table,
						' where ',partition_field,'  = ',p_cycle_id,
						' ',dup_clause );

		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
			
		IF LENGTH(POST_TRANSFER_SP_VAL)>0 THEN
				SET @s = CONCAT('call ',post_transfer_sp_val,'(',p_cycle_id,')');
				
				PREPARE stmt FROM  @s;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
		END IF;
	END LOOP;

	CLOSE staging_areas_cursor;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Final view structure for view `ds_events_partitions`
--

/*!50001 DROP TABLE IF EXISTS `ds_events_partitions`*/;
/*!50001 DROP VIEW IF EXISTS `ds_events_partitions`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `ds_events_partitions` AS (select `partitions`.`TABLE_SCHEMA` AS `TABLE_SCHEMA`,`partitions`.`TABLE_NAME` AS `TABLE_NAME`,`partitions`.`PARTITION_NAME` AS `PARTITION_NAME`,substr(`partitions`.`PARTITION_NAME`,3) AS `partition_number`,`partitions`.`TABLE_ROWS` AS `table_rows`,`partitions`.`CREATE_TIME` AS `CREATE_TIME` from `information_schema`.`partitions` where ((`partitions`.`TABLE_NAME` = 'ds_events') and (`partitions`.`PARTITION_NAME` <> 'p_0'))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

USE kalturadw_bisources;

DROP TABLE IF EXISTS `bisources_asset_status`;
CREATE TABLE `bisources_asset_status` (
  `asset_status_id` smallint(6) NOT NULL,
  `asset_status_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`asset_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_asset_status` VALUES (-1,'ERROR'),(0,'QUEUED'),(1,'CONVERTING'),(2,'READY'),(3,'DELETED'),(4,'NOT_APPLICABLE');

DROP TABLE IF EXISTS `bisources_batch_job_error_type`;
CREATE TABLE `bisources_batch_job_error_type` (
  `batch_job_error_type_id` int(11) NOT NULL,
  `batch_job_error_type_name` varchar(100) DEFAULT 'missing value',
  PRIMARY KEY (`batch_job_error_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_batch_job_error_type` VALUES (0,'PENDING'),(1,'QUEUED'),(2,'PROCESSING'),(3,'PROCESSED'),(4,'MOVEFILE'),(5,'FINISHED'),(6,'FAILED'),(7,'ABORTED'),(8,'ALMOST_DONE'),(9,'RETRY'),(10,'FATAL'),(11,'DONT_PROCESS');

DROP TABLE IF EXISTS `bisources_batch_job_status`;
CREATE TABLE `bisources_batch_job_status` (
  `batch_job_status_id` int(11) NOT NULL,
  `batch_job_status_name` varchar(100) DEFAULT 'missing value',
  PRIMARY KEY (`batch_job_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_batch_job_status` VALUES (0,'APP'),(1,'RUNTIME'),(2,'HTTP'),(3,'CURL'),(4,'KALTURA_API'),(5,'KALTURA_CLIENT');

DROP TABLE IF EXISTS `bisources_batch_job_type`;
CREATE TABLE `bisources_batch_job_type` (
  `batch_job_type_id` int(11) NOT NULL,
  `batch_job_type_name` varchar(100) DEFAULT 'missing value',
  PRIMARY KEY (`batch_job_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_batch_job_type` VALUES (0,'CONVERT'),(1,'IMPORT'),(2,'DELETE'),(3,'FLATTEN'),(4,'BULKUPLOAD'),(5,'DVDCREATOR'),(6,'DOWNLOAD'),(7,'OOCONVERT'),(10,'CONVERT_PROFILE'),(11,'POSTCONVERT'),(12,'PULL'),(13,'REMOTE_CONVERT'),(14,'EXTRACT_MEDIA'),(15,'MAIL'),(16,'NOTIFICATION'),(17,'CLEANUP'),(18,'SCHEDULER_HELPER'),(19,'BULKDOWNLOAD'),(20,'DB_CLEANUP'),(21,'PROVISION_PROVIDE'),(22,'CONVERT_COLLECTION'),(23,'STORAGE_EXPORT'),(24,'PROVISION_DELETE'),(25,'STORAGE_DELETE'),(26,'EMAIL_INGESTION'),(27,'METADATA_IMPORT'),(28,'METADATA_TRANSFORM'),(29,'FILESYNC_IMPORT'),(10001,'BatchJobType.VirusScan.virusScan'),(10011,'entryStatus.Infected.virusScan'),(10021,'conversionEngineType.QuickTimeTools.quickTimeTools'),(10031,'conversionEngineType.FastStart.fastStart'),(10041,'conversionEngineType.FastStart.fastStart'),(10051,'conversionEngineType.FastStart.fastStart'),(10061,'conversionEngineType.ExpressionEncoder.expressionEncoder');

DROP TABLE IF EXISTS `bisources_control`;
CREATE TABLE `bisources_control` (
  `control_id` smallint(6) NOT NULL,
  `control_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`control_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_control` VALUES (1,'control_one'),(2,'control_two');

DROP TABLE IF EXISTS `bisources_creation_mode`;
CREATE TABLE `bisources_creation_mode` (
  `creation_mode_id` smallint(6) NOT NULL,
  `creation_mode_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`creation_mode_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_creation_mode` VALUES (-1,''),(1,'MANUAL'),(2,'KMC'),(3,'AUTOMATIC'),(4,'AUTOMATIC_BYPASS_FLV');

DROP TABLE IF EXISTS `bisources_entry_media_source`;
CREATE TABLE `bisources_entry_media_source` (
  `entry_media_source_id` smallint(6) NOT NULL,
  `entry_media_source_name` varchar(25) DEFAULT 'missing value',
  PRIMARY KEY (`entry_media_source_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_entry_media_source` VALUES (-1,'UNKNOWN'),(0,'OTHER'),(1,'FILE'),(2,'WEBCAM'),(3,'FLICKR'),(4,'YOUTUBE'),(5,'URL'),(6,'TEXT'),(7,'MYSPACE'),(8,'PHOTOBUCKET'),(9,'JAMENDO'),(10,'CCMIXTER'),(11,'NYPL'),(12,'CURRENT'),(13,'COMMONS'),(20,'KALTURA'),(21,'KALTURA_USER_CLIPS'),(22,'ARCHIVE_ORG'),(23,'KALTURA_PARTNER'),(24,'METACAFE'),(25,'KALTURA_QA'),(26,'KALTURA_KSHOW'),(27,'KALTURA_PARTNER_KSHOW'),(28,'SEARCH_PROXY'),(29,'AKAMAI_LIVE');

DROP TABLE IF EXISTS `bisources_entry_media_type`;
CREATE TABLE `bisources_entry_media_type` (
  `entry_media_type_id` smallint(6) NOT NULL,
  `entry_media_type_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`entry_media_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_entry_media_type` VALUES (-1,'AUTOMATIC'),(0,'ANY'),(1,'VIDEO'),(2,'IMAGE'),(3,'TEXT'),(4,'HTML'),(5,'AUDIO'),(6,'SHOW'),(7,'SHOW_XML'),(9,'BUBBLES'),(10,'XML'),(11,'DOCUMENT'),(101,'GENERIC_1'),(102,'GENERIC_2'),(103,'GENERIC_3'),(104,'GENERIC_4'),(201,'LIVE_STREAM_FLASH'),(202,'LIVE_STREAM_WINDOWS_MEDIA'),(203,'LIVE_STREAM_REAL_MEDIA'),(204,'LIVE_STREAM_QUICKTIME');

DROP TABLE IF EXISTS `bisources_entry_status`;
CREATE TABLE `bisources_entry_status` (
  `entry_status_id` smallint(6) NOT NULL,
  `entry_status_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`entry_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_entry_status` VALUES (-2,'ERROR_IMPORTING'),(-1,'ERROR_CONVERTING'),(0,'IMPORT'),(1,'PRECONVERT'),(2,'READY'),(3,'DELETED'),(4,'PENDING'),(5,'MODERATE '),(6,'BLOCKED');

DROP TABLE IF EXISTS `bisources_entry_type`;
CREATE TABLE `bisources_entry_type` (
  `entry_type_id` smallint(6) NOT NULL,
  `entry_type_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`entry_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_entry_type` VALUES (-1,'AUTOMATIC'),(0,'BACKGROUND'),(1,'MEDIACLIP'),(2,'SHOW'),(4,'BUBBLES'),(5,'PLAYLIST'),(7,'LIVE_STREAM'),(10,'DOCUMENT'),(300,'DVD');

DROP TABLE IF EXISTS `bisources_event_type`;
CREATE TABLE `bisources_event_type` (
  `event_type_id` smallint(6) NOT NULL,
  `event_type_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`event_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_event_type` VALUES (1,'Widget Loaded'),(2,'Media Loaded (view)'),(3,'Play'),(4,'Play reached 25%'),(5,'Play reached 50%'),(6,'Play reached 75%'),(7,'Play reached 100%'),(8,'Open Edit'),(9,'Open Viral'),(10,'Open Download'),(11,'Open Report'),(12,'Buffer Start'),(13,'Buffer End'),(14,'Open Full Screen'),(15,'Close Full Screen'),(16,'Replay'),(17,'Seek'),(18,'Open Upload'),(19,'Save & Publish'),(20,'Close Edtior'),(21,'Pre Bumper Played'),(22,'Post Bumper Played'),(23,'Bumper Clicked'),(24,'Preroll Started'),(25,'Midroll Started'),(26,'Postroll Started'),(27,'Overlay Started'),(28,'Preroll Clicked'),(29,'Midroll Clicked'),(30,'Postroll Clicked'),(31,'Overlay Clicked'),(32,'Preroll 25'),(33,'Preroll 50'),(34,'Preroll 75'),(35,'Midroll 25'),(36,'Midroll 50'),(37,'Midroll 75'),(38,'Postroll 25'),(39,'Postroll 50'),(40,'Postroll 75');

DROP TABLE IF EXISTS `bisources_file_sync_object_type`;
CREATE TABLE `bisources_file_sync_object_type` (
  `file_sync_object_type_id` smallint(6) NOT NULL,
  `file_sync_object_type_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`file_sync_object_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_file_sync_object_type` VALUES (1,'ENTRY'),(2,'UICONF'),(3,'BATCHJOB'),(4,'FLAVOR_ASSET'),(5,'METADATA'),(6,'METADATA_PROFILE');

DROP TABLE IF EXISTS `bisources_file_sync_status`;
CREATE TABLE `bisources_file_sync_status` (
  `file_sync_status_id` smallint(6) NOT NULL,
  `file_sync_status_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`file_sync_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_file_sync_status` VALUES (-1,'PENDING'),(1,'PENDING'),(2,'READY'),(3,'DELETED'),(4,'PURGED');

DROP TABLE IF EXISTS `bisources_flavor_asset_status`;
CREATE TABLE `bisources_flavor_asset_status` (
  `flavor_asset_status_id` smallint(6) NOT NULL,
  `flavor_asset_status_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`flavor_asset_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_flavor_asset_status` VALUES (-1,'STATUS_ERROR'),(0,'STATUS_QUEUED'),(1,'STATUS_CONVERTING'),(2,'STATUS_READY'),(3,'STATUS_DELETED'),(4,'STATUS_NOT_APPLICABLE');

DROP TABLE IF EXISTS `bisources_fms_app`;
CREATE TABLE `bisources_fms_app` (
  `fms_app_id` smallint(6) NOT NULL,
  `fms_app_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`fms_app_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_fms_app` VALUES (1,'ondemand'),(5,'live');

DROP TABLE IF EXISTS `bisources_gender`;
CREATE TABLE `bisources_gender` (
  `gender_id` smallint(6) NOT NULL,
  `gender_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`gender_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_gender` VALUES (1,'MALE'),(2,'FEMALE');

DROP TABLE IF EXISTS `bisources_moderation_status`;
CREATE TABLE `bisources_moderation_status` (
  `moderation_status_id` smallint(6) NOT NULL,
  `moderation_status_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`moderation_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_moderation_status` VALUES (1,'PENDING'),(2,'APROVED'),(3,'BLOCK'),(4,'DELETE'),(5,'REVIEW'),(6,'AUTO_APPROVED');

DROP TABLE IF EXISTS `bisources_partner_class_of_service`;
CREATE TABLE `bisources_partner_class_of_service` (
  `partner_class_of_service_id` smallint(6) NOT NULL,
  `partner_class_of_service_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`partner_class_of_service_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_partner_class_of_service` VALUES (0,'N/A'),(1,'Silver'),(2,'Gold'),(3,'Platinum');

DROP TABLE IF EXISTS `bisources_partner_group_type`;
CREATE TABLE `bisources_partner_group_type` (
  `partner_group_type_id` smallint(6) NOT NULL,
  `partner_group_type_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`partner_group_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_partner_group_type` VALUES (1,'Publisher'),(2,'VAR'),(3,'Group');

DROP TABLE IF EXISTS `bisources_partner_status`;
CREATE TABLE `bisources_partner_status` (
  `partner_status_id` smallint(6) NOT NULL,
  `partner_status_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`partner_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_partner_status` VALUES (1,'status_one'),(2,'status_two');

DROP TABLE IF EXISTS `bisources_partner_type`;
CREATE TABLE `bisources_partner_type` (
  `partner_type_id` smallint(6) NOT NULL,
  `partner_type_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`partner_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_partner_type` VALUES (1,'KMC_SIGNUP'),(2,'OTHER'),(100,'WIKI'),(101,'WORDPRESS'),(102,'DRUPAL'),(103,'MIND_TOUCH'),(104,'MOODLE'),(105,'COMMUNITY_EDITION'),(106,'JOOMLA ');

DROP TABLE IF EXISTS `bisources_partner_vertical`;

CREATE TABLE `bisources_partner_vertical` (
  `partner_vertical_id` smallint(6) NOT NULL,
  `partner_vertical_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`partner_vertical_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_partner_vertical` VALUES (0,'N/A'),(1,'Media'),(2,'Education'),(3,'Enterprise'),(4,'Service Provider'),(5,'Other');

DROP TABLE IF EXISTS `bisources_ready_behavior`;
CREATE TABLE `bisources_ready_behavior` (
  `ready_behavior_id` smallint(6) NOT NULL,
  `ready_behavior_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`ready_behavior_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_ready_behavior` VALUES (-1,'_IGNORE'),(0,'INHERIT_FLAVOR_PARAMS'),(1,'REQUIRED'),(2,'OPTIONAL');

DROP TABLE IF EXISTS `bisources_ui_conf_status`;
CREATE TABLE `bisources_ui_conf_status` (
  `ui_conf_status_id` smallint(6) NOT NULL,
  `ui_conf_status_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`ui_conf_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_ui_conf_status` VALUES (1,'PENDING'),(2,'READY'),(3,'DELETED');

DROP TABLE IF EXISTS `bisources_ui_conf_type`;
CREATE TABLE `bisources_ui_conf_type` (
  `ui_conf_type_id` smallint(6) NOT NULL,
  `ui_conf_type_name` varchar(50) DEFAULT 'missing value',
  PRIMARY KEY (`ui_conf_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_ui_conf_type` VALUES (1,'KDP'),(2,'CW'),(3,'EDITOR'),(4,'ADVANCED_EDITOR'),(5,'PLAYLIST'),(6,'APP_STUDIO'), (8, 'KDP3'), (14, 'Silverlight player'), (15, 'CLIENTSIDE_ENCODER'), (0, 'GENERIC'), (7, 'KRecord'), (11, 'KMC_CONTENT'), (12, 'KMC_DASHBOARD'), (9, 'KMC_ACCOUNT'), (10, 'KMC_ANALYTICS'), (17, 'KMC_ROLES_AND_PERMISSIONS'), (16, 'KMC_GENERAL'), (18, 'Clipper');
	
DROP TABLE IF EXISTS `bisources_user_status`;
CREATE TABLE `bisources_user_status` (
  `user_status_id` smallint(6) NOT NULL,
  `user_status_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`user_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_user_status` VALUES (0,'BLOCKED'),(1,'ACTIVE'),(2,'DELETED');

DROP TABLE IF EXISTS `bisources_widget_security_policy`;
CREATE TABLE `bisources_widget_security_policy` (
  `widget_security_policy_id` smallint(6) NOT NULL,
  `widget_security_policy_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`widget_security_policy_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_widget_security_policy` VALUES (1,'NONE'),(2,'ROOT');

DROP TABLE IF EXISTS `bisources_widget_security_type`;
CREATE TABLE `bisources_widget_security_type` (
  `widget_security_type_id` smallint(6) NOT NULL,
  `widget_security_type_name` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`widget_security_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `bisources_widget_security_type` VALUES (1,'NONE'),(2,'TIMEHASH'),(3,'MATCH_IP'),(4,'FORCE_KS');

USE kalturadw;

ALTER TABLE aggr_managment
ADD COLUMN `hour_id` int(11) unsigned NOT NULL DEFAULT '0' AFTER `aggr_day`,
DROP PRIMARY KEY,
ADD PRIMARY KEY (`aggr_name`,`aggr_day_int`,`hour_id`);

UPDATE aggr_managment
SET aggr_name = 'bandwidth_usage', is_calculated = 0
WHERE aggr_name = 'partner_usage';

UPDATE aggr_managment
SET is_calculated = 0
WHERE aggr_name = 'storage_usage';

INSERT INTO aggr_managment (aggr_name, aggr_day_int, aggr_day, is_calculated, start_time, end_time, hour_id)
SELECT aggr_name, aggr_day_int, aggr_day, is_calculated, start_time, end_time, hours.hour_id
FROM aggr_managment aggr, 
(SELECT 1 hour_id UNION 
SELECT 2 UNION
SELECT 3 UNION
SELECT 4 UNION
SELECT 5 UNION
SELECT 6 UNION
SELECT 7 UNION
SELECT 8 UNION
SELECT 9 UNION
SELECT 10 UNION
SELECT 11 UNION
SELECT 12 UNION
SELECT 13 UNION
SELECT 14 UNION
SELECT 15 UNION
SELECT 16 UNION
SELECT 17 UNION
SELECT 18 UNION
SELECT 19 UNION
SELECT 20 UNION
SELECT 21 UNION
SELECT 22 UNION
SELECT 23 ) hours
WHERE aggr_name in ('country','domain','domain_referrer','entry','partner','uid','widget');

INSERT INTO aggr_managment (aggr_name, aggr_day_int, aggr_day, hour_id, is_calculated)
SELECT distinct 'devices' aggr_name, aggr_day_int, aggr_day, hour_id, IF(aggr_day_int <= date(now())*1,1,0) FROM aggr_managment;

DELETE FROM bisources_tables;
INSERT INTO `bisources_tables` VALUES ('entry_media_source',1),('entry_media_type',1),('entry_status',1),('entry_type',1),('partner_status',1),('partner_type',1),('ui_conf_status',1),('ui_conf_type',1),('user_status',1),('moderation_status',1),('widget_security_type',1),('widget_security_policy',1),('event_type',1),('control',1),('gender',1),('partner_group_type',1),('file_sync_object_type',1),('file_sync_status',1),('ready_behavior',1),('asset_status',1),('creation_mode',1),('batch_job_type',1),('batch_job_status',1),('batch_job_error_type',1),('fms_app',1),('partner_vertical',1),('partner_class_of_service',1);

ALTER TABLE dwh_dim_asset_status
ALTER ri_ind SET DEFAULT '1';

ALTER TABLE dwh_dim_audio_codec
CHANGE `audio_codec` `audio_codec` VARCHAR(333) DEFAULT NULL,
ADD UNIQUE KEY `audio_codec` (`audio_codec`);

DROP TABLE IF EXISTS `dwh_dim_bandwidth_source`;
CREATE TABLE `dwh_dim_bandwidth_source` (
  `bandwidth_source_id` int(11) NOT NULL DEFAULT '0',
  `bandwidth_source_name` varchar(50) DEFAULT NULL,
  `dwh_creation_date` datetime DEFAULT NULL,
  `dwh_update_date` datetime DEFAULT NULL,
  `ri_ind` tinyint(4) DEFAULT '0',
  `is_live` tinyint(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`bandwidth_source_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `dwh_dim_bandwidth_source` VALUES (1,'WWW',NULL,NULL,0,0),(2,'LLN',NULL,NULL,0,0),(3,'LEVEL3',NULL,NULL,0,0),(4,'akamai_vod_http',NULL,NULL,0,0),(5,'akamai_live_fms',NULL,NULL,0,1),(6,'akamai_vod_fms',NULL,NULL,0,0),(7,'akamai_vod_fms_HD',NULL,NULL,0,0);

CREATE TABLE `dwh_dim_browser` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `browser` varchar(50) NOT NULL,
  `group` varchar(50) NOT NULL,
  `manufacturer` varchar(50) NOT NULL,
  `render_engine` varchar(50) NOT NULL,
  `type` varchar(50) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `browser` (`browser`,`group`,`manufacturer`,`render_engine`,`type`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `dwh_dim_batch_job` (
  `dwh_id` int(11) NOT NULL AUTO_INCREMENT,
  `id` int(11) NOT NULL,
  `job_type_id` smallint(6) DEFAULT NULL,
  `job_sub_type_id` smallint(6) DEFAULT NULL,
  `data` varchar(8192) DEFAULT NULL,
  `file_size` int(11) DEFAULT NULL,
  `duplication_key` varchar(41) DEFAULT NULL,
  `status_id` int(11) DEFAULT NULL,
  `abort` tinyint(4) DEFAULT NULL,
  `check_again_timeout` int(11) DEFAULT NULL,
  `progress` tinyint(4) DEFAULT NULL,
  `message` varchar(1024) DEFAULT NULL,
  `description` varchar(1024) DEFAULT NULL,
  `updates_count` smallint(6) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `created_by` varchar(20) DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `updated_by` varchar(20) DEFAULT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `priority` tinyint(4) DEFAULT NULL,
  `work_group_id` int(11) DEFAULT NULL,
  `queue_time` datetime DEFAULT NULL,
  `finish_time` datetime DEFAULT NULL,
  `entry_id` varchar(20) DEFAULT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `subp_id` int(11) DEFAULT NULL,
  `scheduler_id` int(11) DEFAULT NULL,
  `worker_id` int(11) DEFAULT NULL,
  `batch_index` int(11) DEFAULT NULL,
  `last_scheduler_id` int(11) DEFAULT NULL,
  `last_worker_id` int(11) DEFAULT NULL,
  `last_worker_remote` int(11) DEFAULT '0',
  `processor_name` varchar(64) DEFAULT NULL,
  `processor_expiration` datetime DEFAULT NULL,
  `parent_job_id` int(11) DEFAULT NULL,
  `processor_location` varchar(64) DEFAULT NULL,
  `execution_attempts` tinyint(4) DEFAULT NULL,
  `lock_version` int(11) DEFAULT NULL,
  `twin_job_id` int(11) DEFAULT NULL,
  `bulk_job_id` int(11) DEFAULT NULL,
  `root_job_id` int(11) DEFAULT NULL,
  `dc` varchar(2) DEFAULT NULL,
  `error_type_id` int(11) DEFAULT '0',
  `err_number` int(11) DEFAULT '0',
  `on_stress_divert_to` int(11) DEFAULT '0',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`dwh_id`),
  UNIQUE KEY `id` (`id`),
  KEY `dwh_update_date` (`dwh_update_date`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `dwh_dim_batch_job_error_type` (
  `batch_job_error_type_id` int(11) NOT NULL,
  `batch_job_error_type_name` varchar(100) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `dwh_update_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `ri_ind` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`batch_job_error_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `dwh_dim_batch_job_status` (
  `batch_job_status_id` int(11) NOT NULL,
  `batch_job_status_name` varchar(100) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `dwh_update_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `ri_ind` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`batch_job_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `dwh_dim_batch_job_sub_type` (
  `batch_job_type_id` int(11) NOT NULL,
  `batch_job_sub_type_id` int(11) NOT NULL,
  `batch_job_type_name` varchar(100) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `dwh_update_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `ri_ind` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`batch_job_type_id`,`batch_job_sub_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `dwh_dim_batch_job_sub_type` VALUES (0,0,'KALTURA_COM','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,1,'ON2','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,2,'FFMPEG','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,3,'MENCODER','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,4,'ENCODING_COM','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,5,'EXPRESSION_ENCODER3','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,98,'FFMPEG_VP8','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,99,'FFMPEG_AUX','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,201,'PDF2SWF','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,202,'PDF_CREATOR','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(0,10021,'quickTimeTools.QUICK_TIME_PLAYER_TOOLS','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,0,'KALTURA_COM','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,1,'ON2','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,2,'FFMPEG','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,3,'MENCODER','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,4,'ENCODING_COM','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,5,'EXPRESSION_ENCODER3','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,98,'FFMPEG_VP8','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,99,'FFMPEG_AUX','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,201,'PDF2SWF','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,202,'PDF_CREATOR','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(22,10021,'quickTimeTools.QUICK_TIME_PLAYER_TOOLS','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,100,'GENERAL_ALERT_TYPE_STF','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,10,'GENERAL_ALERT_TYPE_NEWSLETTER','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,11,'GENERAL_ALERT_TYPE_FAVORITED_ME','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,12,'GENERAL_ALERT_TYPE_FAVORITED_MY_CLIP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,13,'GENERAL_ALERT_TYPE_COMMENT_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,20,'KALTURAS_PRODUCED_ALERT_TYPE_CONTRIB_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,21,'KALTURAS_PRODUCED_ALERT_TYPE_SUBSCRIBER_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,22,'KALTURAS_PRODUCED_ALERT_TYPE_ROUGHCUT_CREATED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,23,'KALTURAS_PRODUCED_ALERT_TYPE_FAVORITED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,24,'KALTURAS_PRODUCED_ALERT_TYPE_COMMENT_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,30,'KALTURAS_PARTOF_ALERT_TYPE_CONTRIB_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,31,'KALTURAS_PARTOF_ALERT_TYPE_ROUGHCUT_CREATED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,32,'KALTURAS_PARTOF_ALERT_TYPE_FAVORITED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,33,'KALTURAS_PARTOF_ALERT_TYPE_COMMENT_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,40,'KALTURAS_SUBSCRIBEDTO_ALERT_TYPE_CONTRIB_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,41,'KALTURAS_SUBSCRIBEDTO_ALERT_TYPE_ROUGHCUT_CREATED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,42,'KALTURAS_SUBSCRIBEDTO_ALERT_TYPE_FAVORITED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,43,'KALTURAS_SUBSCRIBEDTO_ALERT_TYPE_COMMENT_ADDED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,50,'KALTURAS_CMS_REGISTRATION_CONFIRMATION','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,54,'KALTURAS_DEFAULT_REGISTRATION_CONFIRMATION','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,51,'KALTURAS_CMS_PASSWORD_RESET','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,52,'KALTURAS_PARTNER_EMAIL_CHANGE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,55,'KALTURAS_EXISTING_USER_REGISTRATION_CONFIRMATION','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,56,'KALTURAS_DEFAULT_EXISTING_USER_REGISTRATION_CONFIRMATION','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,60,'KALTURAS_FLATTEN_READY','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,62,'KALTURAS_DOWNLOAD_READY','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,63,'KALTURAS_BULKDOWNLOAD_READY','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,80,'KALTURA_PACKAGE_UPGRADE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,81,'KALTURA_PACKAGE_EIGHTY_PERCENT_WARNING','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,82,'KALTURA_PACKAGE_LIMIT_WARNING_1','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,83,'KALTURA_PACKAGE_LIMIT_WARNING_2','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,84,'KALTURA_DELETE_ACCOUNT','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,90,'KALTURA_BATCH_ALERT','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,110,'SYSTEM_USER_RESET_PASSWORD','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,111,'SYSTEM_USER_RESET_PASSWORD_SUCCESS','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,112,'SYSTEM_USER_NEW_PASSWORD','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,113,'SYSTEM_USER_CREDENTIALS_SAVED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,120,'KALTURA_NEW_USER_EMAIL','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,121,'KALTURA_NEW_EXISTING_USER_EMAIL','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,122,'KALTURA_NEW_USER_EMAIL_TO_ADMINS','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(15,210,'KMC_SUPPORT_FORM','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,1,'ENTRY_ADD','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,2,'ENTR_UPDATE_PERMISSIONS','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,3,'ENTRY_DELETE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,4,'ENTRY_BLOCK','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,5,'ENTRY_UPDATE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,6,'ENTRY_UPDATE_THUMBNAIL','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,7,'ENTRY_UPDATE_MODERATION','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,21,'USER_ADD','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(16,26,'USER_BANNED','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(21,1,'FILE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(21,2,'WEBCAM','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(21,5,'URL','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(21,6,'SEARCH_PROVIDER','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(21,29,'AKAMAI_LIVE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(24,1,'FILE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(24,2,'WEBCAM','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(24,5,'URL','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(24,6,'SEARCH_PROVIDER','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(24,29,'AKAMAI_LIVE','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(23,0,'Kaltura','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(23,1,'FTP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(23,2,'SCP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(23,3,'SFTP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(23,4,'HTTP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(23,5,'HTTPS','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(25,0,'Kaltura','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(25,1,'FTP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(25,2,'SCP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(25,3,'SFTP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(25,4,'HTTP','2011-09-18 20:04:32','0000-00-00 00:00:00',0),(25,5,'HTTPS','2011-09-18 20:04:32','0000-00-00 00:00:00',0);

CREATE TABLE `dwh_dim_batch_job_type` (
  `batch_job_type_id` int(11) NOT NULL,
  `batch_job_type_name` varchar(100) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `dwh_update_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `ri_ind` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`batch_job_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

ALTER TABLE dwh_dim_container_format
CHANGE `container_format` `container_format` VARCHAR(333)  DEFAULT NULL,
ADD UNIQUE KEY `container_format` (`container_format`);

ALTER TABLE dwh_dim_conversion_profile
ADD KEY `dwh_update_date` (`dwh_update_date`);

ALTER TABLE dwh_dim_creation_mode
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_domain
ADD UNIQUE KEY `domain_name` (`domain_name`);

CREATE TABLE `dwh_dim_domain_referrer` (
  `referrer_id` int(11) NOT NULL AUTO_INCREMENT,
  `domain_id` int(11) DEFAULT NULL,
  `referrer` varchar(255) DEFAULT NULL,
  `dwh_insertion_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`referrer_id`),
  UNIQUE KEY `domain_id-referrer` (`domain_id`,`referrer`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

ALTER TABLE `dwh_dim_editor_type` 
CHANGE `editor_type_id` `editor_type_id` smallint(6) NOT NULL AUTO_INCREMENT,
ADD UNIQUE KEY `editor_type_name` (`editor_type_name`);

ALTER TABLE dwh_dim_entry_media_source
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_entry_media_type
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_entry_status
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_entry_type
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE `dwh_dim_file_ext`
CHANGE `file_ext` `file_ext` varchar(333) DEFAULT NULL,
ADD UNIQUE KEY `file_ext` (`file_ext`);

ALTER TABLE dwh_dim_file_sync
ADD KEY `dwh_update_date` (`dwh_update_date`);

ALTER TABLE dwh_dim_file_sync_object_type
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_file_sync_status
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_flavor_asset
ADD KEY `deleted_at` (`deleted_at`),
ADD KEY `dwh_update_date` (`dwh_update_date`),
ADD KEY `updated_at` (`updated_at`);

ALTER TABLE dwh_dim_flavor_format
CHANGE `flavor_format` `flavor_format` VARCHAR(333) DEFAULT NULL,
ADD UNIQUE KEY `flavor_format` (`flavor_format`);

ALTER TABLE dwh_dim_flavor_params
ADD KEY `dwh_update_date` (`dwh_update_date`);

ALTER TABLE dwh_dim_flavor_params_conversion_profile
ADD KEY `dwh_update_date` (`dwh_update_date`);

ALTER TABLE dwh_dim_flavor_params_output
ADD KEY `dwh_update_date` (`dwh_update_date`);

ALTER TABLE dwh_dim_fms_adaptor
ADD UNIQUE KEY `adaptor` (`adaptor`);

ALTER TABLE `dwh_dim_fms_app`
CHANGE `app_id` `fms_app_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
CHANGE `app` `fms_app_name` varchar(45) NOT NULL,
ADD COLUMN `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
ADD COLUMN `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
ADD COLUMN `ri_ind` tinyint(4) NOT NULL DEFAULT '1',
DROP PRIMARY KEY, ADD PRIMARY KEY (`fms_app_id`),
ADD UNIQUE KEY `fms_app_name` (`fms_app_name`);

ALTER TABLE dwh_dim_fms_app_instance
CHANGE `app_instance` `app_instance` VARCHAR(333) DEFAULT NULL,
ADD UNIQUE KEY `app_instance` (`app_instance`);

CREATE TABLE `dwh_dim_fms_bandwidth_source` (
  `process_id` int(10) NOT NULL,
  `fms_app_id` smallint(6) NOT NULL,
  `bandwidth_source_id` int(11) NOT NULL,
  `file_regex` varchar(100) NOT NULL DEFAULT '.*',
  UNIQUE KEY `process_id` (`process_id`,`fms_app_id`,`file_regex`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

INSERT INTO `dwh_dim_fms_bandwidth_source` VALUES (2,5,5,'.*'),(7,1,6,'_77658\\.|_86593\\.'),(7,1,7,'_105515\\.');

ALTER TABLE dwh_dim_fms_client_protocol
ADD UNIQUE KEY `client_protocol` (`client_protocol`);

ALTER TABLE dwh_dim_fms_event_category
ADD UNIQUE KEY `event_category` (`event_category`);

ALTER TABLE dwh_dim_fms_event_type
ADD UNIQUE KEY `event_type` (`event_type`);

ALTER TABLE dwh_dim_fms_stream_type
ADD UNIQUE KEY `stream_type` (`stream_type`);

ALTER TABLE dwh_dim_fms_virtual_host
ADD UNIQUE KEY `virtual_host` (`virtual_host`);

ALTER TABLE dwh_dim_kusers
ADD COLUMN `is_admin` tinyint(4) DEFAULT NULL;

ALTER TABLE dwh_dim_locations
CHANGE `country` `country` varchar(50) NOT NULL DEFAULT '',
CHANGE `state` `state` varchar(50) NOT NULL DEFAULT '',
CHANGE `city` `city` varchar(50) NOT NULL DEFAULT '',
ADD UNIQUE KEY `location_name` (`location_name`,`location_type_name`,`country`,`state`,`city`);

ALTER TABLE dwh_dim_media_info
ADD KEY `dwh_update_date` (`dwh_update_date`);

ALTER TABLE dwh_dim_moderation_status
ALTER `ri_ind` SET DEFAULT '1';

CREATE TABLE `dwh_dim_os` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `device` varchar(50) NOT NULL,
  `is_mobile` tinyint(1) NOT NULL,
  `manufacturer` varchar(50) NOT NULL,
  `group` varchar(50) NOT NULL,
  `os` varchar(50) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `os` (`os`,`device`,`is_mobile`,`manufacturer`,`group`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

CREATE TABLE `dwh_dim_partner_class_of_service` (
  `partner_class_of_service_id` smallint(6) NOT NULL,
  `partner_class_of_service_name` varchar(50) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`partner_class_of_service_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

ALTER TABLE dwh_dim_partner_group_type
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_partner_status
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE `dwh_dim_partner_type` 
ALTER `ri_ind` SET DEFAULT '1';

CREATE TABLE `dwh_dim_partner_vertical` (
  `partner_vertical_id` smallint(6) NOT NULL,
  `partner_vertical_name` varchar(50) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '1',
  PRIMARY KEY (`partner_vertical_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`etl`@`%`*/ /*!50003 TRIGGER `kalturadw`.`dwh_dim_partner_vertical_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_partner_vertical`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW() */;;
DELIMITER ;

ALTER TABLE dwh_dim_partners
ADD COLUMN `class_of_service_id` int(11) DEFAULT NULL,
ADD COLUMN `vertical_id` int(11) DEFAULT NULL,
ADD COLUMN internal_use BOOLEAN NOT NULL DEFAULT 0;

CREATE TABLE `dwh_dim_partners_billing` (
  `partner_id` int(11) NOT NULL,
  `partner_group_type_id` smallint(6) DEFAULT '1',
  `updated_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `max_monthly_bandwidth_kb` decimal(15,3) DEFAULT NULL,
  `charge_monthly_bandwidth_kb_usd` decimal(15,3) DEFAULT NULL,
  `charge_monthly_bandwidth_kb_unit` decimal(15,3) DEFAULT NULL,
  `max_monthly_storage_mb` decimal(15,3) DEFAULT NULL,
  `charge_monthly_storage_mb_usd` decimal(15,3) DEFAULT NULL,
  `charge_monthly_storage_mb_unit` decimal(15,3) DEFAULT NULL,
  `max_monthly_total_usage_mb` decimal(15,3) DEFAULT NULL,
  `charge_monthly_total_usage_mb_usd` decimal(15,3) DEFAULT NULL,
  `charge_monthly_total_usage_mb_unit` decimal(15,3) DEFAULT NULL,
  `max_monthly_entries` bigint(20) DEFAULT NULL,
  `charge_monthly_entries_usd` decimal(15,3) DEFAULT NULL,
  `charge_monthly_entries_unit` int(11) DEFAULT NULL,
  `max_monthly_plays` bigint(20) DEFAULT NULL,
  `charge_monthly_plays_usd` decimal(15,3) DEFAULT NULL,
  `charge_monthly_plays_unit` int(11) DEFAULT NULL,
  `max_kusers` bigint(20) DEFAULT NULL,
  `charge_kusers_usd` decimal(15,3) DEFAULT NULL,
  `charge_kusers_unit` int(11) DEFAULT NULL,
  `max_publishers` bigint(20) DEFAULT NULL,
  `charge_publishers_usd` decimal(15,3) DEFAULT NULL,
  `charge_publishers_unit` int(11) DEFAULT NULL,
  `max_end_users` bigint(20) DEFAULT NULL,
  `charge_end_users_usd` decimal(15,3) DEFAULT NULL,
  `charge_end_users_unit` int(11) DEFAULT NULL,
  `class_of_service_id` int(11) DEFAULT NULL,
  `vertical_id` int(11) DEFAULT NULL,
  `is_active` tinyint(4) DEFAULT '1',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`partner_id`,`updated_at`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

DELIMITER ;;
/*!50003 CREATE*/ /*!50017 DEFINER=`etl`@`%`*/ /*!50003 TRIGGER `kalturadw`.`dwh_dim_partners_billing_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_partners_billing`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW() */;;
DELIMITER ;

ALTER TABLE dwh_dim_ready_behavior
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_ui_conf_status
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_ui_conf_type
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE kalturadw.dwh_dim_ui_conf 
ADD COLUMN version varchar(60), 
ADD COLUMN swf_interface_id INT(11);

DROP TABLE IF EXISTS kalturadw.dwh_dim_ui_conf_swf_interfaces;
CREATE TABLE kalturadw.dwh_dim_ui_conf_swf_interfaces (
	id INT NOT NULL AUTO_INCREMENT,
	swf_file varchar(255) NOT NULL,
	tags_search_string varchar(255) NOT NULL DEFAULT '',
	display_name varchar(255),
	PRIMARY KEY (id),
	UNIQUE KEY (swf_file,tags_search_string)) ENGINE=MYISAM CHARSET=latin1;

TRUNCATE TABLE dwh_dim_ui_conf_swf_interfaces;
INSERT INTO dwh_dim_ui_conf_swf_interfaces (id, swf_file, tags_search_string, display_name) VALUES (-1,'','','Unknown');

INSERT INTO dwh_dim_ui_conf_swf_interfaces (swf_file, tags_search_string, display_name) 
VALUES ('ContributionWizard.swf', '', 'KCW (Kaltura Contribution Wizard)'),
 ('KUpload.swf', '', 'KSU (Kaltura Simple Uploader)'),
 ('KRecord.swf', '', 'KRecord (Webcam recorder)'),
 ('simpleeditor.swf', '', 'KSE (Kaltura Simple Editor)'),
 ('KalturaAdvancedVideoEditor.swf', '', 'KAE (Kaltura Advanced Editor)'),
 ('KClip.swf', '', 'Clipping tool'),
 ('kdp.swf', 'Playlist', 'KDP – single video player'),
 ('kdp.swf', 'Player', 'KDP – playlist player'),
 ('kdp3.swf', 'Playlist', 'KDP3 – single video player'),
 ('kdp3.swf', 'Player', 'KDP3 – playlist player');

UPDATE kalturadw.dwh_dim_ui_conf ui_conf LEFT OUTER JOIN kalturadw.dwh_dim_ui_conf_swf_interfaces swf_interfaces
ON (SUBSTRING_INDEX(ui_conf.swf_url, '/', -1) = swf_interfaces.swf_file AND tags LIKE CONCAT('%',tags_search_string,'%'))
SET ui_conf.VERSION = SUBSTRING_INDEX(SUBSTRING_INDEX(swf_url, '/', -2),'/',1),
ui_conf.swf_interface_id = IFNULL(swf_interfaces.id, -1);

ALTER TABLE dwh_dim_user_status
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_video_codec
CHANGE `video_codec` `video_codec` VARCHAR(333) DEFAULT NULL,
ADD UNIQUE KEY `video_codec` (`video_codec`);

ALTER TABLE dwh_dim_widget_security_policy
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_dim_widget_security_type
ALTER `ri_ind` SET DEFAULT '1';

ALTER TABLE dwh_fact_bandwidth_usage
DROP KEY `file_id`;

ALTER TABLE dwh_fact_bandwidth_usage
ADD COLUMN `line_number` int(11) DEFAULT NULL AFTER `file_id`,
ADD COLUMN `user_ip` varchar(15) DEFAULT NULL,
ADD COLUMN `user_ip_number` int(10) unsigned DEFAULT NULL,
ADD COLUMN `country_id` int(11) DEFAULT NULL,
ADD COLUMN `location_id` int(11) DEFAULT NULL,
ADD COLUMN `os_id` int(11) DEFAULT NULL,
ADD COLUMN `browser_id` int(11) DEFAULT NULL,
ADD UNIQUE KEY `file_id` (`file_id`,`line_number`,`activity_date_id`),
ENGINE=InnoDB;

CREATE TABLE `dwh_fact_bandwidth_usage_archive` (
  `file_id` int(11) NOT NULL,
  `line_number` int(11) DEFAULT NULL,
  `partner_id` int(11) NOT NULL DEFAULT '-1',
  `activity_date_id` int(11) DEFAULT '-1',
  `activity_hour_id` tinyint(4) DEFAULT '-1',
  `bandwidth_source_id` bigint(20) DEFAULT NULL,
  `url` varchar(2000) DEFAULT NULL,
  `bandwidth_bytes` bigint(20) DEFAULT '0',
  `user_ip` varchar(15) DEFAULT NULL,
  `user_ip_number` int(10) unsigned DEFAULT NULL,
  `country_id` int(11) DEFAULT NULL,
  `location_id` int(11) DEFAULT NULL,
  `os_id` int(11) DEFAULT NULL,
  `browser_id` int(11) DEFAULT NULL
) ENGINE=ARCHIVE DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (activity_date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = ARCHIVE) */;

ALTER TABLE dwh_fact_entries_sizes
DROP PRIMARY KEY,
ADD PRIMARY KEY (`partner_id`,`entry_id`,`entry_size_date_id`);

CREATE TABLE `dwh_fact_events_new` (
  `file_id` int(11) NOT NULL,
  `event_id` int(11) NOT NULL,
  `event_type_id` smallint(6) NOT NULL,
  `client_version` varchar(31) DEFAULT NULL,
  `event_time` datetime DEFAULT NULL,
  `event_date_id` int(11) NOT NULL DEFAULT '0',
  `event_hour_id` tinyint(4) DEFAULT NULL,
  `session_id` varchar(50) DEFAULT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `entry_id` varchar(20) DEFAULT NULL,
  `unique_viewer` varchar(40) DEFAULT NULL,
  `widget_id` varchar(31) DEFAULT NULL,
  `ui_conf_id` int(11) DEFAULT NULL,
  `uid` varchar(64) DEFAULT NULL,
  `current_point` int(11) DEFAULT NULL,
  `duration` int(11) DEFAULT NULL,
  `user_ip` varchar(15) DEFAULT NULL,
  `user_ip_number` int(10) unsigned DEFAULT NULL,
  `country_id` int(11) DEFAULT NULL,
  `location_id` int(11) DEFAULT NULL,
  `process_duration` int(11) DEFAULT NULL,
  `control_id` varchar(15) DEFAULT NULL,
  `seek` int(11) DEFAULT NULL,
  `new_point` int(11) DEFAULT NULL,
  `domain_id` int(11) DEFAULT NULL,
  `entry_media_type_id` int(11) DEFAULT NULL,
  `entry_partner_id` int(11) DEFAULT NULL,
  `referrer_id` int(11) DEFAULT NULL,
  `os_id` int(11) DEFAULT NULL,
  `browser_id` int(11) DEFAULT NULL,
  PRIMARY KEY (`file_id`,`event_id`,`event_date_id`),
  KEY `Entry_id` (`entry_id`),
  KEY `event_hour_id_event_date_id_partner_id` (`event_hour_id`,`event_date_id`,`partner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
 PARTITION BY RANGE (event_date_id)
(PARTITION p_20101231 VALUES LESS THAN (20110101) ENGINE = InnoDB);

CALL add_daily_partition_for_table('dwh_fact_events_new');

INSERT INTO dwh_fact_events_new
(
  `file_id` ,
  `event_id`,
  `event_type_id`,
  `client_version` ,
  `event_time` ,
  `event_date_id` ,
  `event_hour_id` ,
  `session_id` ,
  `partner_id` ,
  `entry_id` ,
  `unique_viewer`,
  `widget_id` ,
  `ui_conf_id` ,
  `uid` ,
  `current_point` ,
  `duration` ,
  `user_ip` ,
  `user_ip_number` ,
  `country_id` ,
  `location_id` ,
  `process_duration` ,
  `control_id` ,
  `seek` ,
  `new_point` ,
  `domain_id` ,
  `entry_media_type_id` ,
  `entry_partner_id` ,
  `referrer_id` 
 )
 SELECT `file_id` ,
  `event_id`,
  `event_type_id`,
  `client_version` ,
  `event_time` ,
  `event_date_id` ,
  `event_hour_id` ,
  `session_id` ,
  `partner_id` ,
  `entry_id` ,
  `unique_viewer`,
  `widget_id` ,
  `ui_conf_id` ,
  `uid` ,
  `current_point` ,
  `duration` ,
  `user_ip` ,
  `user_ip_number` ,
  `country_id` ,
  `location_id` ,
  `process_duration` ,
  `control_id` ,
  `seek` ,
  `new_point` ,
  `domain_id` ,
  `entry_media_type_id` ,
  `entry_partner_id` ,
  `referrer_id` 
FROM dwh_fact_events;

DROP TABLE dwh_fact_events;
RENAME TABLE kalturadw.dwh_fact_events_new TO kalturadw.dwh_fact_events;

CREATE TABLE `dwh_fact_events_archive` (
  `file_id` int(11) NOT NULL,
  `event_id` int(11) NOT NULL,
  `event_type_id` smallint(6) NOT NULL,
  `client_version` varchar(31) DEFAULT NULL,
  `event_time` datetime NOT NULL DEFAULT '0000-00-00 00:00:00',
  `event_date_id` int(11) NOT NULL DEFAULT '0',
  `event_hour_id` tinyint(4) DEFAULT NULL,
  `session_id` varchar(50) DEFAULT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `entry_id` varchar(20) DEFAULT NULL,
  `unique_viewer` varchar(40) DEFAULT NULL,
  `widget_id` varchar(31) DEFAULT NULL,
  `ui_conf_id` int(11) DEFAULT NULL,
  `uid` varchar(64) DEFAULT NULL,
  `current_point` int(11) DEFAULT NULL,
  `duration` int(11) DEFAULT NULL,
  `user_ip` varchar(15) DEFAULT NULL,
  `user_ip_number` int(10) unsigned DEFAULT NULL,
  `country_id` int(11) DEFAULT NULL,
  `location_id` int(11) DEFAULT NULL,
  `process_duration` int(11) DEFAULT NULL,
  `control_id` varchar(15) DEFAULT NULL,
  `seek` int(11) DEFAULT NULL,
  `new_point` int(11) DEFAULT NULL,
  `domain_id` int(11) DEFAULT NULL,
  `entry_media_type_id` int(11) DEFAULT NULL,
  `entry_partner_id` int(11) DEFAULT NULL,
  `referrer_id` int(11) DEFAULT NULL,
  `os_id` int(11) DEFAULT NULL,
  `browser_id` int(11) DEFAULT NULL
) ENGINE=ARCHIVE DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (event_date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = ARCHIVE) */;

DROP TABLE IF EXISTS `dwh_fact_events_v`;

CREATE TABLE `dwh_fact_fms_session_events_new` (
  `file_id` int(11) unsigned NOT NULL,
  `line_number` int(11) DEFAULT NULL,
  `event_type_id` tinyint(3) unsigned NOT NULL,
  `event_category_id` tinyint(3) unsigned NOT NULL,
  `event_time` datetime NOT NULL,
  `event_time_tz` varchar(3) NOT NULL,
  `event_date_id` int(11) NOT NULL,
  `event_hour_id` tinyint(3) NOT NULL,
  `context` varchar(100) DEFAULT NULL,
  `entry_id` varchar(20) DEFAULT NULL,
  `partner_id` int(10) DEFAULT NULL,
  `external_id` varchar(50) DEFAULT NULL,
  `server_ip` varchar(15) DEFAULT NULL,
  `server_ip_number` int(10) unsigned DEFAULT NULL,
  `server_process_id` int(10) unsigned NOT NULL,
  `server_cpu_load` smallint(5) unsigned NOT NULL,
  `server_memory_load` smallint(5) unsigned NOT NULL,
  `adaptor_id` smallint(5) unsigned NOT NULL,
  `virtual_host_id` smallint(5) unsigned NOT NULL,
  `fms_app_id` tinyint(3) unsigned NOT NULL,
  `app_instance_id` tinyint(3) unsigned NOT NULL,
  `duration_secs` int(10) unsigned NOT NULL,
  `status_id` smallint(3) unsigned DEFAULT NULL,
  `status_desc_id` tinyint(3) unsigned NOT NULL,
  `client_ip` varchar(15) NOT NULL,
  `client_ip_number` int(10) unsigned NOT NULL,
  `client_country_id` int(10) unsigned DEFAULT '0',
  `client_location_id` int(10) unsigned DEFAULT '0',
  `client_protocol_id` tinyint(3) unsigned NOT NULL,
  `uri` varchar(4000) NOT NULL,
  `uri_stem` varchar(2000) DEFAULT NULL,
  `uri_query` varchar(2000) DEFAULT NULL,
  `referrer` varchar(4000) DEFAULT NULL,
  `user_agent` varchar(2000) DEFAULT NULL,
  `session_id` varchar(20) NOT NULL,
  `client_to_server_bytes` bigint(20) unsigned NOT NULL,
  `server_to_client_bytes` bigint(20) unsigned NOT NULL,
  `stream_name` varchar(50) DEFAULT NULL,
  `stream_query` varchar(50) DEFAULT NULL,
  `stream_file_name` varchar(4000) DEFAULT NULL,
  `stream_type_id` tinyint(3) unsigned DEFAULT NULL,
  `stream_size_bytes` int(11) DEFAULT NULL,
  `stream_length_secs` int(11) DEFAULT NULL,
  `stream_position` int(11) DEFAULT NULL,
  `client_to_server_stream_bytes` int(10) unsigned DEFAULT NULL,
  `server_to_client_stream_bytes` int(10) unsigned DEFAULT NULL,
  `server_to_client_qos_bytes` int(10) unsigned DEFAULT NULL,
  UNIQUE KEY `file_id` (`file_id`,`line_number`,`event_date_id`),
  KEY `partner_id` (`partner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
 PARTITION BY RANGE (event_date_id)
(PARTITION p_20101231 VALUES LESS THAN (20110101) ENGINE = InnoDB);

CALL add_daily_partition_for_table('dwh_fact_fms_session_events_new');

INSERT INTO `dwh_fact_fms_session_events_new` (
  `file_id` ,
  `event_type_id` ,
  `event_category_id` ,
  `event_time` ,
  `event_time_tz` ,
  `event_date_id` ,
  `event_hour_id` ,
  `context` ,
  `entry_id` ,
  `partner_id` ,
  `external_id` ,
  `server_ip_number` ,
  `server_process_id` ,
  `server_cpu_load` ,
  `server_memory_load` ,
  `adaptor_id` ,
  `virtual_host_id` ,
  `fms_app_id` ,
  `app_instance_id` ,
  `duration_secs` ,
  `status_id` ,
  `status_desc_id` ,
  `client_ip` ,
  `client_ip_number` ,
  `client_country_id` ,
  `client_location_id` ,
  `client_protocol_id`,
  `uri` ,
  `uri_stem` ,
  `uri_query` ,
  `referrer` ,
  `user_agent`,
  `session_id` ,
  `client_to_server_bytes` ,
  `server_to_client_bytes` ,
  `stream_name` ,
  `stream_query` ,
  `stream_file_name` ,
  `stream_type_id` ,
  `stream_size_bytes` ,
  `stream_length_secs` ,
  `stream_position` ,
  `client_to_server_stream_bytes` ,
  `server_to_client_stream_bytes` ,
  `server_to_client_qos_bytes` )
SELECT    `file_id` ,
  `event_type_id` ,
  `event_category_id` ,
  `event_time` ,
  `event_time_tz` ,
  `event_date_id` ,
  `event_hour_id` ,
  `context` ,
  `entry_id` ,
  `partner_id` ,
  `external_id` ,
  `server_ip` ,
  `server_process_id` ,
  `server_cpu_load` ,
  `server_memory_load` ,
  `adaptor_id` ,
  `virtual_host_id` ,
  `app_id` ,
  `app_instance_id` ,
  `duration_secs` ,
  `status_id` ,
  `status_desc_id` ,
  `client_ip_str` ,
  `client_ip` ,
  `client_country_id` ,
  `client_location_id` ,
  `client_protocol_id`,
  `uri` ,
  `uri_stem` ,
  `uri_query` ,
  `referrer` ,
  `user_agent`,
  `session_id` ,
  `client_to_server_bytes` ,
  `server_to_client_bytes` ,
  `stream_name` ,
  `stream_query` ,
  `stream_file_name` ,
  `stream_type_id` ,
  `stream_size_bytes` ,
  `stream_length_secs` ,
  `stream_position` ,
  `client_to_server_stream_bytes` ,
  `server_to_client_stream_bytes` ,
  `server_to_client_qos_bytes`
 FROM dwh_fact_fms_session_events;

DROP TABLE dwh_fact_fms_session_events;
RENAME TABLE kalturadw.dwh_fact_fms_session_events_new TO kalturadw.dwh_fact_fms_session_events;

CREATE TABLE `dwh_fact_fms_session_events_archive` (
  `file_id` int(11) unsigned NOT NULL,
  `line_number` int(11) DEFAULT NULL,
  `event_type_id` tinyint(3) unsigned NOT NULL,
  `event_category_id` tinyint(3) unsigned NOT NULL,
  `event_time` datetime NOT NULL,
  `event_time_tz` varchar(3) NOT NULL,
  `event_date_id` int(11) NOT NULL,
  `event_hour_id` tinyint(3) NOT NULL,
  `context` varchar(1024) DEFAULT NULL,
  `entry_id` varchar(20) DEFAULT NULL,
  `partner_id` int(10) DEFAULT NULL,
  `external_id` varchar(50) DEFAULT NULL,
  `server_ip` varchar(15) DEFAULT NULL,
  `server_ip_number` int(10) unsigned DEFAULT NULL,
  `server_process_id` int(10) unsigned NOT NULL,
  `server_cpu_load` smallint(5) unsigned NOT NULL,
  `server_memory_load` smallint(5) unsigned NOT NULL,
  `adaptor_id` smallint(5) unsigned NOT NULL,
  `virtual_host_id` smallint(5) unsigned NOT NULL,
  `fms_app_id` tinyint(3) DEFAULT NULL,
  `app_instance_id` tinyint(3) unsigned NOT NULL,
  `duration_secs` int(10) unsigned NOT NULL,
  `status_id` smallint(3) unsigned DEFAULT NULL,
  `status_desc_id` tinyint(3) unsigned NOT NULL,
  `client_ip` varchar(15) NOT NULL,
  `client_ip_number` int(10) unsigned NOT NULL,
  `client_country_id` int(10) unsigned DEFAULT '0',
  `client_location_id` int(10) unsigned DEFAULT '0',
  `client_protocol_id` tinyint(3) unsigned NOT NULL,
  `uri` varchar(4000) NOT NULL,
  `uri_stem` varchar(2000) DEFAULT NULL,
  `uri_query` varchar(2000) DEFAULT NULL,
  `referrer` varchar(4000) DEFAULT NULL,
  `user_agent` varchar(2000) DEFAULT NULL,
  `session_id` varchar(20) NOT NULL,
  `client_to_server_bytes` bigint(20) unsigned NOT NULL,
  `server_to_client_bytes` bigint(20) unsigned NOT NULL,
  `stream_name` varchar(1024) DEFAULT NULL,
  `stream_query` varchar(1024) DEFAULT NULL,
  `stream_file_name` varchar(4000) DEFAULT NULL,
  `stream_type_id` tinyint(3) unsigned DEFAULT NULL,
  `stream_size_bytes` int(11) DEFAULT NULL,
  `stream_length_secs` int(11) DEFAULT NULL,
  `stream_position` int(11) DEFAULT NULL,
  `client_to_server_stream_bytes` int(10) unsigned DEFAULT NULL,
  `server_to_client_stream_bytes` int(10) unsigned DEFAULT NULL,
  `server_to_client_qos_bytes` int(10) unsigned DEFAULT NULL
) ENGINE=ARCHIVE DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (event_date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = ARCHIVE) */;

CREATE TABLE `dwh_fact_fms_sessions_new` (
  `session_id` varchar(20) NOT NULL,
  `session_time` datetime NOT NULL,
  `session_date_id` int(11) unsigned DEFAULT NULL,
  `bandwidth_source_id` int(11) NOT NULL,
  `session_client_ip` varchar(15) DEFAULT NULL,
  `session_client_ip_number` int(10) unsigned DEFAULT NULL,
  `session_client_country_id` int(10) unsigned DEFAULT NULL,
  `session_client_location_id` int(10) unsigned DEFAULT NULL,
  `session_partner_id` int(10) unsigned DEFAULT NULL,
  `total_bytes` bigint(20) unsigned DEFAULT NULL,
  UNIQUE KEY `session_id` (`session_id`,`session_date_id`),
  KEY `session_partner_id` (`session_partner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1
PARTITION BY RANGE (session_date_id)
(PARTITION p_20101231 VALUES LESS THAN (20110101) ENGINE = InnoDB);

CALL add_daily_partition_for_table('dwh_fact_fms_sessions_new');

INSERT INTO dwh_fact_fms_sessions_new(
`session_id`,
  `session_time` ,
  `session_date_id` ,
  `session_partner_id`,
  `total_bytes` )
  SELECT `session_id`,
  `session_time` ,
  `session_date_id` ,
  `session_partner_id`,
  `total_bytes`
  FROM dwh_fact_fms_sessions;
  
DROP TABLE dwh_fact_fms_sessions;
RENAME TABLE kalturadw.dwh_fact_fms_sessions_new TO kalturadw.dwh_fact_fms_sessions;

CREATE TABLE `dwh_fact_fms_sessions_archive` (
  `session_id` varchar(20) NOT NULL,
  `session_time` datetime NOT NULL,
  `session_date_id` int(11) unsigned DEFAULT NULL,
  `bandwidth_source_id` int(11) NOT NULL,
  `session_client_ip` varchar(15) DEFAULT NULL,
  `session_client_ip_number` int(10) unsigned DEFAULT NULL,
  `session_client_country_id` int(10) unsigned DEFAULT NULL,
  `session_client_location_id` int(10) unsigned DEFAULT NULL,
  `session_partner_id` int(10) unsigned DEFAULT NULL,
  `total_bytes` bigint(20) unsigned DEFAULT NULL
) ENGINE=ARCHIVE DEFAULT CHARSET=latin1
/*!50100 PARTITION BY RANGE (session_date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = ARCHIVE) */;
  
DROP TABLE IF EXISTS `dwh_fact_partner_activities`;
DROP TABLE IF EXISTS `dwh_fact_partner_activities_v`;

ALTER TABLE dwh_hourly_events_country
DROP KEY `country_id`,
DROP KEY `date_id`,
ENGINE=InnoDB;

ALTER TABLE dwh_hourly_events_domain
DROP KEY `domain_id`,
DROP KEY `date_id`,
ENGINE=InnoDB;

ALTER TABLE dwh_hourly_events_domain_referrer
DROP PRIMARY KEY,
ADD PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`domain_id`,`referrer_id`),
ENGINE=InnoDB;

ALTER TABLE dwh_hourly_events_entry
DROP KEY `entry_id`,
DROP KEY `date_id`,
ADD KEY `entry_id` (`entry_id`),
ENGINE=InnoDB;

ALTER TABLE dwh_hourly_events_uid
DROP KEY `uid`,
DROP KEY `date_id`,
ENGINE=InnoDB;

ALTER TABLE dwh_hourly_events_widget
DROP KEY `widget_id`,
DROP KEY `date_id`,
ENGINE=InnoDB;

CREATE TABLE `dwh_hourly_partner_usage` (
  `partner_id` int(11) NOT NULL,
  `date_id` int(11) NOT NULL,
  `hour_id` int(11) NOT NULL,
  `bandwidth_source_id` int(11) NOT NULL,
  `count_bandwidth_kb` decimal(19,4) DEFAULT '0.0000',
  `count_storage_mb` decimal(19,4) DEFAULT '0.0000',
  `aggr_storage_mb` decimal(19,4) DEFAULT NULL,
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`bandwidth_source_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = InnoDB) */;

DELETE FROM `ri_defaults`;
INSERT INTO `ri_defaults` VALUES ('dwh_dim_domains','domain_name','CONCAT(a.domain_id, \"-Missing Value\")'),('dwh_dim_kusers','screen_name','\"Missing Value\"'),('dwh_dim_kusers','full_name','\"Missing Value\"'),('dwh_dim_kusers','email','\"Missing Value\"'),('dwh_dim_kusers','location_id','\"-1\"'),('dwh_dim_kusers','country_id','\"-1\"'),('dwh_dim_kusers','gender_id','\"-1\"'),('dwh_dim_kusers','Kuser_status_id','\"-1\"'),('dwh_dim_kusers','created_at','\"2099-01-01 00:00:00\"'),('dwh_dim_kusers','created_date_id','\"-1\"'),('dwh_dim_kusers','created_hour_id','\"-1\"'),('dwh_dim_kusers','updated_at','\"2099-01-01 00:00:00\"'),('dwh_dim_kusers','updated_date_id','\"-1\"'),('dwh_dim_kusers','updated_hour_id','\"-1\"'),('dwh_dim_kusers','partner_id','\"-1\"'),('dwh_dim_entries','kuser_id','\"-1\"'),('dwh_dim_entries','entry_type_id','\"-1\"'),('dwh_dim_entries','entry_Media_type_id','\"-1\"'),('dwh_dim_entries','views','\"0\"'),('dwh_dim_entries','votes','\"0\"'),('dwh_dim_entries','comments','\"0\"'),('dwh_dim_entries','favorites','\"0\"'),('dwh_dim_entries','total_rank','\"0\"'),('dwh_dim_entries','rank','\"0\"'),('dwh_dim_entries','entry_status_id','\"-1\"'),('dwh_dim_entries','entry_media_source_id','\"-1\"'),('dwh_dim_entries','entry_source_id','\"-1\"'),('dwh_dim_entries','entry_license_type_id','\"-1\"'),('dwh_dim_entries','created_at','\"2099-01-01 00:00:00\"'),('dwh_dim_entries','created_date_id','\"-1\"'),('dwh_dim_entries','created_hour_id','\"-1\"'),('dwh_dim_entries','updated_at','\"2099-01-01 00:00:00\"'),('dwh_dim_entries','updated_date_id','\"-1\"'),('dwh_dim_entries','updated_hour_id','\"-1\"'),('dwh_dim_entries','partner_id','\"-1\"'),('dwh_dim_entries','subp_id','\"-1\"'),('dwh_dim_entries','int_id','\"-1\"'),('dwh_dim_entries','moderation_status','\"-1\"'),('dwh_dim_entries','modified_at','\"2099-01-01 00:00:00\"'),('dwh_dim_entries','modified_date_id','\"-1\"'),('dwh_dim_entries','modified_hour_id','\"-1\"'),('dwh_dim_partners','partner_name','\"Missing Value\"'),('dwh_dim_partners','created_at','\"2099-01-01 00:00:00\"'),('dwh_dim_partners','created_date_id','\"-1\"'),('dwh_dim_partners','created_hour_id','\"-1\"'),('dwh_dim_partners','updated_at','\"2099-01-01 00:00:00\"'),('dwh_dim_partners','updated_date_id','\"-1\"'),('dwh_dim_partners','updated_hour_id','\"-1\"'),('dwh_dim_partners','anonymous_kuser_id','\"-1\"'),('dwh_dim_partners','admin_name','\"Missing Value\"'),('dwh_dim_partners','admin_email','\"Missing Value\"'),('dwh_dim_partners','description','\"Missing Value\"'),('dwh_dim_partners','commercial_use','\"-1\"'),('dwh_dim_partners','moderate_content','\"-1\"'),('dwh_dim_partners','notify','\"-1\"'),('dwh_dim_partners','partner_status_id','\"-1\"'),('dwh_dim_partners','partner_status_name','\"Missing Value\"'),('dwh_dim_partners','content_categories','\"Missing Value\"'),('dwh_dim_partners','partner_type_id','\"-1\"'),('dwh_dim_partners','partner_type_name','\"Missing Value\"'),('dwh_dim_partners','adult_content','\"-1\"'),('dwh_dim_partners','partner_package','\"-1\"'),('dwh_dim_ui_conf','ui_conf_type_id','\"-1\"'),('dwh_dim_ui_conf','partner_id','\"-1\"'),('dwh_dim_ui_conf','subp_id','\"-1\"'),('dwh_dim_ui_conf','created_at','\"2099-01-01 00:00:00\"'),('dwh_dim_ui_conf','created_date_id','\"-1\"'),('dwh_dim_ui_conf','created_hour_id','\"-1\"'),('dwh_dim_ui_conf','updated_at','\"2099-01-01 00:00:00\"'),('dwh_dim_ui_conf','updated_date_id','\"-1\"'),('dwh_dim_ui_conf','updated_hour_id','\"-1\"'),('dwh_dim_ui_conf','ui_conf_status_id','\"-1\"'),('dwh_dim_widget','partner_id','\"-1\"'),('dwh_dim_widget','subp_id','\"-1\"'),('dwh_dim_widget','kshow_id','\"-1\"'),('dwh_dim_widget','entry_id','\"-1\"'),('dwh_dim_widget','ui_conf_id','\"-1\"'),('dwh_dim_widget','security_type','\"-1\"'),('dwh_dim_widget','security_policy','\"-1\"'),('dwh_dim_widget','created_at','\"2099-01-01 00:00:00\"'),('dwh_dim_widget','created_date_id','\"-1\"'),('dwh_dim_widget','created_hour_id','\"-1\"'),('dwh_dim_widget','updated_at','\"2099-01-01 00:00:00\"'),('dwh_dim_widget','updated_date_id','\"-1\"'),('dwh_dim_widget','updated_hour_id','\"-1\"'),('dwh_dim_widget_security_policy','widget_security_policy_name','\"Missing Value\"'),('dwh_dim_widget','widget_int_id','\"-1\"'),('dwh_dim_asset_status','asset_status_name\"','Missing Value\"'),('dwh_dim_flavor_pararms','version','\"-1\"'),('dwh_dim_flavor_pararms','partner_id','\"-1\"'),('dwh_dim_flavor_pararms','name','\"Missing Value\"'),('dwh_dim_flavor_pararms','tags','\"Missing Value\"'),('dwh_dim_flavor_pararms','description','\"Missing Value\"'),('dwh_dim_flavor_pararms','ready_behavior','\"-2\"'),('dwh_dim_flavor_pararms','is_default','\"-1\"'),('dwh_dim_flavor_pararms','formot','\"Missing Value\"'),('dwh_dim_flavor_pararms','video_codec','\"Missing Value\"'),('dwh_dim_flavor_pararms','video_bitrate','\"-1\"'),('dwh_dim_flavor_pararms','audio_codec','\"Missing Value\"'),('dwh_dim_flavor_pararms','audio_bitrate','\"Missing Value\"'),('dwh_dim_flavor_pararms','audio_channels','\"Missing Value\"'),('dwh_dim_flavor_pararms','audio_sample_rate','\"-1\"'),('dwh_dim_flavor_pararms','audio_resolution','\"-1\"'),('dwh_dim_flavor_pararms','width','\"-1\"'),('dwh_dim_flavor_pararms','height','\"-1\"'),('dwh_dim_flavor_pararms','frame_rate','\"-1\"'),('dwh_dim_flavor_pararms','gop_size','\"-1\"'),('dwh_dim_flavor_pararms','two_pass','\"-1\"'),('dwh_dim_flavor_pararms','conversion_engines','\"Missing Value\"'),('dwh_dim_flavor_pararms','conversion_engines_extra_params','\"Missing Value\"'),('dwh_dim_flavor_pararms','view_order','\"-1\"'),('dwh_dim_flavor_pararms','bypass_by_extension','\"Missing Value\"'),('dwh_dim_flavor_pararms','creation_mode','\"-1\"'),('dwh_dim_flavor_pararms','deinterlice','\"-1\"'),('dwh_dim_flavor_pararms','rotate','\"-1\"'),('dwh_dim_flavor_pararms','engine_version','\"-1\"'),('dwh_dim_flavor_pararms','created_at','\"2099-01-01 00:00:00\"'),('dwh_dim_flavor_pararms','updated_at','\"2099-01-01 00:00:00\"'),('dwh_dim_flavor_pararms','deleted_at','\"2099-01-01 00:00:00\"'),('dwh_dim_conversion_profile','partner_id','\"-1\"'),('dwh_dim_conversion_profile','name\"','Missing Value\"'),('dwh_dim_conversion_profile','created_at\"','2099-01-01 00:00:00\"'),('dwh_dim_conversion_profile','updated_at\"','2099-01-01 00:00:00\"'),('dwh_dim_conversion_profile','deleted_at\"','2099-01-01 00:00:00\"'),('dwh_dim_conversion_profile','clip_start','\"-1\"'),('dwh_dim_conversion_profile','clip_duration','\"-1\"'),('dwh_dim_conversion_profile','creation_mode','\"-1\"'),('dwh_dim_flavor_asset','int_id','\"-1\"'),('dwh_dim_flavor_asset','partner_id','\"-1\"'),('dwh_dim_flavor_asset','entry_id','\"-1\"'),('dwh_dim_flavor_asset','flavor_params_id','\"-1\"'),('dwh_dim_flavor_asset','status','\"-2\"'),('dwh_dim_flavor_asset','VERSION','\"-1\"'),('dwh_dim_flavor_asset','width','\"-1\"'),('dwh_dim_flavor_asset','height','\"-1\"'),('dwh_dim_flavor_asset','bitrate','\"-1\"'),('dwh_dim_flavor_asset','frame_rate','\"-1\"'),('dwh_dim_flavor_asset','size','\"-1\"'),('dwh_dim_flavor_asset','is_original','\"-1\"'),('dwh_dim_flavor_asset','created_at','\"2099-01-01 00:00:00\"'),('dwh_dim_flavor_asset','updated_at','\"2099-01-01 00:00:00\"'),('dwh_dim_flavor_asset','deleted_at','\"2099-01-01 00:00:00\"'),('dwh_dim_referrer','referrer','CONCAT(a.referrer_id, \"-Missing Value\")'),('dwh_dim_locations','location_type_name','\"Missing Value\"'),('dwh_dim_locations','location_name','CONCAT(a.location_id, \" - Missing Value\")'),('dwh_dim_locations','country','\"Missing Value\"'),('dwh_dim_locations','country_id','\"-1\"'),('dwh_dim_locations','country_name','\"Missing Value\"'),('dwh_dim_locations','region','\"Missing Value\"'),('dwh_dim_locations','region_id','\"-1\"'),('dwh_dim_locations','state','\"Missing Value\"'),('dwh_dim_locations','state_id','\"-1\"'),('dwh_dim_locations','city','\"Missing Value\"'),('dwh_dim_locations','dwh_creation_date','\"2099-01-01 00:00:00\"'),('dwh_dim_locations','dwh_update_date','\"2099-01-01 00:00:00\"');

DROP TABLE IF EXISTS `ri_mapping`;
CREATE TABLE `ri_mapping` (
  `table_name` varchar(300) DEFAULT NULL,
  `column_name` varchar(300) DEFAULT NULL,
  `date_id_column_name` varchar(300) DEFAULT NULL,
  `date_column_name` varchar(50) DEFAULT NULL,
  `reference_table` varchar(300) DEFAULT NULL,
  `reference_column` varchar(300) DEFAULT NULL,
  `perform_check` tinyint(1) DEFAULT NULL,
  UNIQUE KEY `table_name` (`table_name`,`column_name`)
) ENGINE=MyISAM DEFAULT CHARSET=latin1;

INSERT INTO `ri_mapping` VALUES ('dwh_dim_kusers','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_ui_conf','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_widget','source_widget_id','','dwh_update_date','dwh_dim_widget','widget_id',1),('dwh_dim_widget','root_widget_id','','dwh_update_date','dwh_dim_widget','widget_id',1),('dwh_dim_widget','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_widget','entry_id','','dwh_update_date','dwh_dim_entries','entry_id',1),('dwh_dim_widget','ui_conf_id','','dwh_update_date','dwh_dim_ui_conf','ui_conf_id',1),('dwh_dim_entries','kuser_id','','dwh_update_date','dwh_dim_kusers','kuser_id',1),('dwh_dim_entries','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_partners','partner_parent_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_fact_events','event_type_id','event_date_id','','dwh_dim_event_type','event_type_id',0),('dwh_dim_file_sync','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_flavor_asset','entry_id','','dwh_update_date','dwh_dim_entries','entry_id',1),('dwh_dim_flavor_asset','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_flavor_asset','flavor_params_id','','dwh_update_date','dwh_dim_flavor_params','id',1),('dwh_dim_flavor_params','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_conversion_profile','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_dim_flavor_params_conversion_profile','conversion_profile_id','','dwh_update_date','dwh_dim_conversion_profile','id',1),('dwh_dim_flavor_params_conversion_profile','flavor_params_id','','dwh_update_date','dwh_dim_flavor_params','id',1),('dwh_dim_flavor_params_output','entry_id','','dwh_update_date','dwh_dim_entries','entry_id',1),('dwh_dim_flavor_params_output','flavor_asset_id','','dwh_update_date','dwh_dim_flavor_asset','id',1),('dwh_dim_media_info','flavor_asset_id','','dwh_update_date','dwh_dim_flavor_asset','id',1),('dwh_dim_batch_job','partner_id','','dwh_update_date','dwh_dim_partners','partner_id',1),('dwh_hourly_partner','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_partner_usage','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_events_entry','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_events_entry','entry_id','date_id','','dwh_dim_entries','entry_id',1),('dwh_hourly_events_domain','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_events_domain','domain_id','date_id','','dwh_dim_domain','domain_id',1),('dwh_hourly_events_domain_referrer','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_events_domain_referrer','domain_id','date_id','','dwh_dim_domain','domain_id',1),('dwh_hourly_events_domain_referrer','referrer_id','date_id','','dwh_dwh_referrer','referrer_id',1),('dwh_dim_locations','country_id','','dwh_update_date','dwh_dim_locations','location_id',1),('dwh_dim_locations','state_id','','dwh_update_date','dwh_dim_locations','location_id',1),('dwh_hourly_events_country','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_events_country','country_id','date_id','','dwh_dim_locations','location_id',1),('dwh_hourly_events_country','location_id','date_id','','dwh_dim_locations','location_id',1),('dwh_hourly_events_uid','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_events_uid','kuser_id','date_id','','dwh_dim_kusers','kuser_id',1),('dwh_hourly_events_widget','partner_id','date_id','','dwh_dim_partners','partner_id',1),('dwh_hourly_events_widget','widget_id','date_id','','dwh_dim_widget','widget_id',1);

USE `kalturadw`;

DROP TABLE IF EXISTS kalturadw.`dwh_dim_entry_type_display`;

CREATE TABLE kalturadw.`dwh_dim_entry_type_display` 
(
    entry_type_id SMALLINT NOT NULL,
    entry_media_type_id SMALLINT NOT NULL,
    display varchar(256) default null,
    dwh_creation_date TIMESTAMP NOT NULL DEFAULT 0,
    dwh_update_date TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
    UNIQUE KEY (entry_type_id, entry_media_type_id)
);

CREATE TRIGGER `kalturadw`.`dwh_dim_entry_type_display_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_entry_type_display`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();
    
DELIMITER $$

USE `kalturadw`$$

DROP VIEW IF EXISTS `dwh_view_entry_type_display`$$

CREATE VIEW `kalturadw`.`dwh_view_entry_type_display` AS 
SELECT
  t.entry_type_id, t.entry_type_name, m.entry_media_type_id, m.entry_media_type_name,
    ifnull(d.display, concat(t.entry_type_name,'-',m.entry_media_type_name)) as display
FROM dwh_dim_entry_type_display d,
dwh_dim_entry_type t,
dwh_dim_entry_media_type m
WHERE t.entry_type_id = d.entry_type_id AND m.entry_media_type_id = d.entry_media_type_id
$$

DELIMITER ;

USE kalturadw;

INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('1','1','Video');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('1','2','Image');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('1','5','Audio');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('1','101','Generic Media');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('2','6','Mix');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('5','3','Manual Playlist');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('5','10','Dynamic Playlist');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('6','-1','Data');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('7','201','Flash live stream');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('7','202','Windows media live stream');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('7','203','Real media live stream');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('7','204','Quicktime live stream');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('10','11','Document');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('10','12','SWF Document');
INSERT INTO  dwh_dim_entry_type_display  ( entry_type_id ,  entry_media_type_id ,  display ) VALUES('10','13','PDF Document');

INSERT IGNORE INTO `dwh_dim_entry_type_display` (entry_type_id , entry_media_type_id) SELECT DISTINCT entry_type_id, entry_media_type_id FROM dwh_dim_entries;

drop table if exists `dwh_dim_tags`;

create table `dwh_dim_tags` (
	`tag_id` int(11) NOT NULL AUTO_INCREMENT,
    `tag_name` varchar(50) NOT NULL,
	`dwh_creation_date` TIMESTAMP  NOT NULL DEFAULT '0000-00-00 00:00:00',
	`dwh_update_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`tag_id`), UNIQUE KEY (tag_name)
) ENGINE=MYISAM; 

CREATE TRIGGER `kalturadw`.`dwh_dim_tags_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_tags`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();
    
use kalturadw;

drop table if exists `dwh_dim_flavor_asset_tags`;

create table `dwh_dim_flavor_asset_tags` (
	`flavor_asset_id` varchar(60) NOT NULL,
        `tag_id` int(11) NOT NULL,
	`updated_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	`ri_ind` TINYINT(4)  NOT NULL DEFAULT 0 ,
	UNIQUE (`flavor_asset_id`, `tag_id`)
) ENGINE=MYISAM; 

USE kalturadw;

DROP TABLE IF EXISTS dwh_dim_flavor_asset_new; 

CREATE TABLE `dwh_dim_flavor_asset_new` (
  `id` VARCHAR(60) NOT NULL DEFAULT '',
  `int_id` INT(11) DEFAULT NULL,
  `partner_id` INT(11) DEFAULT NULL,
  `tags` BLOB,
  `created_at` DATETIME DEFAULT NULL,
  `updated_at` DATETIME DEFAULT NULL,
  `deleted_at` DATETIME DEFAULT NULL,
  `entry_id` VARCHAR(60) DEFAULT NULL,
  `flavor_params_id` INT(11) DEFAULT NULL,
  `status` TINYINT(4) DEFAULT NULL,
  `version` VARCHAR(60) NOT NULL,
  `description` VARCHAR(765) DEFAULT NULL,
  `width` INT(11) DEFAULT NULL,
  `height` INT(11) DEFAULT NULL,
  `bitrate` INT(11) DEFAULT NULL,
  `frame_rate` FLOAT DEFAULT NULL,
  `size` INT(11) DEFAULT NULL,
  `is_original` INT(11) DEFAULT NULL,
  `file_ext_id` INT(11) DEFAULT NULL,
  `container_format_id` INT(11) DEFAULT NULL,
  `video_codec_id` INT(11) DEFAULT NULL,
  `dwh_creation_date` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ri_ind` TINYINT(4) NOT NULL DEFAULT '0',
  PRIMARY KEY `id` (`id`),
  KEY `deleted_at` (`deleted_at`),
  KEY `dwh_update_date` (`dwh_update_date`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

INSERT IGNORE INTO kalturadw.dwh_dim_flavor_asset_new
SELECT 	id, int_id, partner_id, tags, created_at, 
	updated_at, deleted_at, entry_id, flavor_params_id, 
	STATUS, VERSION, description, width, height, 
	bitrate, frame_rate, size, is_original, file_ext_id, 
	container_format_id, video_codec_id, dwh_creation_date, 
	dwh_update_date, ri_ind	 FROM 
	kalturadw.dwh_dim_flavor_asset 
WHERE ri_ind = 0 ORDER BY id, VERSION DESC;

DROP TABLE kalturadw.dwh_dim_flavor_asset;
RENAME TABLE kalturadw.dwh_dim_flavor_asset_new TO kalturadw.dwh_dim_flavor_asset;

USE kalturadw;

DROP PROCEDURE IF EXISTS load_tags;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `load_tags`()
BEGIN
    DECLARE v_flavor_asset_id VARCHAR(60);
    DECLARE v_tags VARCHAR(256);
    DECLARE v_updated_at TIMESTAMP;
    DECLARE v_tag_name VARCHAR(256);
    DECLARE v_tag_id INT;
    DECLARE v_tags_done INT;
    DECLARE v_tags_idx INT;
    DECLARE done INT DEFAULT 0;
    DECLARE assets CURSOR FOR
    SELECT id, tags, updated_at
    FROM dwh_dim_flavor_asset;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN assets;

    read_loop: LOOP
        FETCH assets INTO v_flavor_asset_id, v_tags, v_updated_at;
        IF done THEN
             LEAVE read_loop;
	END IF;

        SET v_tags_done = 0;
        SET v_tags_idx = 1;

        WHILE NOT v_tags_done DO
            SET v_tag_name = SUBSTRING(v_tags, v_tags_idx,
					IF(LOCATE(',', v_tags, v_tags_idx) > 0,
					LOCATE(',', v_tags, v_tags_idx) - v_tags_idx,
					LENGTH(v_tags)));

            SET v_tag_name = TRIM(v_tag_name);
            IF LENGTH(v_tag_name) > 0 THEN
                SET v_tags_idx = v_tags_idx + LENGTH(v_tag_name) + 1;
                -- add the tag if it doesnt already exist
                INSERT IGNORE INTO dwh_dim_tags (tag_name) VALUES (v_tag_name);
                
		SELECT tag_id INTO v_tag_id FROM dwh_dim_tags WHERE tag_name = v_tag_name;

                -- add the flavor_asset tag
                INSERT IGNORE INTO dwh_dim_flavor_asset_tags (flavor_asset_id, tag_id, updated_at) VALUES (v_flavor_asset_id, v_tag_id, v_updated_at);
            ELSE
                SET v_tags_done = 1;
            END IF;
        END WHILE;
    END LOOP;
END$$

DELIMITER ;

CALL load_tags();

DROP PROCEDURE load_tags;

ALTER TABLE `dwh_dim_flavor_asset` DROP COLUMN tags;

USE kalturadw;

DROP TABLE IF EXISTS dwh_dim_flavor_asset_sorted;

CREATE TABLE `dwh_dim_flavor_asset_sorted` (
  `id` VARCHAR(60) NOT NULL DEFAULT '',
  `int_id` INT(11) DEFAULT NULL,
  `partner_id` INT(11) DEFAULT NULL,
  `created_at` DATETIME DEFAULT NULL,
  `updated_at` DATETIME DEFAULT NULL,
  `deleted_at` DATETIME DEFAULT NULL,
  `entry_id` VARCHAR(60) DEFAULT NULL,
  `flavor_params_id` INT(11) DEFAULT NULL,
  `status` TINYINT(4) DEFAULT NULL,
  `version` VARCHAR(60) NOT NULL,
  `description` VARCHAR(765) DEFAULT NULL,
  `width` INT(11) DEFAULT NULL,
  `height` INT(11) DEFAULT NULL,
  `bitrate` INT(11) DEFAULT NULL,
  `frame_rate` FLOAT DEFAULT NULL,
  `size` INT(11) DEFAULT NULL,
  `is_original` INT(11) DEFAULT NULL,
  `file_ext_id` INT(11) DEFAULT NULL,
  `container_format_id` INT(11) DEFAULT NULL,
  `video_codec_id` INT(11) DEFAULT NULL,
  `dwh_creation_date` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ri_ind` TINYINT(4) NOT NULL DEFAULT '0'
) ENGINE=MYISAM DEFAULT CHARSET=utf8;


INSERT INTO kalturadw.dwh_dim_flavor_asset_sorted (`id` ,
  `int_id` ,
  `partner_id` ,
  `created_at` ,
  `updated_at` ,
  `deleted_at` ,
  `entry_id` ,
  `flavor_params_id` ,
  `status` ,
  `version` ,
  `description` ,   
  `width` ,
  `height` ,
  `bitrate` ,
  `frame_rate` ,
  `size` ,
  `is_original` ,
  `file_ext_id` ,
  `container_format_id` ,
  `video_codec_id` ,
  `dwh_creation_date` ,
  `dwh_update_date` ,
  `ri_ind`) 
SELECT 
  `id` ,
  `int_id` ,
  `partner_id` ,
  `created_at` ,
  `updated_at` ,
  `deleted_at` ,
  `entry_id` ,
  `flavor_params_id` ,
  `status` ,
  `version` ,
  `description` ,   
  `width` ,
  `height` ,
  `bitrate` ,
  `frame_rate` ,
  `size` ,
  `is_original` ,
  `file_ext_id` ,
  `container_format_id` ,
  `video_codec_id` ,
  `dwh_creation_date` ,
  `dwh_update_date` ,
  `ri_ind` 
  FROM dwh_dim_flavor_asset
  ORDER BY deleted_at, updated_at;
  
ALTER TABLE dwh_dim_flavor_asset_sorted
ADD PRIMARY KEY (`id`),
ADD KEY `deleted_at` (`deleted_at`),
ADD KEY `dwh_update_date` (`dwh_update_date`);

DROP TABLE dwh_dim_flavor_asset;
RENAME TABLE dwh_dim_flavor_asset_sorted TO dwh_dim_flavor_asset;

ALTER TABLE kalturadw.dwh_dim_entries CHANGE entry_name entry_name VARCHAR(256) DEFAULT NULL;

--
-- Dumping routines for database 'kalturadw'
--
/*!50003 DROP FUNCTION IF EXISTS `calc_month_id` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 FUNCTION `calc_month_id`(date_id INT(11)) RETURNS int(11)
    DETERMINISTIC
BEGIN
	RETURN FLOOR(date_id/100);
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `calc_partner_monthly_storage` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`root`@`localhost`*/ /*!50003 FUNCTION `calc_partner_monthly_storage`(p_month_id INT ,p_partner_id INT) RETURNS decimal(19,4)
    DETERMINISTIC
BEGIN
	DECLARE avg_cont_aggr_storage DECIMAL(19,4);

        SELECT  calc_partner_storage_data_time_range (p_month_id*100+1,p_month_id*100+31,p_partner_id)
	INTO avg_cont_aggr_storage;
        RETURN avg_cont_aggr_storage;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `calc_partner_storage_data_time_range` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`root`@`localhost`*/ /*!50003 FUNCTION `calc_partner_storage_data_time_range`(p_start_date_id INT, p_end_date_id INT ,p_partner_id INT ) RETURNS decimal(19,4)
    DETERMINISTIC
BEGIN	
	DECLARE avg_cont_aggr_storage DECIMAL (19,4);
  	
	SELECT	SUM(continuous_aggr_storage/DAY(LAST_DAY(continuous_partner_storage.date_id))) avg_continuous_aggr_storage_mb
	INTO avg_cont_aggr_storage
	FROM (SELECT * FROM (
			SELECT 	all_times.day_id date_id,
				IF(SUM(aggr_p.aggr_storage_mb) IS NOT NULL, SUM(aggr_p.aggr_storage_mb),
                                (SELECT aggr_storage_mb FROM dwh_hourly_partner_usage inner_a_p 
                                 WHERE  inner_a_p.partner_id=p_partner_id AND 
                                        inner_a_p.date_id<all_times.day_id AND 
                                        inner_a_p.aggr_storage_mb IS NOT NULL 
                                        AND inner_a_p.hour_id = 0 
                                        ORDER BY inner_a_p.date_id DESC LIMIT 1)) continuous_aggr_storage
			FROM 	dwh_hourly_partner_usage aggr_p RIGHT JOIN
				dwh_dim_time all_times
				ON (all_times.day_id=aggr_p.date_id 
					AND aggr_p.hour_id = 0 
					AND aggr_p.partner_id=p_partner_id)
			WHERE 	all_times.day_id BETWEEN 20081230 AND p_end_date_id
			GROUP BY all_times.day_id) results
			WHERE date_id between p_start_date_id AND p_end_date_id
		) continuous_partner_storage;
	RETURN avg_cont_aggr_storage;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `calc_time_shift` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 FUNCTION `calc_time_shift`(date_id INT, hour_id INT, time_shift INT) RETURNS int(11)
    NO SQL
BEGIN
	RETURN DATE_FORMAT((date_id + INTERVAL hour_id HOUR + INTERVAL time_shift HOUR), '%Y%m%d')*1;
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `get_overage_charge` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 FUNCTION `get_overage_charge`(max_amount DECIMAL(19,4), actual_amount DECIMAL(19,4), charge_units INT(11), charge_usd_per_unit DECIMAL(19,4)) RETURNS decimal(19,4)
    NO SQL
BEGIN
	RETURN GREATEST(0,IFNULL(CEILING((actual_amount - max_amount)/charge_units)*charge_usd_per_unit,0));
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP FUNCTION IF EXISTS `resolve_aggr_name` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 FUNCTION `resolve_aggr_name`(p_aggr_name VARCHAR(100),p_field_name VARCHAR(100)) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	DECLARE v_aggr_id_field VARCHAR(100);
	SELECT aggr_table, aggr_id_field
	INTO  v_aggr_table, v_aggr_id_field
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = p_aggr_name;
	
	IF p_field_name = 'aggr_table' THEN RETURN v_aggr_table;
	ELSEIF p_field_name = 'aggr_id_field' THEN RETURN v_aggr_id_field;
	END IF;
	RETURN '';
    END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `add_daily_partition_for_table` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `add_daily_partition_for_table`(table_name VARCHAR(40))
BEGIN
	DECLARE p_name,p_value VARCHAR(100);
	DECLARE p_date,_current_date DATETIME;
	DECLARE p_continue BOOL;
	
	SELECT NOW()
		INTO _current_date;
	SET p_continue = TRUE;
	WHILE (p_continue) DO
		SELECT MAX(partition_description) n,
			   (MAX(partition_description) + INTERVAL 1 DAY)*1  v,
			   STR_TO_DATE(MAX(partition_description),'%Y%m%d')
		INTO p_name,p_value, p_date
		FROM `information_schema`.`partitions` 
		WHERE `partitions`.`TABLE_NAME` = table_name;
		IF (_current_date > p_date - INTERVAL 7 DAY AND p_name IS NOT NULL) THEN
			SET @s = CONCAT('alter table kalturadw.' , table_name , ' ADD PARTITION (partition p_' ,p_name ,' values less than (', p_value ,'))');
			PREPARE stmt FROM  @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		ELSE
			SET p_continue = FALSE;
		END IF;
	END WHILE;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `add_monthly_partition_for_table` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `add_monthly_partition_for_table`(table_name VARCHAR(40))
BEGIN
	DECLARE p_name,p_value VARCHAR(100);
	DECLARE p_date,_current_date DATETIME;
	DECLARE p_continue BOOL;
	
	SELECT NOW()
		INTO _current_date;
	SET p_continue = TRUE;
	WHILE (p_continue) DO
		SELECT EXTRACT( YEAR_MONTH FROM MAX(partition_description)) n,
			   (MAX(partition_description) + INTERVAL 1 MONTH)*1  v,
			   STR_TO_DATE(MAX(partition_description),'%Y%m%d')
		INTO p_name,p_value, p_date
		FROM `information_schema`.`partitions` 
		WHERE `partitions`.`TABLE_NAME` = table_name;
		IF (_current_date > p_date - INTERVAL 1 MONTH AND p_name IS NOT NULL) THEN
			SET @s = CONCAT('alter table kalturadw.' , table_name , ' ADD PARTITION (partition p_' ,p_name ,' values less than (', p_value ,'))');
			PREPARE stmt FROM  @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		ELSE
			SET p_continue = FALSE;
		END IF;
	END WHILE;
END */;;
DELIMITER ;
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `add_partitions`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `add_partitions`()
BEGIN
	CALL add_daily_partition_for_table('dwh_fact_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_session_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_sessions');
	CALL add_daily_partition_for_table('dwh_fact_bandwidth_usage');
	CALL add_monthly_partition_for_table('dwh_fact_entries_sizes');
	CALL add_monthly_partition_for_table('dwh_hourly_events_entry');
	CALL add_monthly_partition_for_table('dwh_hourly_events_domain');
	CALL add_monthly_partition_for_table('dwh_hourly_events_country');
	CALL add_monthly_partition_for_table('dwh_hourly_events_widget');
	CALL add_monthly_partition_for_table('dwh_hourly_events_uid');	
	CALL add_monthly_partition_for_table('dwh_hourly_events_domain_referrer');	
	CALL add_monthly_partition_for_table('dwh_hourly_partner');
	CALL add_monthly_partition_for_table('dwh_hourly_partner_usage');
	CALL add_monthly_partition_for_table('dwh_hourly_events_devices');
END$$

DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `all_tables_to_new` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `all_tables_to_new`()
BEGIN
	DECLARE done INT DEFAULT 0;
	DECLARE v_greater_than_or_equal_date_id INT;
	DECLARE v_less_than_date_id INT;
	DECLARE v_table_name VARCHAR(256);
	DECLARE c_partitions 
	CURSOR FOR 
	SELECT greater_than_or_equal_date_id, less_than_date_id, table_name
	FROM kalturadw_ds.tables_to_new
	ORDER BY less_than_date_id;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	OPEN c_partitions;
	
	read_loop: LOOP
    FETCH c_partitions INTO v_greater_than_or_equal_date_id, v_less_than_date_id, v_table_name;
    IF done THEN
      LEAVE read_loop;
    END IF;
    
	CALL do_tables_to_new(v_greater_than_or_equal_date_id, v_less_than_date_id,v_table_name);
	
	
  END LOOP;

  CLOSE c_partitions;
	
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `apply_table_partitions_to_target_table` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `apply_table_partitions_to_target_table`(p_table_name VARCHAR(255))
BEGIN
        DECLARE done INT DEFAULT 0;
        DECLARE v_partition_statement VARCHAR(255);
        DECLARE c_partitions
        CURSOR FOR
        SELECT CONCAT('ALTER TABLE kalturadw.',p_table_name,'_new ADD PARTITION (PARTITION ', partition_name,' VALUES LESS THAN(', partition_description, '));') cmd
        FROM information_schema.PARTITIONS existing, (SELECT MAX(partition_description) latest FROM information_schema.PARTITIONS WHERE table_name =CONCAT(p_table_name,'_new')) new_table
        WHERE existing.partition_description > new_table.latest AND table_name = p_table_name
        ORDER BY partition_description;

        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

        OPEN c_partitions;

        read_loop: LOOP
        FETCH c_partitions INTO v_partition_statement;
            IF done THEN
              LEAVE read_loop;
            END IF;
	SET @s = v_partition_statement;
        PREPARE stmt FROM @s;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;

        END LOOP;

        CLOSE c_partitions;
END */;;
DELIMITER ;
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day`(p_date_val DATE,p_hour_id INT(11), p_aggr_name VARCHAR(100))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	DECLARE v_aggr_id_field VARCHAR(100);
	DECLARE v_aggr_id_field_str VARCHAR(100);
	DECLARE v_aggr_join_stmt VARCHAR(200);
	DECLARE extra VARCHAR(100);
	DECLARE v_from_archive DATE;
	DECLARE v_ignore DATE;
	DECLARE v_table_name VARCHAR(100);
		
	SELECT DATE(NOW() - INTERVAL archive_delete_days_back DAY), DATE(archive_last_partition)
	INTO v_ignore, v_from_archive
	FROM kalturadw_ds.retention_policy
	WHERE table_name = 'dwh_fact_events';	
	
	IF (p_date_val >= v_ignore) THEN -- not so old that we don't have any data
	
		IF (p_date_val >= v_from_archive) THEN -- aggr from archive or from events
			SET v_table_name = 'dwh_fact_events';
		ELSE
			SET v_table_name = 'dwh_fact_events_archive';
		END IF;
		
		SELECT aggr_table, aggr_id_field, aggr_join_stmt
		INTO  v_aggr_table, v_aggr_id_field, v_aggr_join_stmt
		FROM kalturadw_ds.aggr_name_resolver
		WHERE aggr_name = p_aggr_name;
		
		IF ( v_aggr_id_field <> "" ) THEN
			SET v_aggr_id_field_str = CONCAT (',',v_aggr_id_field);
		ELSE
			SET v_aggr_id_field_str = "";
		END IF;
		
		SET @s = CONCAT('UPDATE aggr_managment SET start_time = NOW()
		WHERE aggr_name = ''',p_aggr_name,''' AND aggr_day = ''',p_date_val,''' AND hour_id = ',p_hour_id);
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		IF ( v_aggr_table <> "" ) THEN
			SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
				(partner_id
				,date_id
				,hour_id
				',v_aggr_id_field_str,' 
				,count_loads
				,count_plays 
				,count_plays_25 
				,count_plays_50 
				,count_plays_75 
				,count_plays_100 
				,count_edit
				,count_viral 
				,count_download 
				,count_report
				,count_buf_start
				,count_buf_end
				,count_open_full_screen
				,count_close_full_screen
				,count_replay
				,count_seek
				,count_open_upload
				,count_save_publish 
				,count_close_editor
				,count_pre_bumper_played
				,count_post_bumper_played
				,count_bumper_clicked
				,count_preroll_started
				,count_midroll_started
				,count_postroll_started
				,count_overlay_started
				,count_preroll_clicked
				,count_midroll_clicked
				,count_postroll_clicked
				,count_overlay_clicked
				,count_preroll_25
				,count_preroll_50
				,count_preroll_75
				,count_midroll_25
				,count_midroll_50
				,count_midroll_75
				,count_postroll_25
				,count_postroll_50
				,count_postroll_75
				) 
			SELECT  ev.partner_id,ev.event_date_id, event_hour_id',v_aggr_id_field_str,',
			SUM(IF(ev.event_type_id = 2, 1,NULL)) count_loads,
			SUM(IF(ev.event_type_id = 3, 1,NULL)) count_plays,
			SUM(IF(ev.event_type_id = 4, 1,NULL)) count_plays_25,
			SUM(IF(ev.event_type_id = 5, 1,NULL)) count_plays_50,
			SUM(IF(ev.event_type_id = 6, 1,NULL)) count_plays_75,
			SUM(IF(ev.event_type_id = 7, 1,NULL)) count_plays_100,
			SUM(IF(ev.event_type_id = 8, 1,NULL)) count_edit,
			SUM(IF(ev.event_type_id = 9, 1,NULL)) count_viral,
			SUM(IF(ev.event_type_id = 10, 1,NULL)) count_download,
			SUM(IF(ev.event_type_id = 11, 1,NULL)) count_report,
			SUM(IF(ev.event_type_id = 12, 1,NULL)) count_buf_start,
			SUM(IF(ev.event_type_id = 13, 1,NULL)) count_buf_end,
			SUM(IF(ev.event_type_id = 14, 1,NULL)) count_open_full_screen,
			SUM(IF(ev.event_type_id = 15, 1,NULL)) count_close_full_screen,
			SUM(IF(ev.event_type_id = 16, 1,NULL)) count_replay,
			SUM(IF(ev.event_type_id = 17, 1,NULL)) count_seek,
			SUM(IF(ev.event_type_id = 18, 1,NULL)) count_open_upload,
			SUM(IF(ev.event_type_id = 19, 1,NULL)) count_save_publish,
			SUM(IF(ev.event_type_id = 20, 1,NULL)) count_close_editor,
			SUM(IF(ev.event_type_id = 21, 1,NULL)) count_pre_bumper_played,
			SUM(IF(ev.event_type_id = 22, 1,NULL)) count_post_bumper_played,
			SUM(IF(ev.event_type_id = 23, 1,NULL)) count_bumper_clicked,
			SUM(IF(ev.event_type_id = 24, 1,NULL)) count_preroll_started,
			SUM(IF(ev.event_type_id = 25, 1,NULL)) count_midroll_started,
			SUM(IF(ev.event_type_id = 26, 1,NULL)) count_postroll_started,
			SUM(IF(ev.event_type_id = 27, 1,NULL)) count_overlay_started,
			SUM(IF(ev.event_type_id = 28, 1,NULL)) count_preroll_clicked,
			SUM(IF(ev.event_type_id = 29, 1,NULL)) count_midroll_clicked,
			SUM(IF(ev.event_type_id = 30, 1,NULL)) count_postroll_clicked,
			SUM(IF(ev.event_type_id = 31, 1,NULL)) count_overlay_clicked,
			SUM(IF(ev.event_type_id = 32, 1,NULL)) count_preroll_25,
			SUM(IF(ev.event_type_id = 33, 1,NULL)) count_preroll_50,
			SUM(IF(ev.event_type_id = 34, 1,NULL)) count_preroll_75,
			SUM(IF(ev.event_type_id = 35, 1,NULL)) count_midroll_25,
			SUM(IF(ev.event_type_id = 36, 1,NULL)) count_midroll_50,
			SUM(IF(ev.event_type_id = 37, 1,NULL)) count_midroll_75,
			SUM(IF(ev.event_type_id = 38, 1,NULL)) count_postroll_25,
			SUM(IF(ev.event_type_id = 39, 1,NULL)) count_postroll_50,
			SUM(IF(ev.event_type_id = 40, 1,NULL)) count_postroll_75
			FROM ',v_table_name,' as ev ',v_aggr_join_stmt,' 
			WHERE ev.event_type_id BETWEEN 2 AND 40 
				AND ev.event_date_id  = DATE(''',p_date_val,''')*1
				AND ev.event_hour_id = ',p_hour_id,'
				AND ev.entry_media_type_id IN (1,2,5,6)  /* allow only video & audio & mix */
			GROUP BY partner_id,event_date_id, event_hour_id',v_aggr_id_field_str,';');
		
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
					
		SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
				(partner_id
				,date_id
				,hour_id
				',v_aggr_id_field_str,'
				,sum_time_viewed
				,count_time_viewed)
				SELECT partner_id, event_date_id, event_hour_id',v_aggr_id_field_str,',
				SUM(duration / 60 / 4 * (v_25+v_50+v_75+v_100)) sum_time_viewed,
				COUNT(DISTINCT s_play) count_time_viewed
				FROM(
				SELECT ev.partner_id, ev.event_date_id, ev.event_hour_id',v_aggr_id_field_str,', ev.session_id,
					MAX(duration) duration,
					COUNT(DISTINCT IF(ev.event_type_id IN (4),1,NULL)) v_25,
					COUNT(DISTINCT IF(ev.event_type_id IN (5),1,NULL)) v_50,
					COUNT(DISTINCT IF(ev.event_type_id IN (6),1,NULL)) v_75,
					COUNT(DISTINCT IF(ev.event_type_id IN (7),1,NULL)) v_100,
					MAX(IF(event_type_id IN (3),session_id,NULL)) s_play
				FROM ',v_table_name,' as ev ',v_aggr_join_stmt,' 
				WHERE ev.event_date_id  = DATE(''',p_date_val,''')*1
					AND ev.event_hour_id = ',p_hour_id,'
					AND ev.entry_media_type_id IN (1,2,5,6)  /* allow only video & audio & mix */
					AND ev.event_type_id IN(3,4,5,6,7) /* time viewed only when player reaches 25,50,75,100 */
				GROUP BY ev.partner_id, ev.event_date_id, ev.event_hour_id , ev.entry_id',v_aggr_id_field_str,',ev.session_id) e
				GROUP BY partner_id, event_date_id, event_hour_id',v_aggr_id_field_str,'
				ON DUPLICATE KEY UPDATE
				sum_time_viewed = values(sum_time_viewed), count_time_viewed=values(count_time_viewed);');
		
			PREPARE stmt FROM  @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			SET extra = CONCAT('post_aggregation_',p_aggr_name);
			IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME=extra) THEN
				SET @ss = CONCAT('CALL ',extra,'(''', p_date_val,''',',p_hour_id,');'); 
				PREPARE stmt1 FROM  @ss;
				EXECUTE stmt1;
				DEALLOCATE PREPARE stmt1;
			END IF ;
			
		END IF;	  
		
	END IF; -- end skip old data
	
	SET @s = CONCAT('UPDATE aggr_managment SET is_calculated = 1,end_time = NOW()
	WHERE aggr_name = ''',p_aggr_name,''' AND aggr_day = ''',p_date_val,''' AND hour_id =',p_hour_id);
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
		
END$$

DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_bandwidth` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `calc_aggr_day_partner_bandwidth`(p_date_val DATE)
BEGIN
	DECLARE v_ignore DATE;
	DECLARE v_from_archive DATE;
        DECLARE v_table_name VARCHAR(100);

	SELECT MAX(date(now() - interval archive_delete_days_back day))
	INTO v_ignore
	FROM kalturadw_ds.retention_policy
	WHERE table_name in('dwh_fact_bandwidth_usage', 'dwh_fact_fms_sessions');
	
	IF (p_date_val >= v_ignore) THEN 
		
		DELETE FROM kalturadw.dwh_hourly_partner_usage WHERE date_id = date(p_date_val)*1 and IFNULL(count_bandwidth_kb,0) > 0 and ifnull(count_storage_mb,0) = 0 and ifnull(aggr_storage_mb,0) = 0;
		UPDATE kalturadw.dwh_hourly_partner_usage SET count_bandwidth_kb = null WHERE date_id = date(p_date_val)*1 and (ifnull(count_storage_mb,0) > 0 or ifnull(aggr_storage_mb,0) > 0);
	
		
		SELECT date(archive_last_partition)
		INTO v_from_archive
		FROM kalturadw_ds.retention_policy
		WHERE table_name = 'dwh_fact_bandwidth_usage';

                IF (p_date_val >= v_from_archive) THEN 
                        SET v_table_name = 'dwh_fact_bandwidth_usage';
                ELSE
                        SET v_table_name = 'dwh_fact_bandwidth_usage_archive';
                END IF;

                SET @s = CONCAT('INSERT INTO kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, count_bandwidth_kb)
				SELECT partner_id, MAX(activity_date_id), 0 hour_id, bandwidth_source_id, SUM(bandwidth_bytes)/1024 count_bandwidth
				FROM ', v_table_name, '	WHERE activity_date_id=date(\'',p_date_val,'\')*1
				GROUP BY partner_id, bandwidth_source_id
				ON DUPLICATE KEY UPDATE	count_bandwidth_kb=VALUES(count_bandwidth_kb)');

 		PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;

			
		
		SELECT date(archive_last_partition)
		INTO v_from_archive
		FROM kalturadw_ds.retention_policy
		WHERE table_name = 'dwh_fact_fms_sessions';
		
		IF (p_date_val >= v_from_archive) THEN 
                        SET v_table_name = 'dwh_fact_fms_sessions';
                ELSE
                        SET v_table_name = 'dwh_fact_fms_sessions_archive';
                END IF;

			
		SET @s = CONCAT('INSERT INTO kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, count_bandwidth_kb)
				SELECT session_partner_id, MAX(session_date_id), 0 hour_id, bandwidth_source_id, SUM(total_bytes)/1024 count_bandwidth 
				FROM ', v_table_name, ' WHERE session_date_id=date(\'',p_date_val,'\')*1
				GROUP BY session_partner_id, bandwidth_source_id
				ON DUPLICATE KEY UPDATE	count_bandwidth_kb=VALUES(count_bandwidth_kb)');

                PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
	
		UPDATE aggr_managment SET is_calculated = 1, end_time = NOW() WHERE aggr_name = 'bandwidth_usage' AND aggr_day_int = date(p_date_val)*1;
	END IF;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_storage` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `calc_aggr_day_partner_storage`(date_val DATE)
BEGIN
	DELETE FROM kalturadw.dwh_hourly_partner_usage WHERE date_id = date(date_val)*1 and IFNULL(count_bandwidth_kb,0) = 0 and (ifnull(count_storage_mb,0) > 0 or ifnull(aggr_storage_mb,0) > 0);
        UPDATE kalturadw.dwh_hourly_partner_usage SET count_storage_mb = null, aggr_storage_mb=null WHERE date_id = date(date_val)*1 and IFNULL(count_bandwidth_kb,0) > 0;
	
	DROP TABLE IF EXISTS temp_aggr_storage;
	CREATE TEMPORARY TABLE temp_aggr_storage(
		partner_id      	INT(11) NOT NULL,
		date_id     		INT(11) NOT NULL,
		hour_id	 		TINYINT(4) NOT NULL,
		count_storage_mb	DECIMAL(19,4) NOT NULL
	) ENGINE = MEMORY;
      
	INSERT INTO 	temp_aggr_storage (partner_id, date_id, hour_id, count_storage_mb)
   	SELECT 		partner_id, MAX(entry_size_date_id), 0 hour_id, SUM(entry_additional_size_kb)/1024 count_storage_mb
	FROM 		dwh_fact_entries_sizes
	WHERE		entry_size_date_id=DATE(date_val)*1
	GROUP BY 	partner_id;
	
	INSERT INTO 	kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, count_storage_mb)
	SELECT		partner_id, date_id, hour_id, 1, count_storage_mb
	FROM		temp_aggr_storage
	ON DUPLICATE KEY UPDATE count_storage_mb=VALUES(count_storage_mb);
	
	INSERT INTO 	kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, aggr_storage_mb)
	SELECT 		a.partner_id, a.date_id, a.hour_id, 1, SUM(b.count_storage_mb)
	FROM 		temp_aggr_storage a, dwh_hourly_partner_usage b 
	WHERE 		a.partner_id=b.partner_id AND DATE(date_val)*1 >=b.date_id AND b.hour_id = 0 AND b.bandwidth_source_id = 1 AND b.count_storage_mb<>0
	GROUP BY 	a.date_id, a.hour_id, a.partner_id
	ON DUPLICATE KEY UPDATE aggr_storage_mb=VALUES(aggr_storage_mb);
END */;;
DELIMITER ;
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_entries_sizes`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_entries_sizes`(p_date_id INT(11))
BEGIN
	DECLARE v_date DATETIME;
	SET v_date = DATE(p_date_id);
	UPDATE aggr_managment SET start_time = NOW() WHERE aggr_name = 'storage_usage' AND aggr_day_int = p_date_id;
	
	DELETE FROM kalturadw.dwh_fact_entries_sizes WHERE entry_size_date_id = p_date_id;
	
	DROP TABLE IF EXISTS today_file_sync_subset; 
	
	CREATE TEMPORARY TABLE today_file_sync_subset AS
	SELECT DISTINCT s.id, s.partner_id, IFNULL(a.entry_id, object_id) entry_id, object_id, object_type, object_sub_type, IFNULL(file_size, 0) file_size
	FROM kalturadw.dwh_dim_file_sync s LEFT OUTER JOIN kalturadw.dwh_dim_flavor_asset a
	ON (object_type = 4 AND s.object_id = a.id AND a.entry_id IS NOT NULL AND a.ri_ind =0 AND s.partner_id = a.partner_id)
	WHERE s.updated_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
	AND object_type IN (1,4)
	AND original = 1
	AND s.STATUS IN (2,3)
	AND s.partner_id NOT IN (100  , -1  , -2  , 0 , 99 );
	
	ALTER TABLE today_file_sync_subset ADD INDEX id (`id`);	
	
	
	DROP TABLE IF EXISTS today_file_sync_max_version_ids;
	CREATE TEMPORARY TABLE today_file_sync_max_version_ids AS
	SELECT MAX(id) id, partner_id, entry_id, object_id, object_type, object_sub_type FROM today_file_sync_subset
	GROUP BY partner_id, entry_id, object_id, object_type, object_sub_type;
	DROP TABLE IF EXISTS today_sizes;
	
	
	CREATE TEMPORARY TABLE today_sizes AS
	SELECT max_id.partner_id, max_id.entry_id, max_id.object_id, max_id.object_type, max_id.object_sub_type, original.file_size 
	FROM today_file_sync_max_version_ids max_id, today_file_sync_subset original
	WHERE max_id.id = original.id;
	
	ALTER TABLE today_sizes ADD UNIQUE INDEX unique_key (`partner_id`, `entry_id`, `object_id`, `object_type`, `object_sub_type`);
	
	DROP TABLE IF EXISTS deleted_flavors;
	CREATE TEMPORARY TABLE deleted_flavors AS 
	SELECT DISTINCT partner_id, entry_id, id
	FROM kalturadw.dwh_dim_flavor_asset FORCE INDEX (deleted_at)
	WHERE STATUS = 3 AND deleted_at BETWEEN v_date AND v_date + INTERVAL 1 DAY;
		
	BEGIN
		DECLARE v_deleted_flavor_partner_id INT;
		DECLARE v_deleted_flavor_entry_id VARCHAR(60);
		DECLARE v_deleted_flavor_id VARCHAR(60);
		DECLARE done INT DEFAULT 0;
		DECLARE deleted_flavors_cursor CURSOR FOR 
		SELECT partner_id, entry_id, id	FROM deleted_flavors;
		DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
		
		OPEN deleted_flavors_cursor;
	
		read_loop: LOOP
			FETCH deleted_flavors_cursor INTO v_deleted_flavor_partner_id, v_deleted_flavor_entry_id, v_deleted_flavor_id;
			IF done THEN
				LEAVE read_loop;
			END IF;
			INSERT INTO today_sizes
				SELECT partner_id, v_deleted_flavor_entry_id, object_id, object_type, object_sub_type, 0 file_size
				FROM kalturadw.dwh_dim_file_sync
				WHERE object_id = v_deleted_flavor_id AND object_type = 4 AND updated_at < v_date AND file_size > 0
			ON DUPLICATE KEY UPDATE
				file_size = VALUES(file_size);
		END LOOP;
		CLOSE deleted_flavors_cursor;
	END;
	
	
	
	DROP TABLE IF EXISTS today_deleted_entries;
	CREATE TEMPORARY TABLE today_deleted_entries AS 
	SELECT entry_id FROM kalturadw.dwh_dim_entries
	WHERE modified_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
	AND partner_id NOT IN (100  , -1  , -2  , 0 , 99 )
	AND entry_status_id = 3
	AND entry_type_id = 1;	
	
	DELETE today_sizes FROM today_sizes, today_deleted_entries e 
		WHERE today_sizes.entry_id = e.entry_id;
	
	ALTER TABLE today_sizes DROP INDEX unique_key;
	
	
	DROP TABLE IF EXISTS yesterday_file_sync_subset; 
	CREATE TEMPORARY TABLE yesterday_file_sync_subset AS
	SELECT f.id, f.partner_id, f.object_id, f.object_type, f.object_sub_type, IFNULL(f.file_size, 0) file_size
	FROM today_sizes today, kalturadw.dwh_dim_file_sync f
	WHERE f.object_id = today.object_id
	AND f.partner_id = today.partner_id
	AND f.object_type = today.object_type
	AND f.object_sub_type = today.object_sub_type
	AND f.updated_at < v_date
	AND f.original = 1
	AND f.STATUS IN (2,3);
	
	
	DROP TABLE IF EXISTS yesterday_file_sync_max_version_ids;
	CREATE TEMPORARY TABLE yesterday_file_sync_max_version_ids AS
	SELECT MAX(id) id, partner_id, object_id, object_type, object_sub_type FROM yesterday_file_sync_subset
	GROUP BY partner_id, object_id, object_type, object_sub_type;
	
	DROP TABLE IF EXISTS yesterday_sizes;
	CREATE TEMPORARY TABLE yesterday_sizes AS
	SELECT max_id.partner_id, max_id.object_id, max_id.object_type, max_id.object_sub_type, original.file_size 
	FROM yesterday_file_sync_max_version_ids max_id, yesterday_file_sync_subset original
	WHERE max_id.id = original.id;
	
	
	INSERT INTO kalturadw.dwh_fact_entries_sizes (partner_id, entry_id, entry_additional_size_kb, entry_size_date, entry_size_date_id)
	SELECT t.partner_id, t.entry_id, ROUND(SUM(t.file_size - IFNULL(Y.file_size, 0))/1024, 3) entry_additional_size_kb,v_date, p_date_id 
	FROM today_sizes t LEFT OUTER JOIN yesterday_sizes Y 
	ON t.object_id = Y.object_id
	AND t.partner_id = Y.partner_id
	AND t.object_type = Y.object_type
	AND t.object_sub_type = Y.object_sub_type
	AND t.file_size <> Y.file_size
	GROUP BY t.partner_id, t.entry_id
	ON DUPLICATE KEY UPDATE 
		entry_additional_size_kb = VALUES(entry_additional_size_kb);
	
	
	DROP TABLE IF EXISTS deleted_entries;
	CREATE TEMPORARY TABLE deleted_entries AS
		SELECT es.partner_id partner_id, es.entry_id entry_id, v_date entry_size_date, p_date_id entry_size_date_id, -SUM(entry_additional_size_kb) entry_additional_size_kb
		FROM kalturadw.dwh_dim_entries e USE INDEX (modified_at) INNER JOIN kalturadw.dwh_fact_entries_sizes es
		WHERE e.modified_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
		AND e.entry_id = es.entry_id 
		AND e.partner_id = es.partner_id 
		AND e.partner_id NOT IN (100  , -1  , -2  , 0 , 99 )
		AND e.entry_status_id = 3
		AND es.entry_size_date_id < p_date_id
		GROUP BY es.partner_id, es.entry_id
		HAVING SUM(entry_additional_size_kb) > 0;
	
	INSERT INTO kalturadw.dwh_fact_entries_sizes (partner_id, entry_id, entry_size_date, entry_size_date_id, entry_additional_size_kb)
		SELECT partner_id, entry_id, entry_size_date, entry_size_date_id, entry_additional_size_kb FROM deleted_entries
	ON DUPLICATE KEY UPDATE 
		entry_additional_size_kb = VALUES(entry_additional_size_kb);
	
	CALL kalturadw.calc_aggr_day_partner_storage(v_date);
	UPDATE aggr_managment SET is_calculated = 1, end_time = NOW() WHERE aggr_name = 'storage_usage' AND aggr_day_int = p_date_id;
END$$

DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `calc_partner_billing_data` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`root`@`localhost`*/ /*!50003 PROCEDURE `calc_partner_billing_data`(p_date_id INT(11),p_partner_id INT)
BEGIN
    SELECT
        FLOOR(continuous_partner_storage.date_id/100) month_id,
        SUM(continuous_aggr_storage/DAY(LAST_DAY(continuous_partner_storage.date_id))) avg_continuous_aggr_storage_mb,
        SUM(continuous_partner_storage.count_bandwidth) sum_partner_bandwidth_kb
    FROM
    (	
        SELECT  all_times.day_id date_id,
                        p_partner_id partner_id,
                        SUM(aggr_p.count_bandwidth_kb) count_bandwidth,
            IF(SUM(aggr_p.aggr_storage_mb) IS NOT NULL, SUM(aggr_p.aggr_storage_mb),
                                (SELECT aggr_storage_mb FROM dwh_hourly_partner_usage inner_a_p 
                                 WHERE  inner_a_p.partner_id=p_partner_id AND 
                                        inner_a_p.date_id<all_times.day_id AND 
                                        inner_a_p.aggr_storage_mb IS NOT NULL 
                                        AND inner_a_p.hour_id = 0 
                                        ORDER BY inner_a_p.date_id DESC LIMIT 1)) continuous_aggr_storage
                    FROM 
                        dwh_hourly_partner_usage aggr_p RIGHT JOIN
                        dwh_dim_time all_times
                        ON (all_times.day_id=aggr_p.date_id 
                                AND aggr_p.partner_id=p_partner_id
                                AND aggr_p.hour_id = 0
                                )
                WHERE   all_times.day_id>=20081230 AND all_times.day_id <= LEAST(p_date_id,DATE(NOW())*1)
        GROUP BY day_id
    ) continuous_partner_storage
	GROUP BY month_id
	WITH ROLLUP;	
END */;;
DELIMITER ;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `do_tables_to_new` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `do_tables_to_new`(p_greater_than_or_equal_date_id int, p_less_than_date_id int, p_table_name varchar(256))
BEGIN
	DECLARE v_copied int;
	declare v_column varchar(256);
	DECLARE select_fields VARCHAR(4000);


	SELECT is_copied, column_name
	INTO v_copied, v_column
	FROM kalturadw_ds.tables_to_new
	WHERE greater_than_or_equal_date_id = p_greater_than_or_equal_date_id AND 
	      	less_than_date_id = p_less_than_date_id AND 
		table_name = p_table_name;
	
	IF (v_copied=0) THEN
		
		SELECT GROUP_CONCAT(column_name) 
		INTO 	select_fields
		FROM (
			SELECT  column_name
	                FROM information_schema.COLUMNS
        	        WHERE CONCAT(table_schema,'.',table_name) IN (CONCAT('kalturadw.',p_table_name),CONCAT('kalturadw.',p_table_name,'_new'))
			 GROUP BY column_name
			 HAVING COUNT(*) > 1
			 ORDER BY MIN(ordinal_position)
		) COLUMNS;
		
		SET @s = CONCAT('insert into ',p_table_name,'_new (',select_fields,') ',
						' select ',select_fields,
						' from ',p_table_name,
						' where ',v_column,'  >= ',p_greater_than_or_equal_date_id, ' AND ', v_column, ' < ', p_less_than_date_id);
		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

		UPDATE kalturadw_ds.tables_to_new SET is_copied = 1 
		WHERE greater_than_or_equal_date_id = p_greater_than_or_equal_date_id AND
                less_than_date_id = p_less_than_date_id AND
                table_name = p_table_name;
	END IF;
	
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `generate_daily_usage_report` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `generate_daily_usage_report`(p_date_val DATE)
BEGIN
	DECLARE yesterday_date_id INT(11);
	
	DECLARE the_day_before_yesteray_date_id INT(11);
	
	DECLARE 5_days_ago_date_id DATE;
	DECLARE 30_days_ago_date_id DATE;
	
	SET yesterday_date_id = (DATE(p_date_val) - INTERVAL 1 DAY)*1;
	
	SET the_day_before_yesteray_date_id = (DATE(p_date_val) - INTERVAL 2 DAY)*1;
	
	SET 5_days_ago_date_id = (DATE(p_date_val) - INTERVAL 5 DAY)*1;
	SET 30_days_ago_date_id = (DATE(p_date_val) - INTERVAL 30 DAY)*1;
	
	INSERT INTO kalturadw.dwh_daily_usage_reports (measure, classification, DATE, yesterday, the_day_before, diff, last_5_days_avg, last_30_days_avg, outer_order, inner_order)
	
	SELECT 	measure AS Measure, 
		classification AS Classification, 
		IF (measure = 'Bandwidth (MB)',DATE(p_date_val) - INTERVAL 3 DAY, DATE(p_date_val))  AS 'Report Date', 
		IFNULL(yesterday, 0) yesterday, 
		IFNULL(the_day_before, 0) the_day_before, 
		IF(IFNULL(the_day_before, 0) = 0, 0, IFNULL(yesterday, 2)/the_day_before*100 - 100) diff,
		IFNULL(last_5_days, 0) AS last_5_days_avg, 
		IFNULL(last_30_days, 0) AS last_30_days_avg,
		outer_order, inner_order FROM (
	
	SELECT * FROM(
	
	SELECT 'Content' measure, 
		t.caption classification, 
		yesterday, 
		the_day_before, 
		last_5_days, 
		last_30_days,
		1 outer_order,
		sort_order inner_order
	FROM
	(
		SELECT 	IF(entry_media_type_id NOT IN (1, 5, 2, 6), IF (entry_media_type_id IN (11,12,13), -99999, -1), entry_media_type_id) entry_media_type_id,
			SUM(IF (created_at BETWEEN DATE(yesterday_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0)) yesterday,
			SUM(IF (created_at BETWEEN DATE(the_day_before_yesteray_date_id) AND DATE(the_day_before_yesteray_date_id) + INTERVAL 1 DAY, 1, 0)) the_day_before,
			SUM(IF (created_at BETWEEN DATE(5_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0))/5 last_5_days,
			COUNT(*)/30 last_30_days			
			FROM kalturadw.dwh_dim_entries 
			WHERE created_at BETWEEN DATE(30_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY
			GROUP BY IF(entry_media_type_id NOT IN (1, 5, 2, 6), IF (entry_media_type_id IN (11,12,13), -99999, -1), entry_media_type_id)
	) e RIGHT OUTER JOIN 
	(
		SELECT 	entry_media_type_id, 
			CASE entry_media_type_name
				WHEN 'VIDEO' THEN 'Videos'
				WHEN 'AUDIO' THEN 'Audios'
				WHEN 'IMAGE' THEN 'Images'
				WHEN 'SHOW' THEN 'Mixs'
				WHEN 'PDF' THEN 'PDF'
				ELSE 'Other' END caption,
			CASE entry_media_type_name
				WHEN 'VIDEO' THEN 1
				WHEN 'AUDIO' THEN 2
				WHEN 'IMAGE' THEN 3
				WHEN 'SHOW' THEN 4
				WHEN 'PDF' THEN 5
				ELSE 6 END sort_order
		FROM (SELECT entry_media_type_id, entry_media_type_name FROM kalturadw.dwh_dim_entry_media_type UNION SELECT -99999, 'PDF') entry_media_type
		WHERE entry_media_type_id IN (1, 5, 2, 6, -99999, -1)
	) t
	ON e.entry_media_type_id = t.entry_media_type_id
	) Content	
	UNION
	
	SELECT * FROM ( 
	SELECT 	'Deleted' measure, 
		'Entries' Classification, 
		SUM(IF (modified_at BETWEEN DATE(yesterday_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0)) yesterday,
		SUM(IF (modified_at BETWEEN DATE(the_day_before_yesteray_date_id) AND DATE(the_day_before_yesteray_date_id) + INTERVAL 1 DAY, 1, 0)) the_day_before,
		SUM(IF (modified_at BETWEEN DATE(5_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0))/5 last_5_days,
		COUNT(*)/30 last_30_days, 2 outer_order, 1 inner_order
		FROM kalturadw.dwh_dim_entries 
		WHERE modified_at BETWEEN DATE(30_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY
		AND entry_status_id = 3) deleted_entries
	UNION
	
	SELECT * FROM (
	SELECT 'Upload' measure, 
		caption classification, 
		yesterday, 
		the_day_before, 
		last_5_days, 
		last_30_days, 3 outer_order, sort_order inner_order 
		FROM 
		(
			SELECT IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL)) entry_status_id,
			SUM(IF (created_at BETWEEN DATE(yesterday_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0)) yesterday,
			SUM(IF (created_at BETWEEN DATE(the_day_before_yesteray_date_id) AND DATE(the_day_before_yesteray_date_id) + INTERVAL 1 DAY, 1, 0)) the_day_before,
			SUM(IF (created_at BETWEEN DATE(5_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0))/5 last_5_days,
			COUNT(*)/30 last_30_days
			FROM kalturadw.dwh_dim_entries
			WHERE created_at BETWEEN DATE(30_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY
			AND entry_media_source_id = 1
			GROUP BY IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL))
		) e
		RIGHT OUTER JOIN 
		(
			SELECT 0 id, 'Ready' caption, 1 sort_order
			UNION 
			SELECT 1 id, 'Failed' caption, 2 sort_order
		) s
		ON e.entry_status_id = s.id) uploaded_entries
	
	UNION
	
	SELECT * FROM (SELECT 'Web Cam' measure, 
		caption classification, 
		yesterday, 
		the_day_before, 
		last_5_days, 
		last_30_days, 4 outer_order, sort_order inner_order FROM 
		(
			SELECT IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL)) entry_status_id,
			SUM(IF (created_at BETWEEN DATE(yesterday_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0)) yesterday,
			SUM(IF (created_at BETWEEN DATE(the_day_before_yesteray_date_id) AND DATE(the_day_before_yesteray_date_id) + INTERVAL 1 DAY, 1, 0)) the_day_before,
			SUM(IF (created_at BETWEEN DATE(5_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0))/5 last_5_days,
			COUNT(*)/30 last_30_days
			FROM kalturadw.dwh_dim_entries
			WHERE created_at BETWEEN DATE(30_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY
			AND entry_media_source_id = 2
			GROUP BY IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL))
		) e
		RIGHT OUTER JOIN 
		(
			SELECT 0 id, 'Ready' caption, 1 sort_order
			UNION 
			SELECT 1 id, 'Failed' caption, 2 sort_order
		) s
		ON e.entry_status_id = s.id) web_cam
	
	UNION
	
	SELECT * FROM (SELECT 'Import' measure, 
		caption classification, 
		yesterday, 
		the_day_before, 
		last_5_days, 
		last_30_days, 5 outer_order, sort_order inner_order FROM 
		(
			SELECT IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL)) entry_status_id,
			SUM(IF (created_at BETWEEN DATE(yesterday_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0)) yesterday,
			SUM(IF (created_at BETWEEN DATE(the_day_before_yesteray_date_id) AND DATE(the_day_before_yesteray_date_id) + INTERVAL 1 DAY, 1, 0)) the_day_before,
			SUM(IF (created_at BETWEEN DATE(5_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0))/5 last_5_days,
			COUNT(*)/30 last_30_days
			FROM kalturadw.dwh_dim_entries
			WHERE created_at BETWEEN DATE(30_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY
			AND entry_media_source_id NOT IN (1, 2)
			GROUP BY IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL))
		) e
		RIGHT OUTER JOIN 
		(
			SELECT 0 id, 'Ready' caption, 1 sort_order
			UNION 
			SELECT 1 id, 'Failed' caption, 2 sort_order
		) s
		ON e.entry_status_id = s.id) imported_entries
	
	UNION
	
	SELECT * FROM (SELECT 'Conversion' measure, 
		caption classification, 
		yesterday, 
		the_day_before, 
		last_5_days, 
		last_30_days, 6 outer_order, sort_order inner_order  FROM 
		(
			SELECT IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL)) entry_status_id,
			SUM(IF (created_at BETWEEN DATE(yesterday_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0)) yesterday,
			SUM(IF (created_at BETWEEN DATE(the_day_before_yesteray_date_id) AND DATE(the_day_before_yesteray_date_id) + INTERVAL 1 DAY, 1, 0)) the_day_before,
			SUM(IF (created_at BETWEEN DATE(5_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0))/5 last_5_days,
			COUNT(*)/30 last_30_days
			FROM kalturadw.dwh_dim_entries
			WHERE created_at BETWEEN DATE(30_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY
			GROUP BY IF (entry_status_id = 2, 0, IF (entry_status_id IN (-2, -1, 0, 1, 4), 1, NULL))
		) e
		RIGHT OUTER JOIN 
		(
			SELECT 0 id, 'Ready' caption, 1 sort_order
			UNION 
			SELECT 1 id, 'Failed' caption, 2 sort_order
		) s
		ON e.entry_status_id = s.id) conversions
	UNION
	
	SELECT * FROM ( 
	SELECT 	'Storage MB' measure, 
		'Additional daily' Classification, 
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_storage, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_storage, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_storage, 0))/5 last_5_days,
		SUM(count_storage)/30 last_30_days,
		7 outer_order, 1 inner_order
		FROM kalturadw.dwh_aggr_partner 
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id) STORAGE
	
	UNION 
	
	SELECT * FROM ( 
	SELECT 'Playback' Measure, 
		classification, 
		yesterday, the_day_before, last_5_days, last_30_days,
		8 outer_order, sort_order inner_order
	FROM (
		SELECT 'Playback' classification, 
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays, 0))/5 last_5_days,
		SUM(count_plays)/30 last_30_days, 1 sort_order
		FROM kalturadw.dwh_aggr_events_entry
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
		
		UNION
		SELECT '25%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_25, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_25, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_25, 0))/5 last_5_days,
		SUM(count_plays_25)/30 last_30_days, 2 sort_order
		FROM kalturadw.dwh_aggr_events_entry
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
		UNION 
		SELECT '50%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_50, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_50, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_50, 0))/5 last_5_days,
		SUM(count_plays_50)/30 last_30_days, 3 sort_order
		FROM kalturadw.dwh_aggr_events_entry
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
	
		UNION 
	
		SELECT '75%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_75, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_75, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_75, 0))/5 last_5_days,
		SUM(count_plays_75)/30 last_30_days, 4 sort_order
		FROM kalturadw.dwh_aggr_events_entry
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
		UNION 
		SELECT '100%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_100, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_100, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_100, 0))/5 last_5_days,
		SUM(count_plays_100)/30 last_30_days, 5 sort_order
		FROM kalturadw.dwh_aggr_events_entry
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
	) playback ) playback
	
	UNION
	SELECT * FROM ( 
	SELECT 'Registrations' measure, t.caption classification, 
		yesterday, 
		the_day_before, 
		last_5_days, 
		last_30_days, 9 outer_order, sort_order inner_order 
	FROM
	(
		SELECT 	IF(partner_type_id NOT IN (1, 102, 101, 104, 106, 103), 2, partner_type_id) partner_type_id,
			SUM(IF (created_at BETWEEN DATE(yesterday_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0)) yesterday,
			SUM(IF (created_at BETWEEN DATE(the_day_before_yesteray_date_id) AND DATE(the_day_before_yesteray_date_id) + INTERVAL 1 DAY, 1, 0)) the_day_before,
			SUM(IF (created_at BETWEEN DATE(5_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY, 1, 0))/5 last_5_days,
			COUNT(*)/30 last_30_days
		FROM kalturadw.dwh_dim_partners
		WHERE created_at BETWEEN DATE(30_days_ago_date_id) AND DATE(yesterday_date_id) + INTERVAL 1 DAY
		GROUP BY IF(partner_type_id NOT IN (1, 102, 101, 104, 106, 103), 2, partner_type_id)
	) p
	RIGHT OUTER JOIN 
	(
		SELECT partner_type_id, 
		CASE partner_type_name
			WHEN 'KMC_SIGNUP' THEN 'Kaltura'
			WHEN 'DRUPAL' THEN 'Drupal'
			WHEN 'WORDPRESS' THEN 'WordPress'
			WHEN 'MODDLE' THEN 'Moodle'
			WHEN 'JOOMLA ' THEN 'Joomla'
			WHEN 'MIND_TOUCH' THEN 'MindTouch'
			ELSE 'Other' 
		END caption,
		CASE partner_type_name
			WHEN 'KMC_SIGNUP' THEN 1
			WHEN 'DRUPAL' THEN 2
			WHEN 'WORDPRESS' THEN 3
			WHEN 'MOODLE' THEN 4
			WHEN 'JOOMLA ' THEN 5
			WHEN 'MIND_TOUCH' THEN 6
			ELSE 7 
		END sort_order, 
		partner_type_name
		FROM kalturadw.dwh_dim_partner_type WHERE partner_type_id NOT IN (0,-1, 105, 100)
	) t
	ON (p.partner_type_id = t.partner_type_id)) Registrations
	
	
	UNION
	SELECT * FROM ( 
	SELECT 	'Bandwidth (MB)' measure, 
		caption classification, 
		yesterday,
		the_day_before,
		last_5_days,
		last_30_days, 10 outer_order, sort_order inner_order 
		FROM 
		(
			SELECT partner_sub_activity_id,
			SUM(IF (activity_date_id BETWEEN (DATE(yesterday_date_id) - INTERVAL 3 DAY)*1 AND (DATE(yesterday_date_id) - INTERVAL 3 DAY)*1 , amount, 0))/1024 yesterday,
			SUM(IF (activity_date_id BETWEEN (DATE(the_day_before_yesteray_date_id) - INTERVAL 3 DAY)*1 AND (DATE(the_day_before_yesteray_date_id) - INTERVAL 3 DAY)*1 , amount, 0))/1024 the_day_before,
			SUM(IF (activity_date_id BETWEEN (DATE(5_days_ago_date_id) - INTERVAL 3 DAY)*1 AND (DATE(yesterday_date_id) - INTERVAL 3 DAY)*1 , amount, 0))/5/1024 last_5_days,
			SUM(amount)/30/1024 last_30_days
			FROM kalturadw.dwh_fact_partner_activities 
			WHERE partner_activity_id = 1 AND activity_date_id BETWEEN (DATE(30_days_ago_date_id) - INTERVAL 3 DAY)*1 AND (DATE(yesterday_date_id) - INTERVAL 3 DAY)*1 
			GROUP BY partner_sub_activity_id
		) bandwidth 
		RIGHT OUTER JOIN
		(
			SELECT 1 id, 'www.kaltura.com' caption, 4 sort_order
			UNION 
			SELECT 2 id, 'Limelight' caption, 2 sort_order
			UNION 
			SELECT 3 id, 'Level3' caption, 3 sort_order
			UNION 
			SELECT 4 id, 'Akamai' caption, 1 sort_order
		) filler
		ON (bandwidth.partner_sub_activity_id = filler.id)) Bandwidth) all_tables
		ON DUPLICATE KEY UPDATE 
			yesterday = VALUES(yesterday), 
			the_day_before = VALUES(the_day_before), 
			diff = VALUES(diff), 
			last_5_days_avg = VALUES(last_5_days_avg), 
			last_30_days_avg = VALUES(last_30_days_avg),
			outer_order = VALUES(outer_order),
			inner_order = VALUES(inner_order);
		
		SELECT 	measure AS Measure, 
		classification AS Classification, 
		DATE AS 'Report Date', 
		FORMAT(yesterday, 2) AS 'Yesterday', 
		FORMAT(the_day_before, 2) AS 'Day Before Yesterday', 
		CONCAT(diff, '%') AS 'Diff',
		FORMAT(last_5_days_avg, 2) AS 'Last 5 Days (AVG)', 
		FORMAT(last_30_days_avg, 2) AS 'Last 30 Days (AVG)'
		FROM kalturadw.dwh_daily_usage_reports
		WHERE DATE = IF (measure = 'Bandwidth (MB)',DATE(p_date_val) - INTERVAL 3 DAY, DATE(p_date_val))
		ORDER BY outer_order, inner_order;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `move_innodb_to_archive` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `move_innodb_to_archive`()
BEGIN
	DECLARE v_partition VARCHAR(256);
	DECLARE v_column VARCHAR(256);
	DECLARE v_from_archive DATE;
	DECLARE v_date_val INT;
	DECLARE v_table_name VARCHAR(256);
	DECLARE v_archive_name VARCHAR(256);
	DECLARE v_exists INT DEFAULT 0;
	DECLARE done INT DEFAULT 0;
	DECLARE c_partitions 
	CURSOR FOR 
	SELECT partition_name, p.table_name, CONCAT(p.table_name,'_archive') archive_name, partition_expression column_name, DATE(partition_description)*1 date_val
	FROM information_schema.PARTITIONS p, kalturadw_ds.retention_policy r
	WHERE LENGTH(partition_description) = 8 
    AND DATE(partition_description)*1 IS NOT NULL
    AND partition_description <= DATE(NOW() - INTERVAL r.archive_start_days_back DAY)*1
	AND p.table_name = r.table_name
	ORDER BY date_val;
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	
	OPEN c_partitions;
	
	read_loop: LOOP
		FETCH c_partitions INTO v_partition, v_table_name, v_archive_name, v_column, v_date_val;
		IF done THEN
		  LEAVE read_loop;
		END IF;
		
		SELECT COUNT(*)
		INTO v_exists
		FROM information_schema.PARTITIONS p
		WHERE p.partition_description = v_date_val
		AND p.table_name = v_archive_name;
		
		IF (v_exists > 0) THEN 
			SET @s = CONCAT('ALTER TABLE ',v_archive_name,' DROP PARTITION ', v_partition);
		
                        PREPARE stmt FROM @s;
                        EXECUTE stmt;
                        DEALLOCATE PREPARE stmt;
		END IF;
		
		SET @s = CONCAT('ALTER TABLE ',v_archive_name,' ADD PARTITION (PARTITION ',v_partition,' VALUES LESS THAN (',v_date_val,'))');
		
		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @s = CONCAT('INSERT INTO ',v_archive_name,' SELECT * FROM ',v_table_name,' WHERE ', v_column ,' < ',v_date_val);
		
		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @s = CONCAT('ALTER TABLE ',v_table_name,' DROP PARTITION ',v_partition);
		
		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		UPDATE kalturadw_ds.retention_policy
		SET archive_last_partition = DATE(v_date_val)
		WHERE table_name = v_table_name;
		
	END LOOP;
	
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `populate_time_dim` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`etl`@`localhost`*/ /*!50003 PROCEDURE `populate_time_dim`(start_date datetime, end_date datetime)
    DETERMINISTIC
BEGIN    

    WHILE start_date <= end_date DO
        INSERT IGNORE INTO kalturadw.dwh_dim_time 
        SELECT 1*DATE(d), d, YEAR(d), MONTH(d), DAYOFYEAR(d),DAYOFMONTH(d),DAYOFWEEK(d),WEEK(d),DAYNAME(d),DATE_FORMAT(d,'%a'),MONTHNAME(d), DATE_FORMAT(d, '%b'), QUARTER(d)
        FROM(SELECT start_date d) a;
        
        SET start_date = DATE_ADD(start_date, INTERVAL 1 DAY);
    END WHILE;
    
END */;;
DELIMITER ;
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `post_aggregation_partner`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `post_aggregation_partner`(date_val DATE, p_hour_id INT(11))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	
	SELECT aggr_table INTO v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'partner';
	
	SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		date_id, 
		hour_id,
		new_videos,
		new_images,
		new_audios,
		new_livestreams,
		new_playlists,
		new_documents,
		new_other_entries)
    		SELECT partner_id,DATE(''',date_val,''')*1 date_id, ', p_hour_id, ' hour_id,
    			SUM(IF(entry_type_id = 1 AND entry_media_type_id = 1, 1,0)) new_videos,
    			SUM(IF(entry_type_id = 1 AND entry_media_type_id = 2, 1,0)) new_images,
    			SUM(IF(entry_type_id = 1 AND entry_media_type_id = 5, 1,0)) new_audios,
			SUM(IF(entry_type_id = 7, 1,0)) new_livestreams,
			SUM(IF(entry_type_id = 5, 1,0)) new_playlists,
			SUM(IF(entry_type_id = 10, 1,0)) new_documents,
			SUM(IF(entry_type_id NOT IN (1,5,7,10) or (entry_type_id = 1 and entry_media_type_id NOT IN (1,2,5)), 1, 0)) new_other_entries
    		FROM dwh_dim_entries  en 
    		WHERE en.created_at between DATE(''',date_val,''') + INTERVAL ', p_hour_id, ' HOUR ',' 
					   AND DATE(''',date_val,''') + INTERVAL ', p_hour_id, ' + 1 HOUR - INTERVAL 1 SECOND ','
    	
		GROUP BY partner_id
    	ON DUPLICATE KEY UPDATE
    		new_videos=VALUES(new_videos),
		new_images=VALUES(new_images),
		new_audios=VALUES(new_audios),
		new_livestreams=VALUES(new_livestreams),
		new_playlists=VALUES(new_playlists),
		new_documents=VALUES(new_documents),
		new_other_entries=VALUES(new_other_entries);
    	');
	
 
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		 date_id, 
		 hour_id,
		 deleted_audios,
		 deleted_images,
		 deleted_videos,
		 deleted_documents,
		 deleted_livestreams,
		 deleted_playlists,
		 deleted_other_entries)
		SELECT  partner_id,DATE(''',date_val,''')*1 date_id, ', p_hour_id, ' hour_id,
			SUM(IF(entry_type_id = 1 AND entry_media_type_id = 1, 1,0)) deleted_videos,
			SUM(IF(entry_type_id = 1 AND entry_media_type_id = 2, 1,0)) deleted_images,
			SUM(IF(entry_type_id = 1 AND entry_media_type_id = 5, 1,0)) deleted_audios,
			SUM(IF(entry_type_id = 7, 1,0)) deleted_livestreams,
			SUM(IF(entry_type_id = 5, 1,0)) deleted_playlists,
			SUM(IF(entry_type_id = 10, 1,0)) deleted_documents,
			SUM(IF(entry_type_id NOT IN (1,5,7,10) or (entry_type_id = 1 and entry_media_type_id NOT IN (1,2,5)), 1, 0)) deleted_other_entries
		FROM 	dwh_dim_entries  en 
    		WHERE 	entry_status_id = 3
    			AND en.modified_at between DATE(''',date_val,''') + INTERVAL ', p_hour_id, ' HOUR ',' 
					   AND DATE(''',date_val,''') + INTERVAL ', p_hour_id, ' + 1 HOUR - INTERVAL 1 SECOND ','
    		GROUP BY partner_id
		ON DUPLICATE KEY UPDATE
			deleted_videos=VALUES(deleted_videos),
			deleted_images=VALUES(deleted_images),
			deleted_audios=VALUES(deleted_audios),
			deleted_livestreams=VALUES(deleted_livestreams),
			deleted_playlists=VALUES(deleted_playlists),
			deleted_documents=VALUES(deleted_documents),
			deleted_other_entries=VALUES(deleted_other_entries);
    	');

	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		 date_id, 
		 hour_id,
    		 new_admins)
    	SELECT  partner_id, DATE(''',date_val,''')*1 date_id, ', p_hour_id, ' hour_id, count(*) new_admins
    	FROM dwh_dim_kusers  ku
    	WHERE ku.created_at between DATE(''',date_val,''') + INTERVAL ', p_hour_id, ' HOUR ',' 
					   AND DATE(''',date_val,''') + INTERVAL ', p_hour_id, ' + 1 HOUR - INTERVAL 1 SECOND ','
		and is_admin = 1
   		GROUP BY partner_id
    	ON DUPLICATE KEY UPDATE
		new_admins=VALUES(new_admins) ;
        ');
	
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `reaggregate_post_data_partner`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `reaggregate_post_data_partner`()
BEGIN
    DECLARE v_date_id INT;
    DECLARE v_hour_id INT;
    
    DECLARE done INT DEFAULT 0;
    DECLARE aggrs CURSOR FOR SELECT aggr_day_int, hour_id FROM aggr_managment WHERE aggr_name = 'partner' AND is_calculated = 1;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    OPEN aggrs;
    read_loop: LOOP
        FETCH aggrs INTO v_date_id, v_hour_id;
        IF done THEN
             LEAVE read_loop;
	END IF;
        
	CALL post_aggregation_partner(v_date, v_hour_id);
	
    END LOOP;
END$$

DELIMITER ;


CALL reaggregate_post_data_partner();

DROP PROCEDURE reaggregate_post_data_partner;

/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `post_aggregation_widget` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`root`@`localhost`*/ /*!50003 PROCEDURE `post_aggregation_widget`(date_val DATE, p_hour_id INT(11))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
    SELECT aggr_table INTO  v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'widget';
	
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		date_id, 
		hour_id,
		widget_id,
     		count_widget_loads)
    	SELECT  partner_id,event_date_id,HOUR(event_time),widget_id,
    		SUM(IF(event_type_id=1,1,NULL)) count_widget_loads
		FROM dwh_fact_events  ev
		WHERE event_type_id = 1 AND event_date_id = DATE(''',date_val,''')*1 and event_hour_id = ', p_hour_id, '
		GROUP BY partner_id, event_date_id, event_hour_id, widget_id
		ON DUPLICATE KEY UPDATE
    		count_widget_loads=VALUES(count_widget_loads);
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;
/*!50003 DROP PROCEDURE IF EXISTS `recalc_aggr_day` */;
/*!50003 SET @saved_cs_client      = @@character_set_client */ ;
/*!50003 SET @saved_cs_results     = @@character_set_results */ ;
/*!50003 SET @saved_col_connection = @@collation_connection */ ;
/*!50003 SET character_set_client  = latin1 */ ;
/*!50003 SET character_set_results = latin1 */ ;
/*!50003 SET collation_connection  = latin1_swedish_ci */ ;
/*!50003 SET @saved_sql_mode       = @@sql_mode */ ;
/*!50003 SET sql_mode              = '' */ ;
DELIMITER ;;
/*!50003 CREATE*/ /*!50020 DEFINER=`root`@`localhost`*/ /*!50003 PROCEDURE `recalc_aggr_day`(p_date_val DATE, p_hour_id INT(11),p_aggr_name VARCHAR(100))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	DECLARE v_aggr_id_field VARCHAR(100);
	DECLARE v_ignore DATE;
		
	SELECT date(now() - interval archive_delete_days_back day)
	INTO v_ignore
	FROM kalturadw_ds.retention_policy
	WHERE table_name = 'dwh_fact_events';	
	
	IF (p_date_val >= v_ignore) THEN 
	
		SELECT aggr_table, aggr_id_field
		INTO  v_aggr_table, v_aggr_id_field
		FROM kalturadw_ds.aggr_name_resolver
		WHERE aggr_name = p_aggr_name;	
		
		IF (v_aggr_table <> '') THEN 
			SET @s = CONCAT('delete from ',v_aggr_table,'
				where date_id = DATE(''',p_date_val,''')*1 and hour_id = ',p_hour_id);
			PREPARE stmt FROM  @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;	
		END IF;
		
		SET @s = CONCAT('INSERT INTO aggr_managment(aggr_name, aggr_day, aggr_day_int, hour_id, is_calculated)
		VALUES(''',p_aggr_name,''',''',p_date_val,''',''',date(p_date_val)*1,''',',p_hour_id,',0)
		ON DUPLICATE KEY UPDATE is_calculated = 0');

		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		CALL calc_aggr_day(p_date_val,p_hour_id,p_aggr_name);
	
	END IF; 
	
END */;;
DELIMITER ;
/*!50003 SET sql_mode              = @saved_sql_mode */ ;
/*!50003 SET character_set_client  = @saved_cs_client */ ;
/*!50003 SET character_set_results = @saved_cs_results */ ;
/*!50003 SET collation_connection  = @saved_col_connection */ ;

--
-- Final view structure for view `dwh_aggr_events_entry_partitions`
--

/*!50001 DROP TABLE IF EXISTS `dwh_aggr_events_entry_partitions`*/;
/*!50001 DROP VIEW IF EXISTS `dwh_aggr_events_entry_partitions`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `dwh_aggr_events_entry_partitions` AS select `partitions`.`TABLE_NAME` AS `table_name`,`partitions`.`PARTITION_NAME` AS `partition_name`,`partitions`.`PARTITION_DESCRIPTION` AS `partition_description`,`partitions`.`TABLE_ROWS` AS `table_rows`,`partitions`.`CREATE_TIME` AS `create_time` from `information_schema`.`partitions` where (`partitions`.`TABLE_NAME` in ('dwh_aggr_events_entry','dwh_aggr_events_country','dwh_aggr_events_domain','dwh_aggr_partner','dwh_aggr_events_widget')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dwh_dim_countries`
--

/*!50001 DROP TABLE IF EXISTS `dwh_dim_countries`*/;
/*!50001 DROP VIEW IF EXISTS `dwh_dim_countries`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `dwh_dim_countries` AS (select `dwh_dim_locations`.`country` AS `country`,`dwh_dim_locations`.`country_id` AS `country_id` from `dwh_dim_locations` where (`dwh_dim_locations`.`location_type_name` = 'country')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dwh_dim_countries_states`
--

/*!50001 DROP TABLE IF EXISTS `dwh_dim_countries_states`*/;
/*!50001 DROP VIEW IF EXISTS `dwh_dim_countries_states`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `dwh_dim_countries_states` AS (select `dwh_dim_locations`.`country` AS `country`,`dwh_dim_locations`.`country_id` AS `country_id`,`dwh_dim_locations`.`state` AS `state`,`dwh_dim_locations`.`state_id` AS `state_id` from `dwh_dim_locations` where (`dwh_dim_locations`.`location_type_name` = 'state')) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dwh_dim_entries_v`
--

/*!50001 DROP TABLE IF EXISTS `dwh_dim_entries_v`*/;
/*!50001 DROP VIEW IF EXISTS `dwh_dim_entries_v`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `dwh_dim_entries_v` AS select `a`.`entry_id` AS `entry_id`,`a`.`entry_name` AS `entry_name`,`a`.`partner_id` AS `partner_id`,`a`.`entry_source_id` AS `entry_source_id`,`a`.`created_at` AS `created_at`,`a`.`created_date_id` AS `created_date_id`,`a`.`created_hour_id` AS `created_hour_id`,`a`.`updated_date_id` AS `updated_date_id`,`a`.`updated_hour_id` AS `updated_hour_id`,`a`.`entry_type_id` AS `entry_type_id`,`c`.`entry_type_name` AS `entry_type_Name`,`b`.`entry_status_id` AS `entry_status_id`,`b`.`entry_status_name` AS `entry_status_Name`,`d`.`entry_media_type_id` AS `entry_media_type_id`,`e`.`partner_name` AS `partner_name`,`d`.`entry_media_type_name` AS `entry_media_type_name` from ((((`dwh_dim_entries` `a` left join `dwh_dim_entry_status` `b` on((`a`.`entry_status_id` = `b`.`entry_status_id`))) left join `dwh_dim_entry_type` `c` on((`a`.`entry_type_id` = `c`.`entry_type_id`))) left join `dwh_dim_entry_media_type` `d` on((`a`.`entry_media_type_id` = `d`.`entry_media_type_id`))) left join `dwh_dim_partners` `e` on((`a`.`partner_id` = `e`.`partner_id`))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dwh_dim_partners_v`
--

/*!50001 DROP TABLE IF EXISTS `dwh_dim_partners_v`*/;
/*!50001 DROP VIEW IF EXISTS `dwh_dim_partners_v`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `dwh_dim_partners_v` AS (select `a`.`partner_id` AS `partner_id`,`a`.`partner_name` AS `partner_name`,`a`.`url1` AS `url1`,`a`.`url2` AS `url2`,`a`.`secret` AS `secret`,`a`.`admin_secret` AS `admin_secret`,`a`.`max_number_of_hits_per_day` AS `max_number_of_hits_per_day`,`a`.`appear_in_search` AS `appear_in_search`,`a`.`debug_level` AS `debug_level`,`a`.`invalid_login_count` AS `invalid_login_count`,`a`.`created_at` AS `created_at`,`a`.`created_date_id` AS `created_date_id`,`a`.`created_hour_id` AS `created_hour_id`,`a`.`updated_at` AS `updated_at`,`a`.`updated_date_id` AS `updated_date_id`,`a`.`updated_hour_id` AS `updated_hour_id`,`a`.`partner_alias` AS `partner_alias`,`a`.`anonymous_kuser_id` AS `anonymous_kuser_id`,`a`.`ks_max_expiry_in_seconds` AS `ks_max_expiry_in_seconds`,`a`.`create_user_on_demand` AS `create_user_on_demand`,`a`.`prefix` AS `prefix`,`a`.`admin_name` AS `admin_name`,`a`.`admin_email` AS `admin_email`,`a`.`description` AS `description`,`a`.`commercial_use` AS `commercial_use`,`a`.`moderate_content` AS `moderate_content`,`a`.`notify` AS `notify`,`a`.`custom_data` AS `custom_data`,`a`.`service_config_id` AS `service_config_id`,`a`.`partner_status_id` AS `partner_status_id`,`b`.`partner_status_name` AS `partner_status_name`,`a`.`content_categories` AS `content_categories`,`a`.`partner_type_id` AS `partner_type_id`,`c`.`partner_type_name` AS `partner_type_name`,`a`.`phone` AS `phone`,`a`.`describe_yourself` AS `describe_yourself`,`a`.`adult_content` AS `adult_content`,`a`.`partner_package` AS `partner_package`,`a`.`usage_percent` AS `usage_percent`,`a`.`storage_usage` AS `storage_usage`,`a`.`eighty_percent_warning` AS `eighty_percent_warning`,`a`.`usage_limit_warning` AS `usage_limit_warning`,`a`.`dwh_creation_date` AS `dwh_creation_date`,`a`.`dwh_update_date` AS `dwh_update_date`,`a`.`ri_ind` AS `ri_ind`,`a`.`priority_group_id` AS `priority_group_id`,`a`.`work_group_id` AS `work_group_id`,`a`.`partner_group_type_id` AS `partner_group_type_id`,`d`.`partner_group_type_name` AS `partner_group_type_name`,`a`.`partner_parent_id` AS `partner_parent_id` from (((`dwh_dim_partners` `a` left join `dwh_dim_partner_status` `b` on((`a`.`partner_status_id` = `b`.`partner_status_id`))) left join `dwh_dim_partner_type` `c` on((`a`.`partner_type_id` = `c`.`partner_type_id`))) left join `dwh_dim_partner_group_type` `d` on((`a`.`partner_group_type_id` = `d`.`partner_group_type_id`)))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dwh_dim_referrer`
--

/*!50001 DROP TABLE IF EXISTS `dwh_dim_referrer`*/;
/*!50001 DROP VIEW IF EXISTS `dwh_dim_referrer`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `dwh_dim_referrer` AS select `r`.`referrer_id` AS `referrer_id`,if((trim(ifnull(`d`.`domain_name`,'')) = ''),`r`.`referrer`,concat(`d`.`domain_name`,'/',ifnull(`r`.`referrer`,''))) AS `referrer` from (`dwh_dim_domain_referrer` `r` join `dwh_dim_domain` `d`) where (`r`.`domain_id` = `d`.`domain_id`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `dwh_dim_ui_conf_v`
--

/*!50001 DROP TABLE IF EXISTS `dwh_dim_ui_conf_v`*/;
/*!50001 DROP VIEW IF EXISTS `dwh_dim_ui_conf_v`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`%` SQL SECURITY DEFINER */
/*!50001 VIEW `dwh_dim_ui_conf_v` AS (select `a`.`ui_conf_id` AS `ui_conf_id`,`a`.`ui_conf_type_id` AS `ui_conf_type_id`,`a`.`partner_id` AS `partner_id`,`a`.`subp_id` AS `subp_id`,`a`.`conf_file_path` AS `conf_file_path`,`a`.`ui_conf_name` AS `ui_conf_name`,`a`.`width` AS `width`,`a`.`height` AS `height`,`a`.`html_params` AS `html_params`,`a`.`swf_url` AS `swf_url`,`a`.`created_at` AS `created_at`,`a`.`created_date_id` AS `created_date_id`,`a`.`created_hour_id` AS `created_hour_id`,`a`.`updated_at` AS `updated_at`,`a`.`updated_date_id` AS `updated_date_id`,`a`.`updated_hour_id` AS `updated_hour_id`,`a`.`conf_vars` AS `conf_vars`,`a`.`use_cdn` AS `use_cdn`,`a`.`tags` AS `tags`,`a`.`custom_data` AS `custom_data`,`a`.`UI_Conf_Status_ID` AS `UI_Conf_Status_ID`,`a`.`description` AS `description`,`a`.`display_in_search` AS `display_in_search`,`a`.`dwh_creation_date` AS `dwh_creation_date`,`a`.`dwh_update_date` AS `dwh_update_date`,`a`.`ri_ind` AS `ri_ind`,`b`.`ui_conf_status_name` AS `ui_conf_status_name`,`c`.`ui_conf_type_name` AS `ui_conf_type_name` from ((`dwh_dim_ui_conf` `a` left join `dwh_dim_ui_conf_status` `b` on((`a`.`UI_Conf_Status_ID` = `b`.`ui_conf_status_id`))) left join `dwh_dim_ui_conf_type` `c` on((`a`.`ui_conf_type_id` = `c`.`ui_conf_type_id`)))) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ri_defaults_grouped`
--

/*!50001 DROP TABLE IF EXISTS `ri_defaults_grouped`*/;
/*!50001 DROP VIEW IF EXISTS `ri_defaults_grouped`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `ri_defaults_grouped` AS (select `ri_defaults`.`table_name` AS `table_name`,group_concat(`ri_defaults`.`default_field` order by `ri_defaults`.`default_field` ASC separator ',') AS `default_fields`,group_concat(`ri_defaults`.`default_value` order by `ri_defaults`.`default_field` ASC separator ',') AS `default_values` from `ri_defaults` group by `ri_defaults`.`table_name`) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

--
-- Final view structure for view `ri_mapping_and_defaults`
--

/*!50001 DROP TABLE IF EXISTS `ri_mapping_and_defaults`*/;
/*!50001 DROP VIEW IF EXISTS `ri_mapping_and_defaults`*/;
/*!50001 SET @saved_cs_client          = @@character_set_client */;
/*!50001 SET @saved_cs_results         = @@character_set_results */;
/*!50001 SET @saved_col_connection     = @@collation_connection */;
/*!50001 SET character_set_client      = latin1 */;
/*!50001 SET character_set_results     = latin1 */;
/*!50001 SET collation_connection      = latin1_swedish_ci */;
/*!50001 CREATE ALGORITHM=UNDEFINED */
/*!50013 DEFINER=`etl`@`localhost` SQL SECURITY DEFINER */
/*!50001 VIEW `ri_mapping_and_defaults` AS (select `m`.`table_name` AS `table_name`,`m`.`column_name` AS `column_name`,`m`.`date_id_column_name` AS `date_id_column_name`,`m`.`date_column_name` AS `date_column_name`,`m`.`reference_table` AS `reference_table`,`m`.`reference_column` AS `reference_column`,`m`.`perform_check` AS `perform_check`,`dg`.`default_fields` AS `default_fields`,`dg`.`default_values` AS `default_values` from (`ri_mapping` `m` join `ri_defaults_grouped` `dg`) where (convert(`m`.`reference_table` using utf8) = `dg`.`table_name`)) */;
/*!50001 SET character_set_client      = @saved_cs_client */;
/*!50001 SET character_set_results     = @saved_cs_results */;
/*!50001 SET collation_connection      = @saved_col_connection */;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `post_aggregation_devices`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `post_aggregation_devices`(date_val DATE, p_hour_id INT(11))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	
	SELECT aggr_table INTO v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'devices';
	
	IF (p_hour_id = 0) THEN 
		SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
				(partner_id, date_id, hour_id, country_id, location_id, count_bandwidth_kb)
				SELECT 	partner_id, DATE(''',date_val,''')*1 date_id, ', p_hour_id, ', country_id,location_id, IFNULL(SUM(bandwidth_bytes), 0) / 1024 count_bandwidth_kb
				FROM dwh_fact_bandwidth_usage  b
				WHERE b.activity_date_id = DATE(''',date_val,''')*1
		GROUP BY partner_id, country_id,location_id
		ON DUPLICATE KEY UPDATE
			count_bandwidth_kb=VALUES(count_bandwidth_kb);
		');
	 
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
				(partner_id, date_id, hour_id, country_id, location_id, count_bandwidth_kb)
				SELECT 	session_partner_id, DATE(''',date_val,''')*1 date_id, ', p_hour_id, ', session_client_country_id,session_client_location_id, IFNULL(SUM(total_bytes), 0) / 1024 count_bandwidth_kb
				FROM dwh_fact_fms_sessions  b
				WHERE b.session_date_id = DATE(''',date_val,''')*1
		GROUP BY session_partner_id, session_client_country_id,session_client_location_id
		ON DUPLICATE KEY UPDATE
			count_bandwidth_kb=count_bandwidth_kb+VALUES(count_bandwidth_kb);
		');
	 
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	END IF;
	
END$$

DELIMITER ;

DELIMITER $$

USE kalturadw$$

DROP VIEW IF EXISTS dwh_view_partners_monthly_billing_last_updated_at$$
CREATE VIEW `kalturadw`.`dwh_view_partners_monthly_billing_last_updated_at` AS (

	SELECT  FLOOR(months.day_id / 100) AS month_id,
		p.partner_id AS partner_id,
		MAX(p.updated_at) AS updated_at
	FROM dwh_dim_time months, dwh_dim_partners_billing p 
	WHERE 	p.updated_at <= LAST_DAY(months.day_id) 
		AND months.day_id = LAST_DAY(months.day_id)*1
	GROUP BY FLOOR(months.day_id/100),p.partner_id

)$$

DELIMITER ;

DROP VIEW IF EXISTS kalturadw.dwh_view_partners_monthly_billing;
CREATE VIEW `kalturadw`.`dwh_view_partners_monthly_billing` 
    AS
(
	SELECT max_blling_updated.month_id, billing.* 
	FROM kalturadw.dwh_view_partners_monthly_billing_last_updated_at max_blling_updated, 
		kalturadw.dwh_dim_partners_billing billing
	WHERE max_blling_updated.partner_id = billing.partner_id
	AND max_blling_updated.updated_at = billing.updated_at
);

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_partner_overage`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_partner_overage`(p_month_id INTEGER)
BEGIN
		DROP TABLE IF EXISTS partner_quotas;
		CREATE TEMPORARY TABLE partner_quotas AS
		SELECT 	q.partner_id partner_id,
			IFNULL(p.partner_id,q.partner_id) usage_partner_id,
			q.max_monthly_bandwidth_kb,
			q.charge_monthly_bandwidth_kb_usd,
			q.charge_monthly_bandwidth_kb_unit,
			q.max_monthly_storage_mb,
			q.charge_monthly_storage_mb_usd,
			q.charge_monthly_storage_mb_unit,
			q.max_monthly_total_usage_mb,
			q.charge_monthly_total_usage_mb_usd,
			q.charge_monthly_total_usage_mb_unit,
			q.max_monthly_plays,
			q.charge_monthly_plays_usd,
			q.charge_monthly_plays_unit,
			q.max_monthly_entries,
			q.charge_monthly_entries_usd,
			q.charge_monthly_entries_unit
		FROM dwh_view_partners_monthly_billing q 
			LEFT OUTER JOIN kalturadw.dwh_dim_partners p ON (q.partner_id = p.partner_parent_id AND q.partner_group_type_id = 3)
		WHERE is_active = 1 AND q.month_id = p_month_id
        GROUP BY partner_id, usage_partner_id;
        

		DROP TABLE IF EXISTS partner_overages;
		CREATE TEMPORARY TABLE partner_overages (
				    partner_id       			INT(11),
				    included_bandwidth_kb    		BIGINT(20),
				    actual_bandwidth_kb	 		BIGINT(20),
				    charge_overage_bandwidth_kb		DECIMAL(15,3),
				    included_storage_mb    		BIGINT(20),
				    actual_storage_mb	 		BIGINT(20),
				    charge_overage_storage_mb 		DECIMAL(15,3),
				    included_total_usage_mb    		BIGINT(20),
				    actual_total_usage_mb		BIGINT(20),
				    charge_overage_total_usage_mb 	DECIMAL(15,3),
				    included_entries			BIGINT(20),
				    actual_entries			BIGINT(20),
				    charge_overage_entries		DECIMAL(15,3),
				    included_plays			BIGINT(20),
				    actual_plays			BIGINT(20),
				    charge_overage_plays		DECIMAL(15,3),
				    PRIMARY KEY (partner_id)
				  ) ENGINE = MEMORY;
	
		INSERT INTO partner_overages (partner_id, included_bandwidth_kb, actual_bandwidth_kb, charge_overage_bandwidth_kb, included_storage_mb, actual_storage_mb, charge_overage_storage_mb, included_total_usage_mb, actual_total_usage_mb, charge_overage_total_usage_mb)
			SELECT partner_id,
				included_bandwidth_kb,actual_bandwidth_kb,
				get_overage_charge(included_bandwidth_kb, actual_bandwidth_kb, charge_monthly_bandwidth_kb_unit, charge_monthly_bandwidth_kb_usd) charge_overage_bandwidth_kb,
				included_storage_mb, actual_storage_mb,
				get_overage_charge(included_storage_mb, actual_storage_mb, charge_monthly_storage_mb_unit, charge_monthly_storage_mb_usd) charge_overage_storage_mb, 
				included_total_usage_mb, actual_bandwidth_kb/1024+actual_storage_mb actual_total_usage_mb,
				get_overage_charge(included_total_usage_mb, actual_bandwidth_kb/1024+actual_storage_mb , charge_monthly_total_usage_mb_unit, charge_monthly_total_usage_mb_usd) charge_overage_total_usage_mb
			FROM
			(SELECT pq.partner_id, 
				MAX(max_monthly_bandwidth_kb) included_bandwidth_kb,
				IFNULL(SUM(count_bandwidth_kb),0) actual_bandwidth_kb,
				MAX(charge_monthly_bandwidth_kb_unit) charge_monthly_bandwidth_kb_unit,
				MAX(charge_monthly_bandwidth_kb_usd) charge_monthly_bandwidth_kb_usd,	
				MAX(max_monthly_storage_mb) included_storage_mb,
				IFNULL(calc_partner_monthly_storage(p_month_id , pq.partner_id),0) actual_storage_mb,
				MAX(charge_monthly_storage_mb_unit) charge_monthly_storage_mb_unit,
				MAX(charge_monthly_storage_mb_usd) charge_monthly_storage_mb_usd,
				MAX(max_monthly_total_usage_mb) included_total_usage_mb,
				MAX(charge_monthly_total_usage_mb_unit) charge_monthly_total_usage_mb_unit,
				MAX(charge_monthly_total_usage_mb_usd) charge_monthly_total_usage_mb_usd
			FROM partner_quotas pq LEFT OUTER JOIN kalturadw.dwh_hourly_partner_usage partner_usage 
					ON (pq.usage_partner_id = partner_usage.partner_id AND partner_usage.date_id BETWEEN p_month_id*100 AND p_month_id*100+31)
			GROUP BY pq.partner_id) inner_q
			WHERE actual_bandwidth_kb > included_bandwidth_kb OR 
				actual_storage_mb > included_storage_mb OR 
				actual_bandwidth_kb/1024+actual_storage_mb > included_total_usage_mb
		ON DUPLICATE KEY UPDATE 
			included_bandwidth_kb=VALUES(included_bandwidth_kb),
			actual_bandwidth_kb=VALUES(actual_bandwidth_kb),
			charge_overage_bandwidth_kb=VALUES(charge_overage_bandwidth_kb),
			included_storage_mb=VALUES(included_storage_mb),
			actual_storage_mb=VALUES(actual_storage_mb),
			charge_overage_storage_mb=VALUES(charge_overage_storage_mb),
			included_total_usage_mb=VALUES(included_total_usage_mb),
			actual_total_usage_mb=VALUES(actual_total_usage_mb),
			charge_overage_total_usage_mb=VALUES(charge_overage_total_usage_mb);
			
		INSERT INTO partner_overages (partner_id, included_entries, actual_entries, charge_overage_entries)
			SELECT 	pq.partner_id, 
				MAX(max_monthly_entries) included_entries,
				COUNT(entries.entry_id) actual_entries,
				get_overage_charge(MAX(max_monthly_entries), COUNT(entries.entry_id), MAX(charge_monthly_entries_unit), MAX(charge_monthly_entries_usd)) charge_overage_entries
			FROM partner_quotas pq, kalturadw.dwh_dim_entries entries
			WHERE 	pq.usage_partner_id = entries.partner_id AND 
				entries.created_at < LAST_DAY(DATE(p_month_id*100+1)) + INTERVAL 1 DAY AND
				entries.entry_type_id IN (1,7) 
				AND entry_status_id NOT IN (-2,-1,3) 
			GROUP BY pq.partner_id
			HAVING actual_entries > included_entries
		ON DUPLICATE KEY UPDATE 
			included_entries=VALUES(included_entries),
			actual_entries=VALUES(actual_entries),
			charge_overage_entries=VALUES(charge_overage_entries);
	
		INSERT INTO partner_overages (partner_id, included_plays, actual_plays, charge_overage_plays)
			SELECT 	pq.partner_id, 
				MAX(max_monthly_plays) included_plays,
				SUM(count_plays) actual_plays,
				get_overage_charge(MAX(max_monthly_plays), SUM(count_plays), MAX(charge_monthly_plays_unit), MAX(charge_monthly_plays_usd)) charge_overage_plays
			FROM partner_quotas pq, kalturadw.dwh_hourly_partner plays
			WHERE 	pq.usage_partner_id = plays.partner_id AND
				date_id BETWEEN p_month_id*100 AND p_month_id*100+31
			GROUP BY pq.partner_id
			HAVING 	SUM(count_plays) > MAX(max_monthly_plays)
		ON DUPLICATE KEY UPDATE 
			included_plays=VALUES(included_plays),
			actual_plays=VALUES(actual_plays),
			charge_overage_plays=VALUES(charge_overage_plays);
	
		SELECT 	IF(children.partner_group_type_id=3,children.partner_id,parents.partner_id) parent_partner_id,
			IF(children.partner_group_type_id=3,children.partner_name,parents.partner_name) parent_partner_name,
			group_types.partner_group_type_name,
			IF(children.partner_group_type_id=3,NULL,children.partner_id) publisher_id,
			IF(children.partner_group_type_id=3,NULL,children.partner_name) publisher_name,
			d_cos.partner_class_of_service_name,
			d_vertical.partner_vertical_name,
			included_bandwidth_kb,
			actual_bandwidth_kb,
			charge_overage_bandwidth_kb,
			included_storage_mb, 
			actual_storage_mb, 
			charge_overage_storage_mb, 
			included_total_usage_mb, 
			actual_total_usage_mb, 
			charge_overage_total_usage_mb,
			included_entries, 
			actual_entries, 
			charge_overage_entries,
			included_plays, 
			actual_plays, 
			charge_overage_plays			
		FROM partner_overages po 
		INNER JOIN kalturadw.dwh_dim_partners children ON (po.partner_id = children.partner_id)
		LEFT OUTER JOIN kalturadw.dwh_dim_partners parents ON (children.partner_parent_id = parents.partner_id)
		INNER JOIN kalturadw.dwh_dim_partner_group_type group_types ON (IF(parents.partner_id IS NOT NULL, parents.partner_group_type_id,children.partner_group_type_id) = group_types.partner_group_type_id)
		LEFT OUTER JOIN kalturadw.dwh_dim_partner_class_of_service d_cos ON (children.class_of_service_id = d_cos.partner_class_of_service_id)
		LEFT OUTER JOIN kalturadw.dwh_dim_partner_vertical d_vertical ON (children.vertical_id = d_vertical.partner_vertical_id)
		WHERE parents.partner_group_type_id <> 3 OR parents.partner_group_type_id IS NULL;
END$$

DELIMITER ;

CALL add_partitions();

ALTER TABLE dwh_hourly_partner
DROP COLUMN `count_bandwidth`,
DROP COLUMN `aggr_bandwidth`,
DROP COLUMN `count_storage`,
DROP COLUMN `aggr_storage`,
DROP COLUMN `count_streaming`,
DROP COLUMN `aggr_streaming`,
DROP KEY `date_id`,
ENGINE=InnoDB;

DROP TABLE IF EXISTS `dwh_hourly_partner_new`;

CREATE TABLE `dwh_hourly_partner_new` (
  `partner_id` int(11) NOT NULL DEFAULT '0',
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` int(11) NOT NULL DEFAULT '0',
  `sum_time_viewed` decimal(20,3) DEFAULT NULL,
  `count_time_viewed` int(11) DEFAULT NULL,
  `count_plays` int(11) DEFAULT NULL,
  `count_loads` int(11) DEFAULT NULL,
  `count_plays_25` int(11) DEFAULT NULL,
  `count_plays_50` int(11) DEFAULT NULL,
  `count_plays_75` int(11) DEFAULT NULL,
  `count_plays_100` int(11) DEFAULT NULL,
  `count_edit` int(11) DEFAULT NULL,
  `count_viral` int(11) DEFAULT NULL,
  `count_download` int(11) DEFAULT NULL,
  `count_report` int(11) DEFAULT NULL,
  `new_admins` INT(11) DEFAULT NULL,
  `new_videos` INT(11) DEFAULT NULL,
  `deleted_videos` INT(11) DEFAULT NULL,
  `new_images` INT(11) DEFAULT NULL,
  `deleted_images` INT(11) DEFAULT NULL,
  `new_audios` INT(11) DEFAULT NULL,
  `deleted_audios` INT(11) DEFAULT NULL,
  `new_livestreams` INT(11) DEFAULT NULL,
  `deleted_livestreams` INT(11) DEFAULT NULL,
  `new_playlists` INT(11) DEFAULT NULL,
  `deleted_playlists` INT(11) DEFAULT NULL,
  `new_documents` INT(11) DEFAULT NULL,
  `deleted_documents` INT(11) DEFAULT NULL,
  `new_other_entries` INT(11) DEFAULT NULL,
  `deleted_other_entries` INT(11) DEFAULT NULL,
  `flag_active_site` tinyint(4) DEFAULT '0',
  `flag_active_publisher` tinyint(4) DEFAULT '0',
  `count_buf_start` int(11) DEFAULT NULL,
  `count_buf_end` int(11) DEFAULT NULL,
  `count_open_full_screen` int(11) DEFAULT NULL,
  `count_close_full_screen` int(11) DEFAULT NULL,
  `count_replay` int(11) DEFAULT NULL,
  `count_seek` int(11) DEFAULT NULL,
  `count_open_upload` int(11) DEFAULT NULL,
  `count_save_publish` int(11) DEFAULT NULL,
  `count_close_editor` int(11) DEFAULT NULL,
  `count_pre_bumper_played` int(11) DEFAULT NULL,
  `count_post_bumper_played` int(11) DEFAULT NULL,
  `count_bumper_clicked` int(11) DEFAULT NULL,
  `count_preroll_started` int(11) DEFAULT NULL,
  `count_midroll_started` int(11) DEFAULT NULL,
  `count_postroll_started` int(11) DEFAULT NULL,
  `count_overlay_started` int(11) DEFAULT NULL,
  `count_preroll_clicked` int(11) DEFAULT NULL,
  `count_midroll_clicked` int(11) DEFAULT NULL,
  `count_postroll_clicked` int(11) DEFAULT NULL,
  `count_overlay_clicked` int(11) DEFAULT NULL,
  `count_preroll_25` int(11) DEFAULT NULL,
  `count_preroll_50` int(11) DEFAULT NULL,
  `count_preroll_75` int(11) DEFAULT NULL,
  `count_midroll_25` int(11) DEFAULT NULL,
  `count_midroll_50` int(11) DEFAULT NULL,
  `count_midroll_75` int(11) DEFAULT NULL,
  `count_postroll_25` int(11) DEFAULT NULL,
  `count_postroll_50` int(11) DEFAULT NULL,
  `count_postroll_75` int(11) DEFAULT NULL,
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(partition p_0 values less than (1))*/;

CALL apply_table_partitions_to_target_table('dwh_hourly_partner');

INSERT INTO kalturadw.dwh_hourly_partner_new
	(partner_id, date_id, hour_id, sum_time_viewed, count_time_viewed, count_plays, 
	count_loads, count_plays_25, count_plays_50, count_plays_75, count_plays_100, count_edit, 
	count_viral, count_download, count_report, flag_active_site, flag_active_publisher, count_buf_start, 
	count_buf_end, count_open_full_screen, count_close_full_screen, count_replay, count_seek, count_open_upload, 
	count_save_publish, count_close_editor, count_pre_bumper_played, count_post_bumper_played, 
	count_bumper_clicked, count_preroll_started, count_midroll_started, count_postroll_started,
	count_overlay_started, count_preroll_clicked, count_midroll_clicked, count_postroll_clicked, 
	count_overlay_clicked, count_preroll_25, count_preroll_50, count_preroll_75, count_midroll_25, 
	count_midroll_50, count_midroll_75, count_postroll_25, count_postroll_50, count_postroll_75)
SELECT 	partner_id, date_id, hour_id, sum_time_viewed, count_time_viewed, count_plays, 
	count_loads, count_plays_25, count_plays_50, count_plays_75, count_plays_100, count_edit, 
	count_viral, count_download, count_report, flag_active_site, flag_active_publisher, count_buf_start, 
	count_buf_end, count_open_full_screen, count_close_full_screen, count_replay, count_seek, count_open_upload, 
	count_save_publish, count_close_editor, count_pre_bumper_played, count_post_bumper_played, 
	count_bumper_clicked, count_preroll_started, count_midroll_started, count_postroll_started,
	count_overlay_started, count_preroll_clicked, count_midroll_clicked, count_postroll_clicked, 
	count_overlay_clicked, count_preroll_25, count_preroll_50, count_preroll_75, count_midroll_25, 
	count_midroll_50, count_midroll_75, count_postroll_25, count_postroll_50, count_postroll_75
FROM 	kalturadw.dwh_hourly_partner;

DROP TABLE kalturadw.dwh_hourly_partner; 
RENAME TABLE kalturadw.dwh_hourly_partner_new to kalturadw.dwh_hourly_partner; 

USE `kalturadw`;

DROP TABLE IF EXISTS kalturadw.`dwh_hourly_events_devices`;

CREATE TABLE kalturadw.`dwh_hourly_events_devices` (
  `partner_id` INT NOT NULL DEFAULT -1,
  `date_id` INT NOT NULL,
  `hour_id` INT NOT NULL,
  `location_id` INT NOT NULL DEFAULT -1,
  `country_id` INT NOT NULL DEFAULT -1,
  `os_id` INT NOT NULL DEFAULT -1,
  `browser_id` INT NOT NULL DEFAULT -1,
  `ui_conf_id` INT NOT NULL DEFAULT -1,
  `entry_id` VARCHAR(20) NOT NULL DEFAULT -1,
  `sum_time_viewed` DECIMAL(20,3) DEFAULT NULL,
  `count_time_viewed` INT DEFAULT NULL,
  `count_plays` INT DEFAULT NULL,
  `count_loads` INT DEFAULT NULL,
  `count_plays_25` INT DEFAULT NULL,
  `count_plays_50` INT DEFAULT NULL,
  `count_plays_75` INT DEFAULT NULL,
  `count_plays_100` INT DEFAULT NULL,
  `count_edit` INT DEFAULT NULL,
  `count_viral` INT DEFAULT NULL,
  `count_download` INT DEFAULT NULL,
  `count_report` INT DEFAULT NULL,
  `count_buf_start` INT DEFAULT NULL,
  `count_buf_end` INT DEFAULT NULL,
  `count_open_full_screen` INT DEFAULT NULL,
  `count_close_full_screen` INT DEFAULT NULL,
  `count_replay` INT DEFAULT NULL,
  `count_seek` INT DEFAULT NULL,
  `count_open_upload` INT DEFAULT NULL,
  `count_save_publish` INT DEFAULT NULL,
  `count_close_editor` INT DEFAULT NULL,    
  `count_pre_bumper_played` INT DEFAULT NULL,
  `count_post_bumper_played` INT DEFAULT NULL,
  `count_bumper_clicked` INT DEFAULT NULL,
  `count_preroll_started` INT DEFAULT NULL,
  `count_midroll_started` INT DEFAULT NULL,
  `count_postroll_started` INT DEFAULT NULL,
  `count_overlay_started` INT DEFAULT NULL,
  `count_preroll_clicked` INT DEFAULT NULL,
  `count_midroll_clicked` INT DEFAULT NULL,
  `count_postroll_clicked` INT DEFAULT NULL,
  `count_overlay_clicked` INT DEFAULT NULL,
  `count_preroll_25` INT DEFAULT NULL,
  `count_preroll_50` INT DEFAULT NULL,
  `count_preroll_75` INT DEFAULT NULL,
  `count_midroll_25` INT DEFAULT NULL,
  `count_midroll_50` INT DEFAULT NULL,
  `count_midroll_75` INT DEFAULT NULL,
  `count_postroll_25` INT DEFAULT NULL,
  `count_postroll_50` INT DEFAULT NULL,
  `count_postroll_75` INT DEFAULT NULL,
  `count_bandwidth_kb` INT DEFAULT NULL,
  `total_admins` INT DEFAULT NULL,
  `total_media_entries` INT DEFAULT NULL,
  PRIMARY KEY `partner_id` (`partner_id`,`date_id`,`hour_id`,`location_id`,`country_id`,`os_id`,`browser_id`,`ui_conf_id`,`entry_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8
PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = INNODB,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = INNODB,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = INNODB,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = INNODB,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = INNODB,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = INNODB,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = INNODB,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = INNODB,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = INNODB,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = INNODB,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = INNODB);

CALL kalturadw.add_monthly_partition_for_table('dwh_hourly_events_devices');

