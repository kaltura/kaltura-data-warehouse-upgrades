/*6087*/
UPDATE kalturadw_ds.files
SET process_id = 3 
WHERE file_name like 'akamai_%'
AND process_id = 1;

INSERT INTO kalturadw_ds.processes(id, process_name, max_files_per_cycle) 
VALUES (3, 'akamai_events',20);
INSERT INTO kalturadw_ds.staging_areas (id, process_id, source_table, target_table, on_duplicate_clause, staging_partition_field, post_transfer_sp, aggr_date_field, hour_id_field, post_transfer_aggregations)
VALUES (3, 3, 'ds_events', 'kalturadw.dwh_fact_events', NULL, 'cycle_id', NULL, 'event_date_id', 'event_hour_id', '(\'country\',\'domain\',\'entry\',\'partner\',\'uid\',\'widget\',\'domain_referrer\',\'devices\')');

/*6088*/
DROP VIEW IF EXISTS `kalturadw`.`dwh_view_monthly_active_partners`;
CREATE VIEW `kalturadw`.`dwh_view_monthly_active_partners` 
    AS
(
	SELECT FLOOR(date_id/100) month_id, partner_id, 
		SUM(IFNULL(new_videos,0)) + SUM(IFNULL(new_images,0)) + SUM(IFNULL(new_audios,0)) + 
		SUM(IFNULL(new_playlists,0)) + SUM(IFNULL(new_livestreams,0)) + SUM(IFNULL(new_other_entries,0)) AS new_entries,
		SUM(count_plays) count_plays
	FROM kalturadw.dwh_hourly_partner
	GROUP BY month_id, partner_id
	HAVING new_entries > 10 AND count_plays > 100
);

/*6089*/
DROP PROCEDURE IF EXISTS kalturadw.post_aggregation_devices;

DROP PROCEDURE IF EXISTS kalturadw.calc_aggr_day_partner_bandwidth;

ALTER TABLE kalturadw.dwh_fact_fms_sessions 
	CHANGE session_client_location_id location_id int(11), 
	CHANGE session_client_country_id country_id int(11);

ALTER TABLE kalturadw_ds.fms_incomplete_sessions
        CHANGE session_client_location_id location_id int(11), 
        CHANGE session_client_country_id country_id int(11);

ALTER TABLE kalturadw_ds.fms_stale_sessions
        CHANGE session_client_location_id location_id int(11), 
        CHANGE session_client_country_id country_id int(11);

DELIMITER $$

USE `kalturadw_ds`$$

DROP PROCEDURE IF EXISTS `fms_sessionize`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `fms_sessionize`(
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
    country_id 			INT(10),
    location_id 		INT(10),
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
 
  INSERT INTO ds_temp_fms_sessions (session_id,session_time,session_date_id, session_client_ip, session_client_ip_number, country_id, location_id, session_partner_id, bandwidth_source_id, total_bytes)
  SELECT agg_session_id,agg_session_time,agg_session_date_id,agg_client_ip, agg_client_ip_number, agg_client_country_id, agg_client_location_id, agg_partner_id,agg_bandwidth_source_id,
  GREATEST(agg_dis_sc_bytes - agg_con_sc_bytes + agg_dis_cs_bytes - agg_con_cs_bytes, 0)
  FROM ds_temp_fms_session_aggr
  WHERE agg_partner_id IS NOT NULL AND agg_partner_id NOT IN (100  , -1  , -2  , 0 , 99 )  AND agg_is_connected_ind = 1 AND agg_is_disconnected_ind = 1;
  
  
  INSERT INTO fms_incomplete_sessions (session_id,session_time,updated_time,session_date_id, session_client_ip, session_client_ip_number, country_id, location_id, con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,partner_id, is_connected_ind, is_disconnected_ind)
  SELECT agg_session_id,agg_session_time,NOW() AS agg_update_time,agg_session_date_id,agg_client_ip, agg_client_ip_number, agg_client_country_id, agg_client_location_id,
         agg_con_cs_bytes,agg_con_sc_bytes,agg_dis_cs_bytes,agg_dis_sc_bytes,agg_partner_id, agg_is_connected_ind, agg_is_disconnected_ind
  FROM ds_temp_fms_session_aggr
  WHERE agg_partner_id IS NULL OR agg_is_connected_ind = 0 AND agg_is_disconnected_ind = 0
  ON DUPLICATE KEY UPDATE
    session_time=GREATEST(session_time,VALUES(session_time)),
    session_date_id=GREATEST(session_date_id,VALUES(session_date_id)),
    session_client_ip=VALUES(session_client_ip),
    session_client_ip_number=VALUES(session_client_ip_number), 
    location_id=VALUES(location_id),
    country_id=VALUES(country_id),
    con_cs_bytes=con_cs_bytes+VALUES(con_cs_bytes),
    con_sc_bytes=con_sc_bytes+VALUES(con_sc_bytes),
    dis_cs_bytes=dis_cs_bytes+VALUES(dis_cs_bytes),
    dis_sc_bytes=dis_sc_bytes+VALUES(dis_sc_bytes),
    partner_id=IF(partner_id IS NULL,VALUES(partner_id),partner_id),
    bandwidth_source_id=VALUES(bandwidth_source_id),
    updated_time=GREATEST(updated_time,VALUES(updated_time)),
    is_connected_ind = GREATEST(is_connected_ind, VALUES(is_connected_ind)),
    is_disconnected_ind = GREATEST(is_disconnected_ind, VALUES(is_disconnected_ind));
  
  INSERT INTO ds_temp_fms_sessions (session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, country_id, location_id,session_partner_id,bandwidth_source_id,total_bytes)
  SELECT session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, country_id, location_id,partner_id,bandwidth_source_id,
  GREATEST(dis_sc_bytes - con_sc_bytes + dis_cs_bytes -con_cs_bytes, 0)
  FROM fms_incomplete_sessions
  WHERE partner_id IS NOT NULL AND partner_id NOT IN (100  , -1  , -2  , 0 , 99 ) AND is_connected_ind = 1 AND is_disconnected_ind = 1;
    
  INSERT INTO fms_stale_sessions (partner_id, bandwidth_source_id, session_id, session_time,session_date_id,session_client_ip, session_client_ip_number, country_id, location_id,con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,last_update_time,purge_time)
  SELECT partner_id,bandwidth_source_id, session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, country_id, location_id,con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,updated_time,NOW()
  FROM fms_incomplete_sessions
  WHERE GREATEST(session_time,updated_time) < FMS_STALE_SESSION_PURGE AND (partner_id IS NULL OR is_connected_ind = 0 AND is_disconnected_ind = 0);
  
  DELETE FROM fms_incomplete_sessions
  WHERE (partner_id IS NOT NULL AND is_connected_ind = 1 AND is_disconnected_ind = 1) OR
        GREATEST(session_time,updated_time) < FMS_STALE_SESSION_PURGE;
  
  INSERT INTO kalturadw.dwh_fact_fms_sessions (session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, country_id, location_id,session_partner_id,bandwidth_source_id,total_bytes)
  SELECT session_id,session_time,session_date_id,session_client_ip, session_client_ip_number, country_id, location_id,session_partner_id,bandwidth_source_id,total_bytes
  FROM ds_temp_fms_sessions
  ON DUPLICATE KEY UPDATE
	total_bytes=VALUES(total_bytes),
	session_partner_id=VALUES(session_partner_id),
	session_time=VALUES(session_time),
	session_client_ip=VALUES(session_client_ip),
	session_client_ip_number=VALUES(session_client_ip_number),
	country_id=VALUES(country_id),
	location_id=VALUES(location_id),
	bandwidth_source_id=VALUES(bandwidth_source_id);
END$$

DELIMITER ;

ALTER TABLE kalturadw_ds.aggr_name_resolver ADD COLUMN aggr_type VARCHAR(60) NOT NULL;
UPDATE kalturadw_ds.aggr_name_resolver SET aggr_type = 'events';

INSERT INTO kalturadw_ds.aggr_name_resolver (aggr_name, aggr_table, aggr_id_field, aggr_type)
VALUES('bandwidth_usage', 'dwh_hourly_partner_usage', 'bandwidth_source_id', 'bandwidth'), ('devices_bandwidth_usage', 'dwh_hourly_events_devices', 'country_id, location_id','bandwidth');

INSERT INTO kalturadw.aggr_managment (aggr_name, aggr_day, hour_id, aggr_day_int, is_calculated)
SELECT 'devices_bandwidth_usage', aggr_day, hour_id, aggr_day_int, is_calculated
FROM kalturadw.aggr_managment
WHERE aggr_name = 'bandwidth_usage';

UPDATE kalturadw_ds.staging_areas
SET post_transfer_aggregations = IF(id IN (1,3) AND source_table = 'ds_events', '(\'country\',\'domain\',\'entry\',\'partner\',\'uid\',\'widget\',\'domain_referrer\',\'devices\')','(\'bandwidth_usage\',\'devices_bandwidth_usage\')');

ALTER TABLE kalturadw.dwh_fact_fms_sessions_archive
	CHANGE session_client_location_id location_id int(11), 
	CHANGE session_client_country_id country_id int(11);

/*6090*/
ALTER TABLE kalturadw.dwh_hourly_events_devices ADD KEY (date_id,hour_id);
ALTER TABLE kalturadw.dwh_hourly_events_widget ADD KEY (date_id,hour_id);
ALTER TABLE kalturadw.dwh_hourly_events_uid ADD KEY (date_id,hour_id);

USE kalturadw;

DROP TABLE IF EXISTS `dwh_hourly_events_country_new`;
CREATE TABLE `dwh_hourly_events_country_new` (
  `partner_id` int(11) NOT NULL DEFAULT '0',
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` int(11) NOT NULL DEFAULT '0',
  `country_id` int(11) NOT NULL DEFAULT '0',
  `location_id` int(11) NOT NULL DEFAULT '0',
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
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`country_id`,`location_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB)*/;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_events_country');

INSERT INTO dwh_hourly_events_country_new
SELECT * FROM dwh_hourly_events_country;

RENAME TABLE dwh_hourly_events_country TO dwh_hourly_events_country_old;
RENAME TABLE dwh_hourly_events_country_new TO dwh_hourly_events_country;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_hourly_events_devices_new`;
CREATE TABLE `dwh_hourly_events_devices_new` (
`partner_id` int(11) NOT NULL DEFAULT '-1',
  `date_id` int(11) NOT NULL,
  `hour_id` int(11) NOT NULL,
  `location_id` int(11) NOT NULL DEFAULT '-1',
  `country_id` int(11) NOT NULL DEFAULT '-1',
  `os_id` int(11) NOT NULL DEFAULT '-1',
  `browser_id` int(11) NOT NULL DEFAULT '-1',
  `ui_conf_id` int(11) NOT NULL DEFAULT '-1',
  `entry_id` varchar(20) NOT NULL DEFAULT '-1',
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
  `count_bandwidth_kb` int(11) DEFAULT NULL,
  `total_admins` int(11) DEFAULT NULL,
  `total_media_entries` int(11) DEFAULT NULL,
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`location_id`,`country_id`,`os_id`,`browser_id`,`ui_conf_id`,`entry_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB)*/;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_events_devices');

INSERT INTO dwh_hourly_events_devices_new
SELECT * FROM dwh_hourly_events_devices;

RENAME TABLE dwh_hourly_events_devices TO dwh_hourly_events_devices_old;
RENAME TABLE dwh_hourly_events_devices_new TO dwh_hourly_events_devices;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_hourly_events_domain_new`;
CREATE TABLE `dwh_hourly_events_domain_new` (
`partner_id` int(11) NOT NULL DEFAULT '0',
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` int(11) NOT NULL DEFAULT '0',
  `domain_id` int(11) NOT NULL DEFAULT '0',
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
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`domain_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB)*/;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_events_domain');

INSERT INTO dwh_hourly_events_domain_new
SELECT * FROM dwh_hourly_events_domain;

RENAME TABLE dwh_hourly_events_domain TO dwh_hourly_events_domain_old;
RENAME TABLE dwh_hourly_events_domain_new TO dwh_hourly_events_domain;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_hourly_events_domain_referrer_new`;
CREATE TABLE `dwh_hourly_events_domain_referrer_new` (
  `partner_id` int(11) NOT NULL DEFAULT '0',
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` int(11) NOT NULL DEFAULT '0',
  `domain_id` int(11) NOT NULL DEFAULT '0',
  `referrer_id` int(11) NOT NULL DEFAULT '0',
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
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`domain_id`,`referrer_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB)*/;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_events_domain_referrer');

INSERT INTO dwh_hourly_events_domain_referrer_new
SELECT * FROM dwh_hourly_events_domain_referrer;

RENAME TABLE dwh_hourly_events_domain_referrer TO dwh_hourly_events_domain_referrer_old;
RENAME TABLE dwh_hourly_events_domain_referrer_new TO dwh_hourly_events_domain_referrer;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_hourly_events_entry_new`;
CREATE TABLE `dwh_hourly_events_entry_new` (
  `partner_id` int(11) NOT NULL DEFAULT '0',
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` int(11) NOT NULL DEFAULT '0',
  `entry_id` varchar(20) NOT NULL DEFAULT '',
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
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`entry_id`),
  KEY (`date_id`, `hour_id`),
  KEY `entry_id` (`entry_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB)*/;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_events_entry');

INSERT INTO dwh_hourly_events_entry_new
SELECT * FROM dwh_hourly_events_entry;

RENAME TABLE dwh_hourly_events_entry TO dwh_hourly_events_entry_old;
RENAME TABLE dwh_hourly_events_entry_new TO dwh_hourly_events_entry;

USE kalturadw;

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
  `new_admins` int(11) DEFAULT NULL,
  `new_videos` int(11) DEFAULT NULL,
  `deleted_videos` int(11) DEFAULT NULL,
  `new_images` int(11) DEFAULT NULL,
  `deleted_images` int(11) DEFAULT NULL,
  `new_audios` int(11) DEFAULT NULL,
  `deleted_audios` int(11) DEFAULT NULL,
  `new_livestreams` int(11) DEFAULT NULL,
  `deleted_livestreams` int(11) DEFAULT NULL,
  `new_playlists` int(11) DEFAULT NULL,
  `deleted_playlists` int(11) DEFAULT NULL,
  `new_documents` int(11) DEFAULT NULL,
  `deleted_documents` int(11) DEFAULT NULL,
  `new_other_entries` int(11) DEFAULT NULL,
  `deleted_other_entries` int(11) DEFAULT NULL,
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
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB)*/;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_partner');

INSERT INTO dwh_hourly_partner_new
SELECT * FROM dwh_hourly_partner;


DROP TABLE IF EXISTS `dwh_hourly_partner_old`;

RENAME TABLE dwh_hourly_partner TO dwh_hourly_partner_old;
RENAME TABLE dwh_hourly_partner_new TO dwh_hourly_partner;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_hourly_partner_usage_new`;
CREATE TABLE `dwh_hourly_partner_usage_new` (
  `partner_id` int(11) NOT NULL,
  `date_id` int(11) NOT NULL,
  `hour_id` int(11) NOT NULL,
  `bandwidth_source_id` int(11) NOT NULL,
  `count_bandwidth_kb` decimal(19,4) DEFAULT '0.0000',
  `count_storage_mb` decimal(19,4) DEFAULT '0.0000',
  `aggr_storage_mb` decimal(19,4) DEFAULT NULL,
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`bandwidth_source_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB)*/;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_partner_usage');

INSERT INTO dwh_hourly_partner_usage_new
SELECT * FROM dwh_hourly_partner_usage;

RENAME TABLE dwh_hourly_partner_usage TO dwh_hourly_partner_usage_old;
RENAME TABLE dwh_hourly_partner_usage_new TO dwh_hourly_partner_usage;

DROP TABLE IF EXISTS kalturadw.dwh_hourly_events_domain_referrer_old;
DROP TABLE IF EXISTS kalturadw.dwh_hourly_events_domain_old;
DROP TABLE IF EXISTS kalturadw.dwh_hourly_events_country_old;
DROP TABLE IF EXISTS kalturadw.dwh_hourly_events_devices_old;
DROP TABLE IF EXISTS kalturadw.dwh_hourly_events_entry_old;
DROP TABLE IF EXISTS kalturadw.dwh_hourly_partner_old;
DROP TABLE IF EXISTS kalturadw.dwh_hourly_partner_usage_old;

/*6091*/
ALTER TABLE kalturadw_ds.retention_policy ADD PRIMARY KEY (table_name);

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `move_innodb_to_archive`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `move_innodb_to_archive`()
BEGIN
	DECLARE v_table_name VARCHAR(256);
	DECLARE v_archive_name VARCHAR(256);	
	DECLARE v_partition_name VARCHAR(256);
	DECLARE v_partition_date_id INT;
	DECLARE v_column VARCHAR(256);
	DECLARE v_is_archived INT;
	DECLARE v_is_in_fact INT;
	
	DECLARE v_drop_from_archive INT DEFAULT 0;
	DECLARE v_drop_from_fact INT DEFAULT 0;
	DECLARE v_migrate_from_fact INT DEFAULT 0;
		
	DECLARE v_done INT DEFAULT 0;
	
	DECLARE c_partitions 
	CURSOR FOR 	
	SELECT 	r.table_name, 
		CONCAT(r.table_name, '_archive') archive_name,
		partition_name, 
		DATE(partition_description)*1 partition_date_id, 
		partition_expression column_name,
		MAX(IF(CONCAT(r.table_name, '_archive') = p.table_name,1,0)) is_archived, 
		MAX(IF(r.table_name=p.table_name,1,0)) is_in_fact
	FROM information_schema.PARTITIONS p, kalturadw_ds.retention_policy r
	WHERE LENGTH(partition_description) = 8 
	AND DATE(partition_description)*1 IS NOT NULL
	AND (p.table_name = r.table_name OR CONCAT(r.table_name, '_archive') = p.table_name)
	GROUP BY r.table_name, partition_name, partition_date_id, column_name
	ORDER BY partition_date_id;
	
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;
	
	OPEN c_partitions;
	
	read_loop: LOOP
		FETCH c_partitions INTO v_table_name, v_archive_name, v_partition_name, v_partition_date_id, v_column, v_is_archived, v_is_in_fact;

		IF v_done = 1 THEN
		  LEAVE read_loop;
		END IF;

		SET v_drop_from_archive = 0;
		SET v_drop_from_fact = 0;
		SET v_migrate_from_fact = 0;

		-- Check if a partition exists in the archive or the fact and is older than the delete policy
		SELECT if(count(*)=0,0,v_is_archived), if(count(*)=0, 0,v_is_in_fact)
		INTO v_drop_from_archive, v_drop_from_fact
		FROM kalturadw_ds.retention_policy
		WHERE DATE(NOW() - INTERVAL archive_delete_days_back DAY)*1 >= v_partition_date_id
		AND table_name = v_table_name;

		IF (v_drop_from_archive > 0) THEN 
			SET @s = CONCAT('ALTER TABLE ',v_archive_name,' DROP PARTITION ', v_partition_name);
			
			PREPARE stmt FROM @s;
                        EXECUTE stmt;
                        DEALLOCATE PREPARE stmt;
		END IF;

		-- Check if a partition exists in the fact and is older than the archive policy
		SELECT if(count(*)=0,0, v_is_in_fact)
		INTO v_migrate_from_fact
		FROM kalturadw_ds.retention_policy
		WHERE DATE(NOW() - INTERVAL archive_start_days_back DAY)*1 >= v_partition_date_id
		AND table_name = v_table_name
		AND v_is_in_fact > 0;
		
		-- If the partition exists in the fact and is older than the archive policy (and not older than the delete policy)
		IF (v_migrate_from_fact > 0 AND v_drop_from_fact = 0) THEN
			-- Check if the archive already has a partition convering this time period (drop it)
			IF (v_is_archived > 0) THEN
				SET @s = CONCAT('ALTER TABLE ',v_archive_name,' DROP PARTITION ', v_partition_name);
		
				PREPARE stmt FROM @s;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
			END IF;
			
			-- Migrate it to the archive
			SET @s = CONCAT('ALTER TABLE ',v_archive_name,' ADD PARTITION (PARTITION ',v_partition_name,' VALUES LESS THAN (',v_partition_date_id,'))');
			
			PREPARE stmt FROM @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			SET @s = CONCAT('INSERT INTO ',v_archive_name,' SELECT * FROM ',v_table_name,' WHERE ', v_column ,' < ',v_partition_date_id);
			
			PREPARE stmt FROM @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
			
			UPDATE kalturadw_ds.retention_policy
			SET archive_last_partition = DATE(v_partition_date_id)
			WHERE table_name = v_table_name;

			SET v_drop_from_fact = 1;
		END IF;

		-- If partition has migrated from the fact or should be dropped due to the fact that it's older than the delete policy
		IF (v_drop_from_fact > 0) THEN
			SET @s = CONCAT('ALTER TABLE ',v_table_name,' DROP PARTITION ',v_partition_name);
			
			PREPARE stmt FROM @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		END IF;
	END LOOP read_loop;

	CLOSE c_partitions;
END$$

DELIMITER ;

UPDATE kalturadw_ds.retention_policy
SET archive_delete_days_back = 2000;

/*6092*/
/*6093*/
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `add_partitions`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `add_partitions`()
BEGIN
	CALL add_daily_partition_for_table('dwh_fact_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_session_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_sessions');
	CALL add_daily_partition_for_table('dwh_fact_bandwidth_usage');
	CALL add_daily_partition_for_table('dwh_fact_api_calls');
	CALL add_daily_partition_for_table('dwh_fact_incomplete_api_calls');
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
	CALL add_monthly_partition_for_table('dwh_hourly_api_calls');
END$$

DELIMITER ;

INSERT INTO kalturadw.aggr_managment (aggr_name, aggr_day, hour_id, aggr_day_int, is_calculated)
SELECT DISTINCT 'api_calls', aggr_day, hour_id, aggr_day_int, IF(aggr_day_int>=date(now())*1,0,1)
FROM kalturadw.aggr_managment;

USE kalturadw_ds;

DROP TABLE IF EXISTS `ds_api_calls`;
CREATE TABLE `ds_api_calls` (
  `cycle_id` INT(11) NOT NULL,
  `file_id` INT(11) NOT NULL,
  `line_number` INT(11) NOT NULL,
  `api_call_time` DATETIME DEFAULT NULL,
  `api_call_date_id` INT(11) NOT NULL DEFAULT '0',
  `api_call_hour_id` TINYINT(4) DEFAULT NULL,
  `session_id` VARCHAR(50) NOT NULL,
  `request_index` INT(11) DEFAULT NULL,
  `partner_id` INT(11) DEFAULT NULL,
  `action_id` INT(11) NOT NULL DEFAULT '0', 
  `os_id` INT(11) DEFAULT NULL,
  `browser_id` INT(11) DEFAULT NULL,
  `client_tag_id` INT(11) DEFAULT NULL,
  `is_admin` BOOL DEFAULT NULL,
  `pid` INT(11) DEFAULT NULL,
  `host_id` INT(11) DEFAULT NULL,
  `user_ip` VARCHAR(15) DEFAULT NULL,
  `user_ip_number` INT(10) UNSIGNED DEFAULT NULL,
  `country_id` INT(11) DEFAULT NULL,
  `location_id` INT(11) DEFAULT NULL,
  `master_partner_id` INT(11) DEFAULT NULL,
  `ks` VARCHAR(100) DEFAULT NULL,
  `kuser_id` INT(11) DEFAULT NULL,
  `is_in_multi_request` BOOL DEFAULT NULL,  
  `success` BOOL DEFAULT NULL,  
  `error_code_id` INT(11) DEFAULT NULL,
  `duration_msecs` INT(11) DEFAULT NULL
) ENGINE=INNODB DEFAULT CHARSET=utf8
PARTITION BY LIST(cycle_id) 
(PARTITION p_0 VALUES IN (0));

USE kalturadw_ds;

DROP TABLE IF EXISTS `ds_incomplete_api_calls`;
CREATE TABLE `ds_incomplete_api_calls` (
  `cycle_id` INT(11) NOT NULL,
  `file_id` INT(11) NOT NULL,
  `line_number` INT(11) NOT NULL,
  `api_call_time` DATETIME DEFAULT NULL,
  `api_call_date_id` INT(11) DEFAULT NULL,
  `api_call_hour_id` TINYINT(4) DEFAULT NULL,
  `session_id` VARCHAR(50) NOT NULL,
  `request_index` INT(11) DEFAULT NULL,
  `partner_id` INT(11) DEFAULT NULL,
  `action_id` INT(11) NOT NULL DEFAULT '0', 
  `os_id` INT(11) DEFAULT NULL,
  `browser_id` INT(11) DEFAULT NULL,
  `client_tag_id` INT(11) DEFAULT NULL,
  `is_admin` BOOL DEFAULT NULL,
  `pid` INT(11) DEFAULT NULL,
  `host_id` INT(11) DEFAULT NULL,
  `user_ip` VARCHAR(15) DEFAULT NULL,
  `user_ip_number` INT(10) UNSIGNED DEFAULT NULL,
  `country_id` INT(11) DEFAULT NULL,
  `location_id` INT(11) DEFAULT NULL,
  `master_partner_id` INT(11) DEFAULT NULL,
  `ks` VARCHAR(100) DEFAULT NULL,
  `kuser_id` INT(11) DEFAULT NULL,
  `is_in_multi_request` BOOL DEFAULT NULL,  
  `success` BOOL DEFAULT NULL,  
  `error_code_id` INT(11) DEFAULT NULL,
  `duration_msecs` INT(11) DEFAULT NULL
) ENGINE=INNODB DEFAULT CHARSET=utf8
PARTITION BY LIST(cycle_id) 
(PARTITION p_0 VALUES IN (0));

DROP TABLE IF EXISTS `kalturadw`.dwh_dim_api_actions;

CREATE TABLE `kalturadw`.`dwh_dim_api_actions` (
  `action_id` INT(11) NOT NULL AUTO_INCREMENT,
  `action_name` VARCHAR(166) NOT NULL DEFAULT '',
  `service_name` VARCHAR(166) NOT NULL DEFAULT '',
  `dwh_creation_date` TIMESTAMP  NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (`action_id`),
   UNIQUE KEY (`action_name`, `service_name`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

CREATE TRIGGER `kalturadw`.`dwh_dim_api_actions_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_api_actions`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();

DROP TABLE IF EXISTS `kalturadw`.dwh_dim_api_error_codes;

CREATE TABLE `kalturadw`.`dwh_dim_api_error_codes` (
  `api_error_code_id` INT(11) NOT NULL AUTO_INCREMENT,
  `api_error_code_name` VARCHAR(333) NOT NULL DEFAULT '',
  `dwh_creation_date` TIMESTAMP  NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (`api_error_code_id`),
   UNIQUE KEY (`api_error_code_name`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

CREATE TRIGGER `kalturadw`.`dwh_dim_api_error_code_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_api_error_codes`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();

DROP TABLE IF EXISTS `kalturadw`.dwh_dim_client_tags;

CREATE TABLE `kalturadw`.`dwh_dim_client_tags` (
  `client_tag_id` INT(11) NOT NULL AUTO_INCREMENT,
  `client_tag_name` VARCHAR(333) NOT NULL DEFAULT '',
  `dwh_creation_date` TIMESTAMP  NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (`client_tag_id`),
   UNIQUE KEY (`client_tag_name`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

CREATE TRIGGER `kalturadw`.`dwh_dim_client_tags_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_client_tags`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();

DROP TABLE IF EXISTS `kalturadw`.dwh_dim_hosts;

CREATE TABLE `kalturadw`.`dwh_dim_hosts` (
  `host_id` INT(11) NOT NULL AUTO_INCREMENT,
  `host_name` VARCHAR(333) NOT NULL DEFAULT '',
  `dwh_creation_date` TIMESTAMP  NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
   PRIMARY KEY (`host_id`),
   UNIQUE KEY (`host_name`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

CREATE TRIGGER `kalturadw`.`dwh_dim_hosts_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_hosts`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();

USE kalturadw;

DROP TABLE IF EXISTS `dwh_fact_api_calls`;
CREATE TABLE `dwh_fact_api_calls` (
  `file_id` INT(11) NOT NULL,
  `line_number` INT(11) NOT NULL,
  `api_call_time` DATETIME DEFAULT NULL,
  `api_call_date_id` INT(11) NOT NULL DEFAULT '0',
  `api_call_hour_id` TINYINT(4) DEFAULT NULL,
  `session_id` VARCHAR(50) NOT NULL,
  `request_index` INT(11) DEFAULT NULL,
  `partner_id` INT(11) NOT NULL,
  `action_id` INT(11) NOT NULL DEFAULT '0', 
  `os_id` INT(11) DEFAULT NULL,
  `browser_id` INT(11) DEFAULT NULL,
  `client_tag_id` INT(11) DEFAULT NULL,
  `is_admin` BOOL DEFAULT NULL,
  `pid` INT(11) DEFAULT NULL,
  `host_id` INT(11) DEFAULT NULL,
  `user_ip` VARCHAR(15) DEFAULT NULL,
  `user_ip_number` INT(10) UNSIGNED DEFAULT NULL,
  `country_id` INT(11) DEFAULT NULL,
  `location_id` INT(11) DEFAULT NULL,
  `master_partner_id` INT(11) DEFAULT NULL,
  `ks` VARCHAR(100) DEFAULT NULL,
  `kuser_id` INT(11) DEFAULT NULL,
  `is_in_multi_request` BOOL DEFAULT NULL,  
  `success` BOOL DEFAULT NULL,  
  `error_code_id` INT(11) DEFAULT NULL,
  `duration_msecs` INT(11) DEFAULT NULL,
  PRIMARY KEY (`file_id`,`line_number`,`api_call_date_id`),
  KEY (`api_call_date_id`,`api_call_hour_id`,`partner_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (api_call_date_id)
(PARTITION p_20120801 VALUES LESS THAN (20120802) ENGINE = InnoDB) */;

CALL add_daily_partition_for_table('dwh_fact_api_calls');

USE kalturadw;

DROP TABLE IF EXISTS `dwh_fact_api_calls_archive`;
CREATE TABLE `dwh_fact_api_calls_archive` (
  `file_id` INT(11) NOT NULL,
  `line_number` INT(11) NOT NULL,
  `api_call_time` DATETIME DEFAULT NULL,
  `api_call_date_id` INT(11) NOT NULL DEFAULT '0',
  `api_call_hour_id` TINYINT(4) DEFAULT NULL,
  `session_id` VARCHAR(50) NOT NULL,
  `request_index` INT(11) DEFAULT NULL,
  `partner_id` INT(11) DEFAULT NULL,
  `action_id` INT(11) NOT NULL DEFAULT '0', 
  `os_id` INT(11) DEFAULT NULL,
  `browser_id` INT(11) DEFAULT NULL,
  `client_tag_id` INT(11) DEFAULT NULL,
  `is_admin` BOOL DEFAULT NULL,
  `pid` INT(11) DEFAULT NULL,
  `host_id` INT(11) DEFAULT NULL,
  `user_ip` VARCHAR(15) DEFAULT NULL,
  `user_ip_number` INT(10) UNSIGNED DEFAULT NULL,
  `country_id` INT(11) DEFAULT NULL,
  `location_id` INT(11) DEFAULT NULL,
  `master_partner_id` INT(11) DEFAULT NULL,
  `ks` VARCHAR(100) DEFAULT NULL,
  `kuser_id` INT(11) DEFAULT NULL,
  `is_in_multi_request` BOOL DEFAULT NULL,  
  `success` BOOL DEFAULT NULL,  
  `error_code_id` INT(11) DEFAULT NULL,
  `duration_msecs` INT(11) DEFAULT NULL
) ENGINE=ARCHIVE DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (api_call_date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = ARCHIVE) */;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_fact_incomplete_api_calls`;
CREATE TABLE `dwh_fact_incomplete_api_calls` (
  `cycle_id` INT(11) NOT NULL,
  `file_id` INT(11) NOT NULL,
  `line_number` INT(11) NOT NULL,
  `api_call_time` DATETIME DEFAULT NULL,
  `api_call_date_id` INT(11) NOT NULL DEFAULT '0',
  `api_call_hour_id` TINYINT(4) DEFAULT NULL,
  `session_id` VARCHAR(50) NOT NULL,
  `request_index` INT(11) NOT NULL,
  `partner_id` INT(11) DEFAULT NULL,
  `action_id` INT(11) NOT NULL DEFAULT '0', 
  `os_id` INT(11) DEFAULT NULL,
  `browser_id` INT(11) DEFAULT NULL,
  `client_tag_id` INT(11) DEFAULT NULL,
  `is_admin` BOOL DEFAULT NULL,
  `pid` INT(11) DEFAULT NULL,
  `host_id` INT(11) DEFAULT NULL,
  `user_ip` VARCHAR(15) DEFAULT NULL,
  `user_ip_number` INT(10) UNSIGNED DEFAULT NULL,
  `country_id` INT(11) DEFAULT NULL,
  `location_id` INT(11) DEFAULT NULL,
  `master_partner_id` INT(11) DEFAULT NULL,
  `ks` VARCHAR(100) DEFAULT NULL,
  `kuser_id` INT(11) DEFAULT NULL,
  `is_in_multi_request` BOOL DEFAULT NULL,  
  `success` BOOL DEFAULT NULL,  
  `error_code_id` INT(11) DEFAULT NULL,
  `duration_msecs` INT(11) DEFAULT NULL,
  PRIMARY KEY (`cycle_id`,`file_id`,`line_number`,`api_call_date_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (api_call_date_id)
(PARTITION p_20120801 VALUES LESS THAN (20120802) ENGINE = InnoDB) */;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_hourly_api_calls`;
CREATE TABLE `dwh_hourly_api_calls` (
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` tinyint(4) DEFAULT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `action_id` int(11) NOT NULL DEFAULT '0',
  `count_calls` decimal(22,0) DEFAULT NULL,
  `count_success` decimal(23,0) DEFAULT NULL,
  `count_is_in_multi_request` decimal(23,0) DEFAULT NULL,
  `count_is_admin` decimal(14,4) DEFAULT NULL,
  `sum_duration_msecs` decimal(32,0) DEFAULT NULL,
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`, `action_id`),
  KEY (`date_id`,`hour_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201208 VALUES LESS THAN (20120901) ENGINE = InnoDB) */;

CALL add_monthly_partition_for_table('dwh_hourly_api_calls');

INSERT INTO kalturadw_ds.processes(id, process_name, max_files_per_cycle) VALUES (8, 'api_calls',20);
INSERT INTO kalturadw_ds.staging_areas (id, process_id, source_table, target_table, on_duplicate_clause, staging_partition_field, post_transfer_sp, aggr_date_field, hour_id_field, post_transfer_aggregations)
VALUES  (9, 8, 'ds_api_calls', 'kalturadw.dwh_fact_api_calls', NULL, 'cycle_id', NULL, 'api_call_date_id', 'api_call_hour_id', '(\'api_calls\')'),
        (10, 8, 'ds_incomplete_api_calls', 'kalturadw.dwh_fact_incomplete_api_calls', NULL, 'cycle_id', 'unify_incomplete_api_calls', '', '', '');


INSERT INTO kalturadw_ds.aggr_name_resolver (aggr_name, aggr_table, aggr_id_field, aggr_type)
VALUES ('api_calls','dwh_hourly_api_calls','action_id','api');

INSERT INTO kalturadw_ds.retention_policy (table_name, archive_start_days_back, archive_delete_days_back, archive_last_partition)
VALUES ('dwh_fact_api_calls', 60, 2000, DATE(20120801)), ('dwh_fact_incomplete_api_calls', NULL, 3, NULL);


/*6094*/
USE kalturadw;

DROP TABLE IF EXISTS dwh_fact_errors;

CREATE TABLE dwh_fact_errors (
	file_id INT(11) NOT NULL,
	line_number INT(11) NOT NULL,
	partner_id INT(11) NOT NULL,
	error_time datetime NOT NULL,
	error_date_id int NOT NULL,
	error_hour_id int NOT NULL,
	error_object_id VARCHAR(50) NOT NULL,
	error_object_type_id INT(11) NOT NULL,
	error_code_id INT(11) NOT NULL,
	description mediumtext DEFAULT NULL,
	PRIMARY KEY (`file_id`, `line_number`, `error_date_id`),
	UNIQUE KEY (`error_date_id`,`error_object_id`,`error_object_type_id`,`error_time`)
	) ENGINE=INNODB DEFAULT CHARSET=latin1
	/*!50100 PARTITION BY RANGE (error_date_id)
	(PARTITION p_20120801 VALUES LESS THAN (20120802) ENGINE = INNODB)*/;

CALL add_daily_partition_for_table('dwh_fact_errors');

USE kalturadw;

DROP TABLE IF EXISTS dwh_fact_errors_archive;

CREATE TABLE dwh_fact_errors_archive (
	file_id INT(11) NOT NULL,
	line_number INT(11) NOT NULL,
	partner_id INT(11) NOT NULL,
	error_time datetime NOT NULL,
	error_date_id int NOT NULL,
	error_hour_id int NOT NULL,
	error_object_id VARCHAR(50) NOT NULL,
	error_object_type_id INT(11) NOT NULL,
	error_code_id INT(11) NOT NULL,
	description mediumtext DEFAULT NULL) ENGINE=ARCHIVE DEFAULT CHARSET = latin1
	/*!50100 PARTITION BY RANGE (error_date_id)
	(PARTITION p_0 VALUES LESS THAN (1))*/;

USE kalturadw_ds;

DROP TABLE IF EXISTS ds_errors;

CREATE TABLE ds_errors (
  	cycle_id INT(11) NOT NULL,
	file_id INT(11) NOT NULL,
	line_number INT(11) NOT NULL,
	partner_id INT(11) NOT NULL,
	error_time datetime NOT NULL,
	error_date_id int NOT NULL,
	error_hour_id int NOT NULL,
	error_object_id VARCHAR(50) NOT NULL,
	error_object_type_id INT(11) NOT NULL,
	error_code_id INT(11) NOT NULL,
	description mediumtext DEFAULT NULL
	) ENGINE=INNODB DEFAULT CHARSET=latin1
	PARTITION BY LIST(cycle_id)
	(PARTITION p_0 VALUES IN (0));

USE kalturadw;

DROP TABLE IF EXISTS dwh_hourly_errors;

CREATE TABLE dwh_hourly_errors (
	partner_id INT(11) NOT NULL,
	date_id int NOT NULL,
	hour_id int NOT NULL,
	error_code_id INT(11) NOT NULL,
	count_errors INT(11) NOT NULL,
	PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`error_code_id`),
	KEY (`date_id`, `hour_id`)
	) ENGINE=INNODB DEFAULT CHARSET=latin1
	/*!50100 PARTITION BY RANGE (date_id)
	(PARTITION p_201208 VALUES LESS THAN (20120901) ENGINE = INNODB)*/;

CALL add_monthly_partition_for_table('dwh_hourly_errors');

RENAME TABLE kalturadw.dwh_dim_api_error_codes TO kalturadw.dwh_dim_error_codes;

ALTER TABLE kalturadw.dwh_dim_error_codes
        CHANGE api_error_code_id error_code_id INT(11) AUTO_INCREMENT,
        CHANGE api_error_code_name error_code_name VARCHAR(165) NOT NULL,
        ADD sub_error_code_name VARCHAR(165) NOT NULL DEFAULT 'unknown' AFTER error_code_name,
        DROP KEY api_error_code_name,
        ADD UNIQUE KEY (error_code_name, sub_error_code_name);

DROP TRIGGER IF EXISTS `kalturadw`.`dwh_dim_api_error_code_setcreationtime_oninsert`;

CREATE TRIGGER `kalturadw`.`dwh_dim_error_code_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_error_codes`
    FOR EACH ROW
        SET new.dwh_creation_date = NOW();

USE kalturadw;

DROP TABLE IF EXISTS dwh_dim_error_object_types;

CREATE TABLE dwh_dim_error_object_types (
	error_object_type_id INT(11) NOT NULL AUTO_INCREMENT,
	error_object_type_name VARCHAR(255) NOT NULL,
	PRIMARY KEY (`error_object_type_id`),
	UNIQUE KEY (`error_object_type_name`)
	) ENGINE=INNODB DEFAULT CHARSET=latin1;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `add_partitions`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `add_partitions`()
BEGIN
	CALL add_daily_partition_for_table('dwh_fact_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_session_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_sessions');
	CALL add_daily_partition_for_table('dwh_fact_bandwidth_usage');
    CALL add_daily_partition_for_table('dwh_fact_api_calls');
    CALL add_daily_partition_for_table('dwh_fact_incomplete_api_calls');	
	CALL add_daily_partition_for_table('dwh_fact_errors');
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
    CALL add_monthly_partition_for_table('dwh_hourly_api_calls');
	CALL add_monthly_partition_for_table('dwh_hourly_errors');
END$$

DELIMITER ;

INSERT INTO kalturadw_ds.staging_areas (id, process_id, source_table, target_table, on_duplicate_clause, staging_partition_field, post_transfer_sp,  aggr_date_field, hour_id_field, post_transfer_aggregations)
VALUES (11, 8, 'ds_errors', 'kalturadw.dwh_fact_errors', NULL, 'cycle_id', NULL, 'error_date_id', 'error_hour_id', '(\'errors\')');

INSERT INTO kalturadw_ds.retention_policy (table_name, archive_start_days_back, archive_delete_days_back, archive_last_partition)
VALUES ('dwh_fact_errors', 365, 2000, DATE(20110101));

INSERT INTO kalturadw_ds.aggr_name_resolver (aggr_name, aggr_table, aggr_id_field, aggr_type)
VALUES ('errors','dwh_hourly_errors','error_code_id','errors');

INSERT INTO kalturadw.aggr_managment (aggr_name, aggr_day, hour_id, aggr_day_int, is_calculated)
SELECT DISTINCT 'errors', aggr_day, hour_id, aggr_day_int, IF(aggr_day_int>=date(now())*1,0,1)
FROM kalturadw.aggr_managment;

/*6095*/
USE kalturadw;

DROP TABLE IF EXISTS `dwh_dim_categories`;

CREATE TABLE `dwh_dim_categories` (
	`category_id` INT(11) NOT NULL AUTO_INCREMENT,
    `category_name` VARCHAR(333) NOT NULL,
	`dwh_creation_date` TIMESTAMP  NOT NULL DEFAULT '0000-00-00 00:00:00',
	`dwh_update_date` TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
	PRIMARY KEY (`category_id`), UNIQUE KEY (category_name)
) ENGINE=MYISAM DEFAULT CHARSET=utf8; 

CREATE TRIGGER `kalturadw`.`dwh_dim_categories_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_categories`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();
    
INSERT INTO dwh_dim_categories (category_id,category_name) VALUES (1,'-');

use kalturadw;

drop table if exists `dwh_dim_entry_categories`;

create table `dwh_dim_entry_categories` (
	`partner_id` int(11) NOT NULL,
    	`entry_id` varchar(60) NOT NULL,
        `category_id` int(11) NOT NULL,
	`updated_at` DATETIME,
	`ri_ind` TINYINT(4)  NOT NULL DEFAULT 0 ,
	UNIQUE (entry_id, partner_id, `category_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8; 

USE kalturadw;

DROP PROCEDURE IF EXISTS load_categories;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `load_categories`()
BEGIN
    DECLARE v_entry_id VARCHAR(60);
    DECLARE v_partner_id INT(11);
    DECLARE v_categories VARCHAR(256);
    DECLARE v_updated_at TIMESTAMP;
    DECLARE v_category_name VARCHAR(256);
    DECLARE v_category_id INT;
    DECLARE v_categories_done INT;
    DECLARE v_categories_idx INT;
    DECLARE v_category_exists INT;
    DECLARE done INT DEFAULT 0;
    DECLARE entries CURSOR FOR
    SELECT partner_id, entry_id, categories, updated_at
    FROM dwh_dim_entries
    WHERE categories IS NOT NULL
    ORDER BY partner_id, entry_id;
    
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;

    OPEN entries;

    read_loop: LOOP
        FETCH entries INTO v_partner_id, v_entry_id, v_categories, v_updated_at;
        IF done THEN
             LEAVE read_loop;
        END IF;

        SET v_categories_done = 0;
        SET v_categories_idx = 1;

        WHILE NOT v_categories_done DO
            SET v_category_name = TRIM(SUBSTRING(v_categories, v_categories_idx,
                    IF(LOCATE(',', v_categories, v_categories_idx) > 0,
                    LOCATE(',', v_categories, v_categories_idx) - v_categories_idx,
                    LENGTH(v_categories))));

            IF LENGTH(v_category_name) > 0 THEN
                SET v_categories_idx = v_categories_idx + LENGTH(v_category_name) + 1;
                -- add the category if it doesnt already exist
                SET v_category_id = NULL;
                
                SELECT COUNT(*) INTO v_category_exists FROM dwh_dim_categories WHERE category_name = v_category_name;
		IF NOT v_category_exists THEN
			INSERT INTO dwh_dim_categories (category_name) VALUES (v_category_name);
		END IF;
		SELECT category_id INTO v_category_id FROM dwh_dim_categories WHERE category_name = v_category_name;
		
                -- add the entry category
                INSERT IGNORE INTO dwh_dim_entry_categories (partner_id, entry_id, category_id, updated_at) VALUES (v_partner_id, v_entry_id, v_category_id, v_updated_at);
            ELSE
                SET v_categories_done = 1;
            END IF;
        END WHILE;
    END LOOP;
END$$

DELIMITER ;


CALL load_categories();

DROP PROCEDURE load_categories;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_partner_billing_storage_per_category`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_partner_billing_storage_per_category`(p_start_date_id INT, p_end_date_id INT ,p_partner_id INT)
BEGIN
   -- Fetch storage per entry of this partner (as seen on selected date)
    
    DROP TEMPORARY TABLE IF EXISTS temp_storage;
    CREATE TEMPORARY TABLE temp_storage(
        category_name       VARCHAR(255) ,
        date_id             INT(11) NOT NULL,
        count_storage_mb    DECIMAL(19,4) NOT NULL
    ) ENGINE = MEMORY;
      
    INSERT INTO     temp_storage (category_name, date_id, count_storage_mb)
    SELECT  IF(ec.updated_at IS NULL OR c.category_name IS NULL,'-', c.category_name) category_name,  
        entry_size_date_id, 
        SUM(entry_additional_size_kb)/1024 aggr_storage_mb
    FROM    kalturadw.dwh_fact_entries_sizes es
        LEFT OUTER JOIN kalturadw.dwh_dim_entry_categories ec ON (ec.entry_id = es.entry_id AND ec.partner_id = es.partner_id)     
        LEFT OUTER JOIN kalturadw.dwh_dim_categories c ON (ec.category_id = c.category_id)
        LEFT OUTER JOIN kalturadw.dwh_dim_entries e ON (ec.partner_id = e.partner_id AND ec.entry_id = e.entry_id AND ec.updated_at = e.updated_at)
    WHERE es.partner_id = p_partner_id
    AND es.entry_size_date_id <= p_end_date_id    
    GROUP BY  IF(ec.updated_at IS NULL OR c.category_name IS NULL,'-', c.category_name) ,  es.entry_size_date_id;
    
    DROP TEMPORARY TABLE IF EXISTS temp_storage_2;
    CREATE TEMPORARY TABLE temp_storage_2 AS SELECT * FROM temp_storage;
    
    -- add aggr_storage
    DROP TEMPORARY TABLE IF EXISTS temp_aggr_storage;
    CREATE TEMPORARY TABLE temp_aggr_storage(
        category_name       VARCHAR(255) ,
        date_id             INT(11) NOT NULL,
        aggr_storage_mb    DECIMAL(19,4) NOT NULL
    ) ENGINE = MEMORY;
    
    INSERT INTO     temp_aggr_storage
    SELECT         a.category_name, a.date_id, SUM(b.count_storage_mb)
    FROM         temp_storage a, temp_storage_2 b 
    WHERE         a.category_name=b.category_name AND p_end_date_id >=a.date_id AND a.date_id >= b.date_id AND b.count_storage_mb<>0
    GROUP BY     a.date_id, a.category_name;
        
    DROP TEMPORARY TABLE IF EXISTS temp_aggr_storage_inner;
    CREATE TEMPORARY TABLE temp_aggr_storage_inner AS SELECT * FROM temp_aggr_storage;
    
    -- fetch results per category
    
    SELECT
        category_name,
        SUM(continuous_aggr_storage/DAY(LAST_DAY(continuous_category_storage.date_id))) avg_continuous_aggr_storage_mb
    FROM
    (    
        SELECT  all_times.day_id date_id,
            category_name,
            (SELECT aggr_storage_mb FROM temp_aggr_storage_inner inner_a_p
             WHERE  inner_a_p.category_name = stor.category_name
                    AND inner_a_p.date_id<=all_times.day_id 
                    AND inner_a_p.aggr_storage_mb IS NOT NULL 
                    ORDER BY inner_a_p.date_id DESC LIMIT 1) continuous_aggr_storage
                    FROM temp_aggr_storage stor, dwh_dim_time all_times
                WHERE   all_times.day_id BETWEEN 20081230 AND p_end_date_id
        GROUP BY category_name, all_times.day_id
    ) continuous_category_storage
    WHERE date_id BETWEEN p_start_date_id AND p_end_date_id
    GROUP BY category_name;
    
END$$

DELIMITER ;

/*6096*/
ALTER TABLE kalturadw_ds.staging_areas
	ADD ignore_duplicates_on_transfer BOOLEAN NOT NULL DEFAULT 0;

UPDATE kalturadw_ds.staging_areas
	SET ignore_duplicates_on_transfer = 1
	WHERE process_id = 8;

DELIMITER $$

USE `kalturadw_ds`$$

DROP PROCEDURE IF EXISTS `transfer_cycle_partition`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `transfer_cycle_partition`(p_cycle_id VARCHAR(10))
BEGIN
	DECLARE src_table VARCHAR(45);
	DECLARE tgt_table VARCHAR(45);
	DECLARE dup_clause VARCHAR(4000);
	DECLARE partition_field VARCHAR(45);
	DECLARE select_fields VARCHAR(4000);
	DECLARE post_transfer_sp_val VARCHAR(4000);
	DECLARE v_ignore_duplicates_on_transfer BOOLEAN;	
	DECLARE aggr_date VARCHAR(400);
	DECLARE aggr_hour VARCHAR(400);
	DECLARE aggr_names VARCHAR(4000);
	
	
	DECLARE done INT DEFAULT 0;
	DECLARE staging_areas_cursor CURSOR FOR SELECT 	source_table, target_table, IFNULL(on_duplicate_clause,''),	staging_partition_field, post_transfer_sp, aggr_date_field, hour_id_field, post_transfer_aggregations, ignore_duplicates_on_transfer
											FROM staging_areas s, cycles c
											WHERE s.process_id=c.process_id AND c.cycle_id = p_cycle_id;
											
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	OPEN staging_areas_cursor;
	
	read_loop: LOOP
		FETCH staging_areas_cursor INTO src_table, tgt_table, dup_clause, partition_field, post_transfer_sp_val, aggr_date, aggr_hour, aggr_names, v_ignore_duplicates_on_transfer;
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
			
		SET @s = CONCAT('INSERT ', IF(v_ignore_duplicates_on_transfer=1, 'IGNORE', '') ,' INTO ',tgt_table, ' (',select_fields,') ',
						' SELECT ',select_fields,
						' FROM ',src_table,
						' WHERE ',partition_field,'  = ',p_cycle_id,
						' ',dup_clause );

		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
			
		IF LENGTH(POST_TRANSFER_SP_VAL)>0 THEN
				SET @s = CONCAT('CALL ',post_transfer_sp_val,'(',p_cycle_id,')');
				
				PREPARE stmt FROM  @s;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
		END IF;
	END LOOP;

	CLOSE staging_areas_cursor;
END$$

DELIMITER ;
/*6097*/
/*6098*/
/*6099*/
ALTER TABLE kalturadw.dwh_dim_time
	CHANGE date_field date_field DATE NOT NULL,
	ADD day_eng_name VARCHAR(50) AFTER date_field,	
	ADD datetime_field DATETIME NOT NULL AFTER date_field,
	ADD week_id INT(11) AFTER day_of_week,
	ADD week_eng_name VARCHAR(50) AFTER week_of_year,
	ADD month_eng_name VARCHAR(50) AFTER YEAR,	
	ADD month_id INT(11) AFTER YEAR, 
	ADD month_str VARCHAR(50) AFTER YEAR,
	ADD quarter_eng_name  VARCHAR(50) AFTER QUARTER,
	ADD quarter_id INT(11) AFTER QUARTER;

UPDATE kalturadw.dwh_dim_time
SET 	datetime_field = date_field,
	day_eng_name = DATE_FORMAT(date_field, '%b %e, %Y'),
	week_id = DATE_FORMAT(date_field, '%Y%U')*1,
	week_eng_name = DATE_FORMAT(date_field, 'Week %U, %Y'),
	month_id = DATE_FORMAT(date_field, '%Y%m')*1,
	month_str = DATE_FORMAT(date_Field, '%Y-%m'),
	month_eng_name = DATE_FORMAT(date_Field, '%b-%y'),
	quarter_id = YEAR(date_Field)*10+QUARTER(date_field),
	quarter_eng_name = CONCAT('Quarter ', QUARTER(date_field), ',', YEAR(date_Field));

	
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `populate_time_dim`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `populate_time_dim`(start_date datetime, end_date datetime)
    DETERMINISTIC
BEGIN    

    WHILE start_date <= end_date DO
	INSERT INTO kalturadw.dwh_dim_time 
	(day_id, date_field, datetime_field, day_eng_name, YEAR, month_str, month_id, month_eng_name, MONTH, day_of_year, day_of_month, 
	day_of_week, week_id, week_of_year, week_eng_name, day_of_week_desc, day_of_week_short_desc, month_desc, month_short_desc, 
	QUARTER, quarter_id, quarter_eng_name)
	SELECT 1*DATE(d), d, d, DATE_FORMAT(d, '%b %e, %Y'), YEAR(d), DATE_FORMAT(d, '%Y-%m'), DATE_FORMAT(d, '%Y%m')*1, DATE_FORMAT(d, '%b-%y'), MONTH(d), DAYOFYEAR(d),DAYOFMONTH(d),
	DAYOFWEEK(d), DATE_FORMAT(d, '%Y%U')*1, WEEK(d), DATE_FORMAT(d, 'Week %U, %Y'), DAYNAME(d),DATE_FORMAT(d,'%a'),MONTHNAME(d), DATE_FORMAT(d, '%b'), 
	QUARTER(d), YEAR(d)*10+QUARTER(d), CONCAT('Quarter ', QUARTER(d), ',', YEAR(d))
        FROM(SELECT start_date d) a;
        
        SET start_date = DATE_ADD(start_date, INTERVAL 1 DAY);
    END WHILE;
    
END$$

DELIMITER ;


/*6100*/
DELIMITER $$

USE `kalturadw_ds`$$

DROP PROCEDURE IF EXISTS `register_file`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `register_file`(p_file_name VARCHAR(750), p_process_id INT, p_file_size_kb INT(11), p_compression_suffix VARCHAR(10), p_subdir VARCHAR(1024))
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
			ORDER BY SUM(file_size_kb) LIMIT 1;
		
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
END$$

DELIMITER ;

/*6101*/
UPDATE kalturadw_ds.retention_policy
SET archive_start_days_back = 30
WHERE archive_start_days_back = 60;

/*6102*/
UPDATE kalturadw.dwh_dim_bandwidth_source
SET bandwidth_source_name = 'level3'
where bandwidth_source_id = 3;

/*6103*/
USE kalturadw;

DROP TABLE IF EXISTS dwh_dim_hours;

CREATE TABLE `dwh_dim_hours` (
  hour_id INT
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

INSERT INTO dwh_dim_hours 
VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9), (10), (11), (12), (13), (14), (15), (16), (17), (18), (19), (20), (21), (22), (23);


/*6104*/

DELIMITER $$

USE `kalturadw`$$

DROP VIEW IF EXISTS `dwh_view_partners_monthly_billing_last_updated_at`$$

CREATE ALGORITHM=UNDEFINED DEFINER=`etl`@`localhost` SQL SECURITY DEFINER VIEW `dwh_view_partners_monthly_billing_last_updated_at` AS (
SELECT
  `month_id` AS `month_id`,
  `p`.`partner_id` AS `partner_id`,
  MAX(`p`.`updated_at`) AS `updated_at`
FROM (`dwh_dim_time` `months`
   JOIN `dwh_dim_partners_billing` `p`)
WHERE ((`p`.`updated_at` <= LAST_DAY(`months`.`day_id`))
       AND (`months`.`day_id` = (LAST_DAY(`months`.`day_id`) * 1)))
GROUP BY FLOOR((`months`.`day_id` / 100)),`p`.`partner_id`)$$

DELIMITER ;

/*6105*/
/* NO NEED TO ADD billable_storage_mb */
USE `kalturadw`;

ALTER TABLE kalturadw.`dwh_hourly_partner_usage`
ADD COLUMN billable_storage_mb DECIMAL(19,4);

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_partner_billing_storage_per_category`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_partner_billing_storage_per_category`(p_start_date_id INT, p_end_date_id INT ,p_partner_id INT)
BEGIN
   -- Fetch storage per entry of this partner (as seen on selected date)
    
    DROP TEMPORARY TABLE IF EXISTS temp_storage;
    CREATE TEMPORARY TABLE temp_storage(
        category_name       VARCHAR(255) ,
        date_id             INT(11) NOT NULL,
        count_storage_mb    DECIMAL(19,4) NOT NULL
    ) ENGINE = MEMORY;
      
    INSERT INTO     temp_storage (category_name, date_id, count_storage_mb)
    SELECT  IF(ec.updated_at IS NULL OR c.category_name IS NULL,'-', c.category_name) category_name,  
        entry_size_date_id, 
        SUM(entry_additional_size_kb)/1024 aggr_storage_mb
    FROM    kalturadw.dwh_fact_entries_sizes es
        LEFT OUTER JOIN kalturadw.dwh_dim_entry_categories ec ON (ec.entry_id = es.entry_id AND ec.partner_id = es.partner_id)     
        LEFT OUTER JOIN kalturadw.dwh_dim_categories c ON (ec.category_id = c.category_id)
        LEFT OUTER JOIN kalturadw.dwh_dim_entries e ON (ec.partner_id = e.partner_id AND ec.entry_id = e.entry_id AND ec.updated_at = e.updated_at)
    WHERE es.partner_id = p_partner_id
    AND es.entry_size_date_id <= p_end_date_id    
    GROUP BY  IF(ec.updated_at IS NULL OR c.category_name IS NULL,'-', c.category_name) ,  es.entry_size_date_id;
      
    -- fetch results per category
        
    SELECT s.category_name, SUM(s.count_storage_mb)/DAY(LAST_DAY(DATE(all_times.day_id))) avg_continuous_aggr_storage_mb
    FROM   dwh_dim_time all_times, temp_storage s 
    WHERE  all_times.day_id BETWEEN p_start_date_id AND p_end_date_id 
    AND all_times.day_id >= s.date_id 
    AND s.count_storage_mb<>0
    GROUP BY s.category_name;
        
END$$

DELIMITER ;

USE `kalturadw`;

INSERT IGNORE INTO dwh_hourly_partner_usage (date_id, hour_id, partner_id, bandwidth_source_id, count_storage_mb, aggr_storage_mb)
SELECT 
	all_time.day_id date_id, 0 hour_id, 
	partner_id, 1 bandwidth_source_id, 0 count_storage_mb, 
	SUM(count_storage_mb)  aggr_storage_mb
FROM dwh_hourly_partner_usage u, dwh_dim_time all_time
WHERE 
	all_time.day_id <= DATE(NOW())*1 AND all_time.day_id >= date_id
AND count_storage_mb <> 0
AND hour_id = 0
	GROUP BY all_time.day_id , partner_id;
	
/*6106*/
DELIMITER $$

USE `kalturadw_ds`$$

DROP PROCEDURE IF EXISTS `unify_incomplete_api_calls`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `unify_incomplete_api_calls`(p_cycle_id INTEGER)
BEGIN
	DROP TABLE IF EXISTS unified_api_calls;
	CREATE TEMPORARY TABLE unified_api_calls
	SELECT 	cycle_calls.file_id,
		cycle_calls.line_number,
		cycle_calls.session_id,
		cycle_calls.request_index,
		IFNULL(cycle_calls.api_call_time, old_calls.api_call_time) api_call_time,
		IFNULL(cycle_calls.api_call_date_id, old_calls.api_call_date_id) api_call_date_id,
		IFNULL(cycle_calls.api_call_hour_id, old_calls.api_call_hour_id) api_call_hour_id,
		IFNULL(cycle_calls.partner_id, old_calls.partner_id) partner_id,
		IFNULL(cycle_calls.action_id, old_calls.action_id) action_id,
		IFNULL(cycle_calls.os_id, old_calls.os_id) os_id,
		IFNULL(cycle_calls.browser_id, old_calls.browser_id) browser_id,
		IFNULL(cycle_calls.client_tag_id, old_calls.client_tag_id) client_tag_id ,
		IFNULL(cycle_calls.is_admin, old_calls.is_admin) is_admin,
		IFNULL(cycle_calls.pid, old_calls.pid) pid,
		IFNULL(cycle_calls.host_id, old_calls.host_id) host_id,
		IFNULL(cycle_calls.user_ip, old_calls.user_ip) user_ip,
		IFNULL(cycle_calls.user_ip_number, old_calls.user_ip_number) user_ip_number,
		IFNULL(cycle_calls.country_id, old_calls.country_id) country_id,
		IFNULL(cycle_calls.location_id, old_calls.location_id) location_id,
		IFNULL(cycle_calls.master_partner_id, old_calls.master_partner_id) master_partner_id,
		IFNULL(cycle_calls.ks, old_calls.ks) ks,
		IFNULL(cycle_calls.kuser_id, old_calls.kuser_id) kuser_id,
		IFNULL(cycle_calls.is_in_multi_request, old_calls.is_in_multi_request) is_in_multi_request,
		IFNULL(cycle_calls.success, old_calls.success) success,
		IFNULL(cycle_calls.error_code_id, old_calls.error_code_id) error_code_id,
		IFNULL(cycle_calls.duration_msecs, old_calls.duration_msecs) duration_msecs
	FROM 
		kalturadw.dwh_fact_incomplete_api_calls cycle_calls, 
		kalturadw.dwh_fact_incomplete_api_calls old_calls
	WHERE 
		cycle_calls.session_id = old_calls.session_id
		AND cycle_calls.request_index = old_calls.request_index
		AND cycle_calls.cycle_id = p_cycle_id
		AND old_calls.cycle_id <> p_cycle_id
		AND IFNULL(cycle_calls.api_call_date_id, old_calls.api_call_date_id) IS NOT NULL
                AND IFNULL(cycle_calls.duration_msecs, old_calls.duration_msecs) IS NOT NULL;
 
		
	INSERT INTO kalturadw.dwh_fact_api_calls (file_id, line_number, session_id, request_index, api_call_time, api_call_date_id, 
						api_call_hour_id, partner_id, action_id, os_id, browser_id, client_tag_id, is_admin,
						pid, host_id, user_ip, user_ip_number, country_id, location_id, master_partner_id, 
						ks, kuser_id, is_in_multi_request, success, error_code_id, duration_msecs)
	SELECT file_id, line_number, session_id, request_index, api_call_time, api_call_date_id, 
						api_call_hour_id, partner_id, action_id, os_id, browser_id, client_tag_id, is_admin,
						pid, host_id, user_ip, user_ip_number, country_id, location_id, master_partner_id, 
						ks, kuser_id, is_in_multi_request, success, error_code_id, duration_msecs
	FROM unified_api_calls;
 	
		
	INSERT INTO kalturadw.dwh_fact_errors
		(file_id, line_number, partner_id, 
		error_time, error_date_id, error_hour_id, 
		error_object_id, error_object_type_id, error_code_id)
	SELECT 	file_id, line_number, partner_id, 
		api_call_time, api_call_date_id, api_call_hour_id,
		CONCAT(session_id, '_', request_index), error_object_type_id, error_code_id
	FROM unified_api_calls u, kalturadw.dwh_dim_error_object_types eot
	WHERE eot.error_object_type_name = 'API Call'
	AND error_code_id IS NOT NULL;
	
	
	DELETE kalturadw.dwh_fact_incomplete_api_calls
	FROM 	kalturadw.dwh_fact_incomplete_api_calls, 
		unified_api_calls unified_calls
	WHERE 
		kalturadw.dwh_fact_incomplete_api_calls.session_id = unified_calls.session_id
		AND kalturadw.dwh_fact_incomplete_api_calls.request_index = unified_calls.request_index;
END$$

DELIMITER ;

/*6107*/
INSERT INTO kalturadw_ds.parameters (id, parameter_name, date_value, process_id)
values (9, 'transcoding_errors_last_update', DATE(20100101), 9);

INSERT INTO kalturadw_ds.processes (id, process_name, max_files_per_cycle)
values (9, 'transcoding_errors', 0);

/*6108*/
use kalturadw;
INSERT INTO dwh_dim_os VALUES(-1,'UNDEFINED',0,'UNDEFINED','UNDEFINED','UNDEFINED');
INSERT INTO dwh_dim_browser VALUES(-1,'UNDEFINED','UNDEFINED','UNDEFINED','UNDEFINED','UNDEFINED');

/*6109*/
/* unify changes on dwh_dim_entries */
USE `kalturadw`;

DROP TABLE IF EXISTS `dwh_dim_entries_new`;

CREATE TABLE `dwh_dim_entries_new` (
  `entry_id` varchar(20) NOT NULL DEFAULT '',
  `kshow_id` varchar(20) DEFAULT NULL,
  `kuser_id` int(11) DEFAULT '-1',
  `entry_name` varchar(256) DEFAULT NULL,
  `entry_type_id` smallint(6) DEFAULT NULL,
  `entry_media_type_id` smallint(6) DEFAULT NULL,
  `data` varchar(48) DEFAULT NULL,
  `thumbnail` varchar(48) DEFAULT NULL,
  `views` int(11) DEFAULT '0',
  `votes` int(11) DEFAULT '0',
  `comments` int(11) DEFAULT '0',
  `favorites` int(11) DEFAULT '0',
  `total_rank` int(11) DEFAULT '0',
  `rank` int(11) DEFAULT '0',
  `tags` text,
  `anonymous` tinyint(4) DEFAULT NULL,
  `entry_status_id` smallint(6) DEFAULT '-1',
  `entry_media_source_id` smallint(6) DEFAULT '-1',
  `entry_source_id` varchar(48) DEFAULT '-1',
  `source_link` varchar(1024) DEFAULT NULL,
  `entry_license_type_id` smallint(6) DEFAULT '-1',
  `credit` varchar(1024) DEFAULT NULL,
  `length_in_msecs` int(11) DEFAULT '0',
  `height` int(11) DEFAULT NULL,
  `width` int(11) DEFAULT NULL,
  `conversion_quality` varchar(50) DEFAULT NULL,
  `storage_size` bigint(20) DEFAULT NULL,
  `editor_type_id` smallint(6) DEFAULT '-1',
  `puser_id` varchar(64) DEFAULT NULL,
  `is_admin_content` tinyint(4) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `created_date_id` int(11) DEFAULT '-1',
  `created_hour_id` tinyint(4) DEFAULT '-1',
  `updated_at` datetime DEFAULT NULL,
  `updated_date_id` int(11) DEFAULT '-1',
  `updated_hour_id` tinyint(4) DEFAULT '-1',
  `operational_measures_updated_at` datetime DEFAULT NULL,
  `partner_id` int(11) DEFAULT '-1',
  `display_in_search` tinyint(4) DEFAULT NULL,
  `subp_id` int(11) DEFAULT '-1',
  `custom_data` text,
  `search_text` varchar(4096) DEFAULT NULL,
  `screen_name` varchar(20) DEFAULT NULL,
  `site_url` varchar(256) DEFAULT NULL,
  `permissions` int(11) DEFAULT NULL,
  `group_id` varchar(64) DEFAULT NULL,
  `plays` int(11) DEFAULT '0',
  `partner_data` varchar(4096) DEFAULT NULL,
  `int_id` int(11) NOT NULL,
  `indexed_custom_data_1` int(11) DEFAULT NULL,
  `description` text,
  `media_date` datetime DEFAULT NULL,
  `admin_tags` text,
  `moderation_status` tinyint(4) DEFAULT '-1',
  `moderation_count` int(11) DEFAULT NULL,
  `modified_at` datetime DEFAULT NULL,
  `modified_date_id` int(11) DEFAULT '-1',
  `modified_hour_id` tinyint(4) DEFAULT '-1',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  `access_control_id` int(11) DEFAULT NULL,
  `conversion_profile_id` int(11) DEFAULT NULL,
  `categories` varchar(4096) DEFAULT NULL,
  `categories_ids` varchar(1024) DEFAULT NULL,
  `search_text_discrete` varchar(4096) DEFAULT NULL,
  `flavor_params_ids` varchar(512) DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `start_date_id` int(11) DEFAULT NULL,
  `start_hour_id` tinyint(4) DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `end_date_id` int(11) DEFAULT NULL,
  `end_hour_id` tinyint(4) DEFAULT NULL,
  PRIMARY KEY (`entry_id`),
  KEY `partner_id_created_media_type_source` (`partner_id`,`created_at`,`entry_media_type_id`,`entry_media_source_id`),
  KEY `created_at` (`created_at`),
  KEY `modified_at` (`modified_at`),
  KEY `operational_measures_updated_at` (`operational_measures_updated_at`),
  FULLTEXT KEY `all_text` (`entry_name`,`description`,`tags`,`admin_tags`),
  FULLTEXT KEY `search_text_discrete_index` (`search_text_discrete`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

DELIMITER $$

DELIMITER ;

INSERT INTO `kalturadw`.`dwh_dim_entries_new` 
	(`entry_id`, 
	`kshow_id`, 
	`kuser_id`, 
	`entry_name`, 
	`entry_type_id`, 
	`entry_media_type_id`, 
	`data`, 
	`thumbnail`, 
	`views`, 
	`votes`, 
	`comments`, 
	`favorites`, 
	`total_rank`, 
	`rank`, 
	`tags`, 
	`anonymous`, 
	`entry_status_id`, 
	`entry_media_source_id`, 
	`entry_source_id`, 
	`source_link`, 
	`entry_license_type_id`, 
	`credit`, 
	`length_in_msecs`, 
	`height`, 
	`width`, 
	`conversion_quality`, 
	`storage_size`, 
	`editor_type_id`, 
	`puser_id`, 
	`is_admin_content`, 
	`created_at`, 
	`created_date_id`, 
	`created_hour_id`, 
	`updated_at`, 
	`updated_date_id`, 
	`updated_hour_id`, 
	`operational_measures_updated_at`,
	`partner_id`, 
	`display_in_search`, 
	`subp_id`, 
	`custom_data`, 
	`search_text`, 
	`screen_name`, 
	`site_url`, 
	`permissions`, 
	`group_id`, 
	`plays`, 
	`partner_data`, 
	`int_id`, 
	`indexed_custom_data_1`, 
	`description`, 
	`media_date`, 
	`admin_tags`, 
	`moderation_status`, 
	`moderation_count`, 
	`modified_at`, 
	`modified_date_id`, 
	`modified_hour_id`, 
	`dwh_creation_date`, 
	`dwh_update_date`, 
	`ri_ind`, 
	`access_control_id`, 
	`conversion_profile_id`, 
	`categories`, 
	`categories_ids`, 
	`search_text_discrete`, 
	`flavor_params_ids`, 
	`start_date`, 
	`start_date_id`, 
	`start_hour_id`, 
	`end_date`, 
	`end_date_id`, 
	`end_hour_id`
	)
SELECT distinct `entry_id`, 
	`kshow_id`, 
	`kuser_id`, 
	`entry_name`, 
	`entry_type_id`, 
	`entry_media_type_id`, 
	`data`, 
	`thumbnail`, 
	`views`, 
	`votes`, 
	`comments`, 
	`favorites`, 
	`total_rank`, 
	`rank`, 
	`tags`, 
	`anonymous`, 
	`entry_status_id`, 
	`entry_media_source_id`, 
	`entry_source_id`, 
	`source_link`, 
	`entry_license_type_id`, 
	`credit`, 
	`length_in_msecs`, 
	`height`, 
	`width`, 
	`conversion_quality`, 
	`storage_size`, 
	`editor_type_id`, 
	`puser_id`, 
	`is_admin_content`, 
	`created_at`, 
	`created_date_id`, 
	`created_hour_id`, 
	`updated_at`, 
	`updated_date_id`, 
	`updated_hour_id`, 
	NOW(),
	`partner_id`, 
	`display_in_search`, 
	`subp_id`, 
	`custom_data`, 
	`search_text`, 
	`screen_name`, 
	`site_url`, 
	`permissions`, 
	`group_id`, 
	`plays`, 
	`partner_data`, 
	`int_id`, 
	`indexed_custom_data_1`, 
	`description`, 
	`media_date`, 
	`admin_tags`, 
	`moderation_status`, 
	`moderation_count`, 
	`modified_at`, 
	`modified_date_id`, 
	`modified_hour_id`, 
	`dwh_creation_date`, 
	`dwh_update_date`, 
	`ri_ind`, 
	`access_control_id`, 
	`conversion_profile_id`, 
	`categories`, 
	`categories_ids`, 
	`search_text_discrete`, 
	`flavor_params_ids`, 
	`start_date`, 
	`start_date_id`, 
	`start_hour_id`, 
	`end_date`, 
	`end_date_id`, 
	`end_hour_id` from dwh_dim_entries;

DROP TABLE kalturadw.dwh_dim_entries;
RENAME TABLE kalturadw.dwh_dim_entries_new TO kalturadw.dwh_dim_entries;

DELIMITER $$

DROP TRIGGER /*!50032 IF EXISTS */ `dwh_dim_entries_setcreationtime_oninsert`$$

CREATE
    /*!50017 DEFINER = 'root'@'localhost' */
    TRIGGER `dwh_dim_entries_setcreationtime_oninsert` BEFORE INSERT ON `dwh_dim_entries`
    FOR EACH ROW SET new.dwh_creation_date = NOW();
$$

DELIMITER ;

DROP TABLE IF EXISTS `kalturadw`.`dwh_dim_kusers_new`;

CREATE TABLE `kalturadw`.`dwh_dim_kusers_new` (
  `kuser_id` INT NOT NULL ,
  `screen_name` VARCHAR(127) DEFAULT 'missing value',
  `full_name` VARCHAR(40) DEFAULT 'missing value',
  `first_name` VARCHAR(40),
  `last_name` VARCHAR(40),
  `email` VARCHAR(100) DEFAULT 'missing value',
  `date_of_birth` DATE DEFAULT NULL,
   location_id INT DEFAULT -1,
   country_id INT DEFAULT -1,
  `zip` VARCHAR(10) DEFAULT NULL,
  `url_list` VARCHAR(256) DEFAULT NULL,
  `picture` VARCHAR(48) DEFAULT NULL,
  `icon` TINYINT DEFAULT NULL,
  `about_me` VARCHAR(4096) DEFAULT NULL,
  `tags` TEXT,
  `tagline` VARCHAR(256) DEFAULT NULL,
  `network_highschool` VARCHAR(30) DEFAULT NULL,
  `network_college` VARCHAR(30) DEFAULT NULL,
  `network_other` VARCHAR(30) DEFAULT NULL,
  `mobile_num` VARCHAR(16) DEFAULT NULL,
  `mature_content` TINYINT DEFAULT '-1',
  `gender_id` TINYINT DEFAULT NULL,
  `gender_name` VARCHAR(7) DEFAULT NULL,
  `registration_ip` INT DEFAULT NULL,
  `registration_cookie` VARCHAR(256) DEFAULT NULL,
  `im_list` VARCHAR(256) DEFAULT NULL,
  `views` INT DEFAULT '0',
  `fans` INT DEFAULT '0',
  `entries` INT DEFAULT '0',
  `produced_kshows` INT DEFAULT '0',
  `kuser_status_id` INT DEFAULT -1,
  `kuser_status_name` VARCHAR(64) DEFAULT 'missing value',
  `created_at` DATETIME DEFAULT NULL,
  `created_date_id` INT DEFAULT '-1',
   created_hour_id TINYINT DEFAULT '-1',
  `updated_at` DATETIME DEFAULT NULL,
   updated_date_id INT DEFAULT '-1',
   updated_hour_id TINYINT DEFAULT '-1',
  `operational_measures_updated_at` datetime default null,
  `partner_id` INT DEFAULT '-1',
  `display_in_search` TINYINT DEFAULT '1',
  `search_text` VARCHAR(4096) DEFAULT NULL,
  `partner_data` VARCHAR(4096) DEFAULT NULL,
   dwh_creation_date TIMESTAMP NOT NULL DEFAULT 0,
   dwh_update_date TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
   ri_ind TINYINT NOT NULL DEFAULT '0',
   storage_size INT,
   puser_id varchar(100),
   admin_tags text,
   indexed_partner_data_int INT,
   indexed_partner_data_string varchar(64),
   is_admin TINYINT(4),
  PRIMARY KEY (`kuser_id`),
  KEY `partner_id_index` (`partner_id`,`kuser_id`),
  KEY `created_index` (`created_at`),
  KEY `operational_measures_updated_at` (`operational_measures_updated_at`)
) ENGINE=INNODB  DEFAULT CHARSET=utf8;

INSERT INTO `kalturadw`.`dwh_dim_kusers_new` 
	(`kuser_id`, 
	`screen_name`, 
	`full_name`, 
	`first_name`, 
	`last_name`, 
	`email`, 
	`date_of_birth`, 
	`location_id`, 
	`country_id`, 
	`zip`, 
	`url_list`, 
	`picture`, 
	`icon`, 
	`about_me`, 
	`tags`, 
	`tagline`, 
	`network_highschool`, 
	`network_college`, 
	`network_other`, 
	`mobile_num`, 
	`mature_content`, 
	`gender_id`, 
	`gender_name`, 
	`registration_ip`, 
	`registration_cookie`, 
	`im_list`, 
	`views`, 
	`fans`, 
	`entries`, 
	`produced_kshows`, 
	`kuser_status_id`, 
	`kuser_status_name`, 
	`created_at`, 
	`created_date_id`, 
	`created_hour_id`, 
	`updated_at`, 
	`updated_date_id`, 
	`updated_hour_id`, 
	`operational_measures_updated_at`,
	`partner_id`, 
	`display_in_search`, 
	`search_text`, 
	`partner_data`, 
	`dwh_creation_date`, 
	`dwh_update_date`, 
	`ri_ind`, 
	`storage_size`, 
	`puser_id`, 
	`admin_tags`, 
	`indexed_partner_data_int`, 
	`indexed_partner_data_string`, 
	`is_admin`
	)
	SELECT 
	`kuser_id`, 
	`screen_name`, 
	`full_name`, 
	`first_name`, 
	`last_name`, 
	`email`, 
	`date_of_birth`, 
	`location_id`, 
	`country_id`, 
	`zip`, 
	`url_list`, 
	`picture`, 
	`icon`, 
	`about_me`, 
	`tags`, 
	`tagline`, 
	`network_highschool`, 
	`network_college`, 
	`network_other`, 
	`mobile_num`, 
	`mature_content`, 
	`gender_id`, 
	`gender_name`, 
	`registration_ip`, 
	`registration_cookie`, 
	`im_list`, 
	`views`, 
	`fans`, 
	`entries`, 
	`produced_kshows`, 
	`kuser_status_id`, 
	`kuser_status_name`, 
	`created_at`, 
	`created_date_id`, 
	`created_hour_id`, 
	`updated_at`, 
	`updated_date_id`, 
	`updated_hour_id`, 
	NOW(),
	`partner_id`, 
	`display_in_search`, 
	`search_text`, 
	`partner_data`, 
	`dwh_creation_date`, 
	`dwh_update_date`, 
	`ri_ind`, 
	`storage_size`, 
	`puser_id`, 
	`admin_tags`, 
	`indexed_partner_data_int`, 
	`indexed_partner_data_string`, 
	`is_admin`
	FROM kalturadw.dwh_dim_kusers;

DROP TABLE kalturadw.dwh_dim_kusers;
RENAME TABLE kalturadw.dwh_dim_kusers_new TO kalturadw.dwh_dim_kusers;

CREATE TRIGGER `kalturadw`.`dwh_dim_kusers_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_kusers`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();

INSERT kalturadw_ds.parameters (id, parameter_name)
VALUES (4, 'sync_last_execution_plays_views'),
(5, 'sync_start_time_plays_views'),
(6, 'sync_last_execution_kuser_storage'),
(7, 'sync_start_time_kuser_storage');

DELETE FROM kalturadw.aggr_managment where aggr_name in ('plays_views','storage_usage_kuser_sync');

USE kalturadw_ds;

DROP TABLE IF EXISTS `updated_entries`;
DROP TABLE IF EXISTS `updated_kusers_storage_usage`;

DROP PROCEDURE IF EXISTS `create_updated_entries`;
DROP PROCEDURE IF EXISTS `create_updated_kusers_storage_usage`;

USE kalturadw_ds;

DROP TABLE IF EXISTS operational_syncs;

CREATE TABLE operational_syncs (
	operational_sync_id INT(11),
	operational_sync_name VARCHAR(1024),
	group_column VARCHAR(1024),
	entity_table VARCHAR(1024),
	aggregation_phrase VARCHAR(1024),
	aggregation_table VARCHAR(1024),
	bridge_entity VARCHAR(1024),
	bridge_table VARCHAR(1024),
	last_execution_parameter_id INT,	
	execution_start_time_parameter_id INT,
	PRIMARY KEY (operational_sync_id)
	);
	
INSERT INTO operational_syncs 
	(operational_sync_id, operational_sync_name, group_column, entity_table, aggregation_phrase, aggregation_table, 
	bridge_entity, bridge_table, last_execution_parameter_id, execution_start_time_parameter_id)
	VALUES
	(1, 'entry', 'entry_id', 'kalturadw.dwh_dim_entries', 'ifnull(sum(count_plays), 0) plays, ifnull(sum(count_loads), 0) views', 'kalturadw.dwh_hourly_events_entry', NULL, NULL, 4, 5),
	(2, 'kuser', 'kuser_id', 'kalturadw.dwh_dim_kusers', 'ifnull(sum(entry_additional_size_kb), 0) storage_size', 'kalturadw.dwh_fact_entries_sizes', 'entry_id', 'kalturadw.dwh_dim_entries', 6, 7);


DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `mark_operational_sync_as_done`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `mark_operational_sync_as_done`(p_sync_type VARCHAR(55))
BEGIN
	DECLARE v_last_execution_parameter_id INT;
	DECLARE v_execution_start_time_parameter_id INT;
	
	SELECT last_execution_parameter_id, execution_start_time_parameter_id
	INTO	v_last_execution_parameter_id, v_execution_start_time_parameter_id
	FROM kalturadw_ds.operational_syncs WHERE operational_sync_name = p_sync_type;

	UPDATE kalturadw_ds.parameters main, kalturadw_ds.parameters start_time	
	SET main.date_value = start_time.date_value
	WHERE main.id = v_last_execution_parameter_id AND start_time.id = v_execution_start_time_parameter_id;
END$$

DELIMITER ;

/*6110*/
INSERT INTO kalturadw_ds.locks (lock_name, lock_state) VALUES('retention_lock', FALSE);

/*6111*/
USE kalturadw;
DROP TABLE IF EXISTS dwh_hourly_events_devices_new;

CREATE TABLE `dwh_hourly_events_devices_new` (
  `partner_id` int(11) NOT NULL DEFAULT '-1',
  `date_id` int(11) NOT NULL,
  `hour_id` int(11) NOT NULL,
  `location_id` int(11) NOT NULL DEFAULT '-1',
  `country_id` int(11) NOT NULL DEFAULT '-1',
  `os_id` int(11) NOT NULL DEFAULT '-1',
  `browser_id` int(11) NOT NULL DEFAULT '-1',
  `ui_conf_id` int(11) NOT NULL DEFAULT '-1',
  `entry_media_type_id` SMALLINT(6) NOT NULL DEFAULT '-1',
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
  `count_bandwidth_kb` int(11) DEFAULT NULL,
  `total_admins` int(11) DEFAULT NULL,
  `total_media_entries` int(11) DEFAULT NULL,
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`location_id`,`country_id`,`os_id`,`browser_id`,`ui_conf_id`,`entry_media_type_id`),
  KEY `date_id` (`date_id`,`hour_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = InnoDB) */;

CALL kalturadw.apply_table_partitions_to_target_table('dwh_hourly_events_devices');

USE kalturadw;
DROP TABLE IF EXISTS tmp_devices_migration;
CREATE TABLE tmp_devices_migration
SELECT day_id, DATE(19700101) start_time, DATE(19700101) end_time, 0 is_copied FROM kalturadw.dwh_dim_time
WHERE day_id BETWEEN (SELECT IFNULL(MIN(date_id),DATE(NOW())*1) FROM kalturadw.dwh_hourly_events_devices) AND DATE(NOW())*1;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `tmp_fix_devices`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `tmp_fix_devices`()
BEGIN
        DECLARE v_date_id INT;
        DECLARE done INT DEFAULT 0;
        DECLARE fix_devices_cursor CURSOR FOR SELECT day_id FROM kalturadw.tmp_devices_migration WHERE is_copied = 0 ORDER BY day_id;
        DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
        OPEN fix_devices_cursor ;

        read_loop: LOOP
                FETCH fix_devices_cursor  INTO v_date_id;
                IF done THEN
                        LEAVE read_loop;
                END IF;

                UPDATE kalturadw.tmp_devices_migration SET start_time = NOW() WHERE day_id = v_date_id;

                INSERT INTO kalturadw.dwh_hourly_events_devices_new
			SELECT fact.partner_id AS partner_id,
			date_id,
			hour_id,
			location_id,
			country_id,
			os_id,
			browser_id,
			ui_conf_id,
			entry_media_type_id,
			SUM(sum_time_viewed) AS sum_time_viewed,
			SUM(count_time_viewed) AS count_time_viewed,
			SUM(count_plays) AS count_plays,
			SUM(count_loads) AS count_loads,
			SUM(count_plays_25) AS count_plays_25,
			SUM(count_plays_50) AS count_plays_50,
			SUM(count_plays_75) AS count_plays_75,
			SUM(count_plays_100) AS count_plays_100,
			SUM(count_edit) AS count_edit,
			SUM(count_viral) AS count_viral,
			SUM(count_download) AS count_download,
			SUM(count_report) AS count_report,
			SUM(count_buf_start) AS count_buf_start,
			SUM(count_buf_end) AS count_buf_end,
			SUM(count_open_full_screen) AS count_open_full_screen,
			SUM(count_close_full_screen) AS count_close_full_screen,
			SUM(count_replay) AS count_replay,
			SUM(count_seek) AS count_seek,
			SUM(count_open_upload) AS count_open_upload,
			SUM(count_save_publish) AS count_save_publish,
			SUM(count_close_editor) AS count_close_editor,
			SUM(count_pre_bumper_played) AS count_pre_bumper_played,
			SUM(count_post_bumper_played) AS count_post_bumper_played,
			SUM(count_bumper_clicked) AS count_bumper_clicked,
			SUM(count_preroll_started) AS count_preroll_started,
			SUM(count_midroll_started) AS count_midroll_started,
			SUM(count_postroll_started) AS count_postroll_started,
			SUM(count_overlay_started) AS count_overlay_started,
			SUM(count_preroll_clicked) AS count_preroll_clicked,
			SUM(count_midroll_clicked) AS count_midroll_clicked,
			SUM(count_postroll_clicked) AS count_postroll_clicked,
			SUM(count_overlay_clicked) AS count_overlay_clicked,
			SUM(count_preroll_25) AS count_preroll_25,
			SUM(count_preroll_50) AS count_preroll_50,
			SUM(count_preroll_75) AS count_preroll_75,
			SUM(count_midroll_25) AS count_midroll_25,
			SUM(count_midroll_50) AS count_midroll_50,
			SUM(count_midroll_75) AS count_midroll_75,
		       SUM(count_postroll_25) AS count_postroll_25,
			SUM(count_postroll_50) AS count_postroll_50,
			SUM(count_postroll_75) AS count_postroll_75,
			SUM(count_bandwidth_kb) AS count_bandwidth_kb,
			SUM(total_admins) AS total_admins,
			SUM(total_media_entries) AS total_media_entries
			FROM kalturadw.dwh_hourly_events_devices fact, kalturadw.dwh_dim_entries dim
			WHERE fact.entry_id = dim.entry_id
			AND fact.date_id = v_date_id
			GROUP BY partner_id, date_id, hour_id, location_id, country_id, os_id, browser_id, ui_conf_id, entry_media_type_id;


                UPDATE kalturadw.tmp_devices_migration SET end_time = NOW(), is_copied = 1 WHERE day_id = v_date_id;
        END LOOP;
        CLOSE fix_devices_cursor;

    END$$

DELIMITER ;

CALL kalturadw.tmp_fix_devices();

DROP TABLE tmp_devices_migration;
DROP PROCEDURE IF EXISTS `tmp_fix_devices`;

DROP TABLE kalturadw.dwh_hourly_events_devices;
RENAME TABLE kalturadw.dwh_hourly_events_devices_new TO kalturadw.dwh_hourly_events_devices;

UPDATE kalturadw_ds.aggr_name_resolver
SET 	aggr_id_field = 'country_id,location_id,os_id,browser_id,ui_conf_id, entry_media_type_id'
WHERE aggr_name = 'devices';

/*6112*/
INSERT IGNORE INTO kalturadw.dwh_dim_ui_conf (ui_conf_type_id, 
						partner_id, 
						subp_id, 
						created_at, 
						created_date_id,
						created_hour_id, 
						updated_at, 
						updated_date_id, 
						updated_hour_id,
						ui_conf_status_id, 
						ui_conf_id, 
						ri_ind)
			SELECT 	DISTINCT 	-1 ui_conf_type_id, 
						-1 partner_id, 
						-1 subp_id,
						"2099-01-01 00:00:00" created_at, 
						-1 created_date_id,
						-1 created_hour_id, 
						"2099-01-01 00:00:00" updated_at,
						-1 updated_date_id, 
						-1 updated_hour_id,
						-1 ui_conf_status_id, 
						ui_conf_id,
						1 ri_ind
			FROM kalturadw.dwh_hourly_events_devices;

INSERT INTO kalturadw.`ri_mapping` (`table_name`, `column_name`, `date_id_column_name`, `date_column_name`, `reference_table`, `reference_column`, `perform_check`)
      VALUES ('dwh_hourly_events_devices', 'ui_conf_id', 'date_id', '', 'dwh_dim_ui_conf', 'ui_conf_id', '1');

	  
/*6113*/

USE `kalturadw_ds`;

ALTER TABLE aggr_name_resolver 
	DROP COLUMN aggr_join_stmt,
	CHANGE aggr_id_field aggr_id_field VARCHAR(1024) NOT NULL DEFAULT '',
	ADD dim_id_field VARCHAR(1024) NOT NULL DEFAULT '' AFTER aggr_id_field;
	
UPDATE kalturadw_ds.aggr_name_resolver
	SET dim_id_field = 'entry_id', aggr_id_field = ''
WHERE aggr_name = 'entry';

UPDATE kalturadw_ds.aggr_name_resolver
	SET dim_id_field = 'entry_media_type_id', aggr_id_field = 'country_id,location_id,os_id,browser_id,ui_conf_id'
WHERE aggr_name = 'devices';

UPDATE kalturadw_ds.aggr_name_resolver
	SET dim_id_field = 'kuser_id', aggr_id_field = ''
WHERE aggr_name = 'uid';

/*6114*/
ALTER TABLE kalturadw.dwh_dim_entry_media_source ADD entry_media_source_category VARCHAR(25) NOT NULL DEFAULT 'IMPORT' AFTER entry_media_source_name ;

UPDATE kalturadw.dwh_dim_entry_media_source 
	SET entry_media_source_category = CASE entry_media_source_id WHEN 1 THEN 'UPLOAD' WHEN 2 THEN 'WEBCAM' ELSE 'IMPORT' END;

	
/*6115*/
DELIMITER $$

USE `kalturadw`$$

DROP VIEW IF EXISTS `dwh_view_monthly_active_partners`$$

CREATE ALGORITHM=UNDEFINED DEFINER=`etl`@`%` SQL SECURITY DEFINER VIEW `dwh_view_monthly_active_partners` AS (
select
  CAST(substr(date_id, 1, 6) as SIGNED INT) as month_id,
  `partner_id` AS `partner_id`,
  sum((((((ifnull(`new_videos`,0) + ifnull(`new_images`,0)) + ifnull(`new_audios`,0)) + ifnull(`new_playlists`,0)) + ifnull(`new_livestreams`,0)) + ifnull(`new_other_entries`,0))) AS `new_entries`,
  sum(`count_plays`) AS `count_plays`
from `dwh_hourly_partner` `hourly`
group by `month_id`,`partner_id`
having ((`new_entries` > 10)
        and (`count_plays` > 100)))$$

DELIMITER ;
 
/*6116*/
/*6117*/
DROP PROCEDURE IF EXISTS `kalrueadw`.`recalc_aggr_day`;

/*6118*/
USE kalturadw;

DROP TABLE IF EXISTS dwh_entry_plays_views;

CREATE TABLE dwh_entry_plays_views
(entry_id VARCHAR(255), 
updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
plays INT(11), 
views INT(11), 
PRIMARY KEY (entry_id),
KEY (updated_at));

USE kalturadw;

DELIMITER $$

CREATE PROCEDURE add_plays_views(p_date_id INT, p_hour_id INT)
BEGIN
    INSERT INTO dwh_entry_plays_views(entry_id, plays, views)
    SELECT aggr.entry_id, IFNULL(count_plays, 0) plays, IFNULL(count_loads, 0) views
    FROM kalturadw.dwh_hourly_events_entry aggr
    WHERE date_id = p_date_id AND hour_id = p_hour_id 
    ON DUPLICATE KEY UPDATE 
        plays = plays + VALUES(plays) ,
        views = views + VALUES(views); 

END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `post_aggregation_entry`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `post_aggregation_entry`(date_val DATE, p_hour_id INT(11))
BEGIN
	CALL add_plays_views(date_val*1, p_hour_id);
END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `remove_plays_views`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `remove_plays_views`(p_date_id INT, p_hour_id INT)
BEGIN
	  DECLARE v_done INT DEFAULT FALSE;
  DECLARE v_entry_id VARCHAR(20);
  DECLARE v_plays INT;
  
  DECLARE entries CURSOR FOR SELECT entry_id, count_plays FROM dwh_hourly_events_entry WHERE date_id = p_date_id AND hour_id = p_hour_id;
  
  DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = TRUE;

  OPEN entries;
  
  read_loop: LOOP
    FETCH entries INTO v_entry_id, v_plays;
     
    IF v_done THEN
      LEAVE read_loop;
    END IF;
    
    UPDATE dwh_entry_plays_views
	SET plays = 
		IF((IFNULL(plays, 0) - IFNULL(v_plays, 0))<0, 
			0, 
			IFNULL(plays, 0) - IFNULL(v_plays, 0))
	WHERE v_entry_id = entry_id;	

  END LOOP;

  CLOSE entries;

END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `pre_aggregation_entry`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `pre_aggregation_entry`(date_val DATE, p_hour_id INT(11))
BEGIN
	CALL remove_plays_views(date_val*1, p_hour_id);
END$$

DELIMITER ;

USE kalturadw;

DROP PROCEDURE IF EXISTS calc_plays_views;

DELIMITER &&

CREATE PROCEDURE calc_plays_views()
BEGIN
    DECLARE v_date_id INT(11);
    DECLARE v_hour_id INT(11);
    DECLARE v_done INT(1) DEFAULT 0;

    DECLARE c_partitions CURSOR FOR SELECT DISTINCT date_id, hour_id FROM kalturadw.dwh_hourly_events_entry;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET v_done = 1;

    OPEN c_partitions;

    TRUNCATE TABLE dwh_entry_plays_views;

    INSERT INTO dwh_entry_plays_views (entry_id, plays, views, updated_at)
    SELECT entry_id, plays, views, DATE(20000101)
    FROM entry_plays_views_before_08_2009;

    read_loop: LOOP
        FETCH c_partitions INTO v_date_id, v_hour_id;
        IF v_done = 1 THEN
         LEAVE read_loop;
        END IF;

        INSERT INTO dwh_entry_plays_views(entry_id, plays, views)
        SELECT aggr.entry_id, IFNULL(SUM(count_plays), 0) plays, IFNULL(SUM(count_loads), 0) views
        FROM kalturadw.dwh_hourly_events_entry aggr
        WHERE date_id BETWEEN v_date_id AND v_date_id AND hour_id = v_hour_id
        group by entry_id
        ON DUPLICATE KEY UPDATE
        plays = plays + VALUES(plays),
        views = views + VALUES(views),
        updated_at = DATE(20000101);
    END LOOP read_loop;
    CLOSE c_partitions;

    UPDATE dwh_entry_plays_views p, dwh_dim_entries e
    SET p.updated_at = e.operational_measures_updated_at
    WHERE e.entry_id = p.entry_id;

END&&

DELIMITER ;

call calc_plays_views();

DROP PROCEDURE IF EXISTS calc_plays_views;


/*6119*/
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_api_calls`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_api_calls`(p_date_val DATE, p_hour_id INT)
BEGIN
        DECLARE v_ignore DATE;
        DECLARE v_from_archive DATE;
        DECLARE v_table_name VARCHAR(100);
        DECLARE v_aggr_table VARCHAR(100);
        DECLARE v_aggr_id_field_str VARCHAR(100);
 
        SELECT aggr_table, IF(IFNULL(aggr_id_field,'')='','', CONCAT(', ', aggr_id_field)) aggr_id_field
        INTO  v_aggr_table, v_aggr_id_field_str
        FROM kalturadw_ds.aggr_name_resolver
        WHERE aggr_name = 'api_calls';
 
 
        UPDATE aggr_managment SET start_time = NOW() WHERE aggr_name = 'api_calls' AND date_id = DATE(p_date_val)*1 AND hour_id = p_hour_id;
 
        SELECT MAX(DATE(NOW() - INTERVAL archive_delete_days_back DAY))
        INTO v_ignore
        FROM kalturadw_ds.retention_policy
        WHERE table_name IN ('dwh_fact_api_calls');
 
        IF (p_date_val >= v_ignore) THEN
                SET @s = CONCAT('DELETE FROM kalturadw.',v_aggr_table, ' WHERE date_id = DATE(\'',p_date_val,'\')*1 and hour_id = ', p_hour_id);
		
                PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
                SELECT DATE(archive_last_partition)
                INTO v_from_archive
                FROM kalturadw_ds.retention_policy
                WHERE table_name = 'dwh_fact_api_calls';
                IF (p_date_val >= v_from_archive) THEN
                        SET v_table_name = 'dwh_fact_api_calls';
                ELSE
                        SET v_table_name = 'dwh_fact_api_calls_archive';
                END IF;
                SET @s = CONCAT('INSERT INTO kalturadw.', v_aggr_table, ' (partner_id, date_id, hour_id ', v_aggr_id_field_str,', count_calls,count_is_admin,count_is_in_multi_request,count_success,sum_duration_msecs)'
				'SELECT partner_id, api_call_date_id, api_call_hour_id hour_id', v_aggr_id_field_str,', count(*), sum(is_admin), sum(is_in_multi_request), sum(success), sum(duration_msecs)
				FROM ', v_table_name, '    WHERE api_call_date_id=date(\'',p_date_val,'\')*1 AND api_call_hour_id = ',p_hour_id,'
				GROUP BY partner_id', v_aggr_id_field_str);
		
                PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
        END IF;
        UPDATE aggr_managment SET end_time = NOW() WHERE aggr_name = 'api_calls' AND date_id = DATE(p_date_val)*1 AND hour_id = p_hour_id;
END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_bandwidth`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_bandwidth`(p_date_val DATE, p_aggr_name VARCHAR(100))
BEGIN
	DECLARE v_ignore DATE;
	DECLARE v_from_archive DATE;
        DECLARE v_table_name VARCHAR(100);
	DECLARE v_aggr_table VARCHAR(100);
	DECLARE v_aggr_id_field_str VARCHAR(100);
	
	UPDATE aggr_managment SET start_time = NOW() WHERE aggr_name = p_aggr_name AND date_id = DATE(p_date_val)*1;
	SELECT MAX(DATE(NOW() - INTERVAL archive_delete_days_back DAY))
	INTO v_ignore
	FROM kalturadw_ds.retention_policy
	WHERE table_name IN('dwh_fact_bandwidth_usage', 'dwh_fact_fms_sessions');
	
	IF (p_date_val >= v_ignore) THEN -- not so old, we don't have any data
		
		SELECT aggr_table, IF(IFNULL(aggr_id_field,'')='','', CONCAT(', ', aggr_id_field)) aggr_id_field
		INTO  v_aggr_table, v_aggr_id_field_str
		FROM kalturadw_ds.aggr_name_resolver
		WHERE aggr_name = p_aggr_name;
		
		SET @s = CONCAT('UPDATE kalturadw.',v_aggr_table, ' SET count_bandwidth_kb = NULL WHERE date_id = DATE(\'',p_date_val,'\')*1');
		PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
		
		/* HTTP */
		SELECT DATE(archive_last_partition)
		INTO v_from_archive
		FROM kalturadw_ds.retention_policy
		WHERE table_name = 'dwh_fact_bandwidth_usage';
	
                IF (p_date_val >= v_from_archive) THEN -- aggr from archive or from events
                        SET v_table_name = 'dwh_fact_bandwidth_usage';
                ELSE
                        SET v_table_name = 'dwh_fact_bandwidth_usage_archive';
                END IF;
                
		SET @s = CONCAT('INSERT INTO kalturadw.', v_aggr_table, ' (partner_id, date_id, hour_id ', v_aggr_id_field_str,', count_bandwidth_kb)'
				'SELECT partner_id, MAX(activity_date_id), 0 hour_id', v_aggr_id_field_str,', SUM(bandwidth_bytes)/1024 count_bandwidth
				FROM ', v_table_name, '	WHERE activity_date_id=date(\'',p_date_val,'\')*1
				GROUP BY partner_id', v_aggr_id_field_str,'
				ON DUPLICATE KEY UPDATE	count_bandwidth_kb=VALUES(count_bandwidth_kb)');
	

		PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
			
		/* FMS */
		SELECT DATE(archive_last_partition)
		INTO v_from_archive
		FROM kalturadw_ds.retention_policy
		WHERE table_name = 'dwh_fact_fms_sessions';
		
		IF (p_date_val >= v_from_archive) THEN -- aggr from archive or from events
                        SET v_table_name = 'dwh_fact_fms_sessions';
                ELSE
                        SET v_table_name = 'dwh_fact_fms_sessions_archive';
                END IF;

		SET @s = CONCAT('INSERT INTO kalturadw.', v_aggr_table, ' (partner_id, date_id, hour_id', v_aggr_id_field_str,', count_bandwidth_kb)
				SELECT session_partner_id, MAX(session_date_id), 0 hour_id', v_aggr_id_field_str,', SUM(total_bytes)/1024 count_bandwidth 
				FROM kalturadw.dwh_fact_fms_sessions WHERE session_date_id=date(\'',p_date_val,'\')*1
				GROUP BY session_partner_id', v_aggr_id_field_str,'
				ON DUPLICATE KEY UPDATE	count_bandwidth_kb=VALUES(count_bandwidth_kb)');
		PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
	
	END IF;
	
	UPDATE aggr_managment SET end_time = NOW() WHERE aggr_name = p_aggr_name AND date_id = DATE(p_date_val)*1;
END$$

DELIMITER ;

 DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_errors`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_errors`(p_date_val DATE, p_hour_id INT)
BEGIN
        DECLARE v_ignore DATE;
        DECLARE v_from_archive DATE;
        DECLARE v_table_name VARCHAR(100);
        DECLARE v_aggr_table VARCHAR(100);
        DECLARE v_aggr_id_field_str VARCHAR(100);
 
        SELECT aggr_table, IF(IFNULL(aggr_id_field,'')='','', CONCAT(', ', aggr_id_field)) aggr_id_field
        INTO  v_aggr_table, v_aggr_id_field_str
        FROM kalturadw_ds.aggr_name_resolver
        WHERE aggr_name = 'errors';
 
 
        UPDATE aggr_managment SET start_time = NOW() WHERE aggr_name = 'errors' AND date_id = DATE(p_date_val)*1 AND hour_id = p_hour_id;
 
        SELECT MAX(DATE(NOW() - INTERVAL archive_delete_days_back DAY))
        INTO v_ignore
        FROM kalturadw_ds.retention_policy
        WHERE table_name IN ('dwh_fact_errors');
 
        IF (p_date_val >= v_ignore) THEN
                SET @s = CONCAT('DELETE FROM kalturadw.',v_aggr_table, ' WHERE date_id = DATE(\'',p_date_val,'\')*1 and hour_id = ', p_hour_id);
		
                PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
                SELECT DATE(archive_last_partition)
                INTO v_from_archive
                FROM kalturadw_ds.retention_policy
                WHERE table_name = 'dwh_fact_errors';
 
                IF (p_date_val >= v_from_archive) THEN
                        SET v_table_name = 'dwh_fact_errors';
                ELSE
                        SET v_table_name = 'dwh_fact_errors_archive';
                END IF;
                SET @s = CONCAT('INSERT INTO kalturadw.', v_aggr_table, ' (partner_id, date_id, hour_id ', v_aggr_id_field_str,', count_errors)'
				'SELECT partner_id, error_date_id, error_hour_id hour_id', v_aggr_id_field_str,', count(*)
				FROM ', v_table_name, '    WHERE error_date_id=date(\'',p_date_val,'\')*1 AND error_hour_id = ',p_hour_id,'
				GROUP BY partner_id', v_aggr_id_field_str);
		
                PREPARE stmt FROM  @s;
                EXECUTE stmt;
                DEALLOCATE PREPARE stmt;
        END IF;
        UPDATE aggr_managment SET end_time = NOW() WHERE aggr_name = 'errors' AND date_id = DATE(p_date_val)*1 AND hour_id = p_hour_id;
END$$

DELIMITER ;

ALTER TABLE kalturadw.aggr_managment
	ADD data_insert_time datetime,
	CHANGE aggr_day_int date_id int(11) unsigned NOT NULL,
	DROP COLUMN aggr_day;

DELETE FROM kalturadw.aggr_managment
	WHERE (is_calculated = 1 or date(date_id) + interval hour_id hour > now());

UPDATE kalturadw.aggr_managment
SET data_insert_time = now();

ALTER TABLE kalturadw.aggr_managment
	DROP COLUMN is_calculated;

DELIMITER $$

USE `kalturadw_ds`$$

DROP PROCEDURE IF EXISTS `transfer_cycle_partition`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `transfer_cycle_partition`(p_cycle_id VARCHAR(10))
BEGIN
	DECLARE src_table VARCHAR(45);
	DECLARE tgt_table VARCHAR(45);
	DECLARE dup_clause VARCHAR(4000);
	DECLARE partition_field VARCHAR(45);
	DECLARE select_fields VARCHAR(4000);
	DECLARE post_transfer_sp_val VARCHAR(4000);
	DECLARE v_ignore_duplicates_on_transfer BOOLEAN;	
	DECLARE aggr_date VARCHAR(400);
	DECLARE aggr_hour VARCHAR(400);
	DECLARE aggr_names VARCHAR(4000);
	
	
	DECLARE done INT DEFAULT 0;
	DECLARE staging_areas_cursor CURSOR FOR SELECT 	source_table, target_table, IFNULL(on_duplicate_clause,''),	staging_partition_field, post_transfer_sp, aggr_date_field, hour_id_field, post_transfer_aggregations, ignore_duplicates_on_transfer
											FROM staging_areas s, cycles c
											WHERE s.process_id=c.process_id AND c.cycle_id = p_cycle_id;
											
	DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
	OPEN staging_areas_cursor;
	
	read_loop: LOOP
		FETCH staging_areas_cursor INTO src_table, tgt_table, dup_clause, partition_field, post_transfer_sp_val, aggr_date, aggr_hour, aggr_names, v_ignore_duplicates_on_transfer;
		IF done THEN
			LEAVE read_loop;
		END IF;
		
		IF ((LENGTH(AGGR_DATE) > 0) && (LENGTH(aggr_names) > 0)) THEN
		
			SET @s = CONCAT(
				'INSERT INTO kalturadw.aggr_managment(aggr_name, date_id, hour_id, data_insert_time)
				SELECT aggr_name, aggr_date, aggr_hour, now() 
				FROM kalturadw_ds.aggr_name_resolver a, 
					(select distinct ',aggr_date, ' aggr_date,' ,aggr_hour,' aggr_hour 
					 from ',src_table,
					' where ',partition_field,' = ',p_cycle_id,') ds
				WHERE aggr_name in ', aggr_names,'
				ON DUPLICATE KEY UPDATE data_insert_time = now()');

			PREPARE stmt FROM @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;
		END IF;

		SELECT 	GROUP_CONCAT(column_name ORDER BY ordinal_position)
		INTO 	select_fields
		FROM information_schema.COLUMNS
		WHERE CONCAT(table_schema,'.',table_name) = tgt_table;
			
		SET @s = CONCAT('INSERT ', IF(v_ignore_duplicates_on_transfer=1, 'IGNORE', '') ,' INTO ',tgt_table, ' (',select_fields,') ',
						' SELECT ',select_fields,
						' FROM ',src_table,
						' WHERE ',partition_field,'  = ',p_cycle_id,
						' ',dup_clause );

		PREPARE stmt FROM @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
			
		IF LENGTH(POST_TRANSFER_SP_VAL)>0 THEN
				SET @s = CONCAT('CALL ',post_transfer_sp_val,'(',p_cycle_id,')');
				
				PREPARE stmt FROM  @s;
				EXECUTE stmt;
				DEALLOCATE PREPARE stmt;
		END IF;
	END LOOP;

	CLOSE staging_areas_cursor;
END$$

DELIMITER ;

/*6120*/
ALTER TABLE kalturadw.aggr_managment
	ADD COLUMN ri_time DATETIME,
	CHANGE hour_id hour_id INT(11) UNSIGNED NOT NULL AFTER date_id;
	
UPDATE kalturadw.aggr_managment
SET ri_time = end_time;

/*6121*/
/*6122*/

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `get_data_for_operational`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `get_data_for_operational`(p_sync_type VARCHAR(55))
BEGIN
	DECLARE v_execution_start_time DATETIME;
	
	DECLARE v_group_column VARCHAR(1024);
	DECLARE v_entity_table VARCHAR(1024);
	DECLARE v_aggregation_phrase VARCHAR(1024);
	DECLARE v_aggregation_table VARCHAR(1024);
	DECLARE v_bridge_entity VARCHAR(1024);
	DECLARE v_bridge_table VARCHAR(1024);
	DECLARE v_last_execution_parameter_id INT;
	DECLARE v_execution_start_time_parameter_id INT;
	
	SET v_execution_start_time = NOW();
   
	SELECT group_column, entity_table, aggregation_phrase, aggregation_table, 
            bridge_entity, bridge_table, last_execution_parameter_id, execution_start_time_parameter_id
        INTO	v_group_column, v_entity_table, v_aggregation_phrase, v_aggregation_table, 
            v_bridge_entity, v_bridge_table, v_last_execution_parameter_id, v_execution_start_time_parameter_id
        FROM kalturadw_ds.operational_syncs WHERE operational_sync_name = p_sync_type;
        UPDATE kalturadw_ds.parameters	SET date_value = v_execution_start_time WHERE id = v_execution_start_time_parameter_id;
    
    IF p_sync_type='entry' THEN 
    
	SELECT partner_id, e.entry_id, e.plays, e.views
        FROM dwh_entry_plays_views e, kalturadw_ds.parameters p, kalturadw.dwh_dim_entries d
        WHERE e.updated_at > p.date_value AND p.id = 4 AND e.entry_id = d.entry_id;
    
    ELSE
    
        SET @s = CONCAT('SELECT dim.', v_group_column,', ', v_aggregation_phrase, 
                ' FROM ', v_aggregation_table ,' aggr, ', IF (v_bridge_table IS NULL, '', CONCAT(v_bridge_table, ' bridge, ')), v_entity_table, ' dim, kalturadw_ds.parameters p',
                ' WHERE aggr.', IF(v_bridge_entity IS NULL, v_group_column, 
                            CONCAT(v_bridge_entity, ' = bridge.',v_bridge_entity, ' AND bridge.', v_group_column)), 
                ' = dim.', v_group_column, ' AND dim.operational_measures_updated_at > p.date_value AND p.id = ', v_last_execution_parameter_id,
                ' GROUP BY dim.',v_group_column);
        
        PREPARE stmt FROM  @s;
        EXECUTE stmt;
        DEALLOCATE PREPARE stmt;
        
    END IF;
END$$

DELIMITER ;

/*6123*/

USE kalturadw;
INSERT INTO dwh_dim_partners 
	(partner_id, admin_email,admin_name,adult_content,anonymous_kuser_id,commercial_use,content_categories,
	created_at,created_date_id,created_hour_id,description,moderate_content,notify,partner_name,partner_package,
	partner_status_id,partner_status_name,partner_type_id,partner_type_name,updated_at,updated_date_id,updated_hour_id ,ri_ind) 
SELECT DISTINCT a.partner_id, "Missing Value","Missing Value","-1","-1","-1", "Missing Value",
	"2099-01-01 00:00:00","-1","-1","Missing Value","-1","-1","Missing Value","-1",
	"-1","Missing Value","-1","Missing Value","2099-01-01 00:00:00","-1","-1" ,1 
	FROM kalturadw.dwh_hourly_errors a LEFT OUTER JOIN kalturadw.dwh_dim_partners b  
	ON a.partner_id = b.partner_id
	WHERE b.partner_id IS NULL AND a.partner_id IS NOT NULL;

INSERT INTO kalturadw.ri_mapping
(table_name, column_name, date_id_column_name, date_column_name, reference_table, reference_column, perform_check)
VALUES ('dwh_hourly_errors', 'partner_id', 'date_id', '', 'dwh_dim_partners', 'partner_id', '1');

/*6124*/
/*6125*/
/*6126*/
DROP TABLE IF EXISTS `kalturadw`.`dwh_dim_applications`;

CREATE TABLE `kalturadw`.`dwh_dim_applications` (
  `application_id` INT AUTO_INCREMENT NOT NULL ,
  `name` VARCHAR(128) DEFAULT 'missing value',
  `partner_id` INT(11) NOT NULL,
  `dwh_creation_date` TIMESTAMP NOT NULL DEFAULT 0,
  `dwh_update_date` TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
  `ri_ind` TINYINT NOT NULL DEFAULT '0',
  PRIMARY KEY (`application_id`),
  KEY `partner_id_name_index` (`partner_id`,`name`)
) ENGINE=MYISAM  DEFAULT CHARSET=utf8;

CREATE TRIGGER `kalturadw`.`dwh_dim_applications_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_applications`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();

/*6127*/
DROP TABLE IF EXISTS `kalturadw`.`dwh_dim_pusers`;

CREATE TABLE `kalturadw`.`dwh_dim_pusers` (
  `puser_id` INT AUTO_INCREMENT NOT NULL ,
  `name` VARCHAR(100) DEFAULT 'missing value',
  `partner_id` INT NOT NULL,
  `dwh_creation_date` TIMESTAMP NOT NULL DEFAULT 0,
  `dwh_update_date` TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
  `ri_ind` TINYINT NOT NULL DEFAULT '0',
  PRIMARY KEY (`puser_id`),
  KEY `partner_name_index` (`partner_id`,`name`)
) ENGINE=MYISAM  DEFAULT CHARSET=utf8;

CREATE TRIGGER `kalturadw`.`dwh_dim_pusers_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_pusers`
    FOR EACH ROW 
	SET new.dwh_creation_date = NOW();

/*6128*/
DROP TABLE IF EXISTS `kalturadw`.`dwh_dim_permissions`;

CREATE TABLE `kalturadw`.`dwh_dim_permissions` (
  `permission_id` INT(11) NOT NULL,
  `type` INT(11) NOT NULL,
  `name` VARCHAR(100) NOT NULL,
  `partner_id` int(11) NOT NULL,
  `status` int(11) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `dwh_creation_date` TIMESTAMP NOT NULL DEFAULT 0,
  `dwh_update_date` TIMESTAMP NOT NULL DEFAULT NOW() ON UPDATE NOW(),
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`permission_id`),
  KEY `partner_permission_index` (`partner_id`,`name`),
  KEY `dwh_update_date` (`dwh_update_date`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TRIGGER `kalturadw`.`dwh_dim_permissions_setcreationtime_oninsert` BEFORE INSERT
    ON `kalturadw`.`dwh_dim_permissions`
    FOR EACH ROW 
        SET new.dwh_creation_date = NOW();


INSERT INTO kalturadw_ds.pentaho_sequences VALUES(10,'dimensions/update_permissions.ktr',1,TRUE);

DELIMITER $$

USE `kalturadw`$$

DROP VIEW IF EXISTS `dwh_dim_user_reports_allowed_partners`$$


CREATE ALGORITHM=UNDEFINED DEFINER=`etl`@`localhost` SQL SECURITY DEFINER VIEW `dwh_dim_user_reports_allowed_partners` AS (
SELECT  `dwh_dim_permissions`.`partner_id` AS `partner_id`,
		DATE(STR_TO_DATE(`dwh_dim_permissions`.`created_at`, '%Y-%m-%d %H:%i:%S.%f'))*1 AS `created_date_id`,
		HOUR(STR_TO_DATE(`dwh_dim_permissions`.`created_at`, '%Y-%m-%d %H:%i:%S.%f')) AS `created_hour_id`
FROM `dwh_dim_permissions` 
WHERE ((`dwh_dim_permissions`.`name` = 'FEATURE_END_USER_REPORTS') AND (`dwh_dim_permissions`.`status` = 1)))$$

DELIMITER ;

/*6129*/
USE kalturadw;

DROP TABLE IF EXISTS kalturadw.dwh_fact_events_new;

CREATE TABLE kalturadw.dwh_fact_events_new (
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
  `context_id` int(11) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `application_id` int(11) DEFAULT NULL,
  `feature_type` int(11) DEFAULT NULL,
  PRIMARY KEY (`file_id`,`event_id`,`event_date_id`),
  KEY `Entry_id` (`entry_id`),
  KEY `event_hour_id_event_date_id_partner_id` (`event_hour_id`,`event_date_id`,`partner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (event_date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = INNODB) */;

USE kalturadw;

DROP TABLE IF EXISTS `dwh_fact_events_archive_new`;
CREATE TABLE `dwh_fact_events_archive_new` (
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
  `context_id` int(11) DEFAULT NULL,
  `user_id` int(11) DEFAULT NULL,
  `application_id` int(11) DEFAULT NULL,
  `feature_type` int(11) DEFAULT NULL
) ENGINE=ARCHIVE DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (event_date_id)
(PARTITION p_0 VALUES LESS THAN (1) ENGINE = ARCHIVE)*/;

USE kalturadw;

DELIMITER $$

DROP PROCEDURE IF EXISTS `apply_table_partitions_to_target_table`$$
CREATE DEFINER=`etl`@`localhost` PROCEDURE `apply_table_partitions_to_target_table`(p_table_name VARCHAR(255))
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
END$$

DELIMITER ;

CALL apply_table_partitions_to_target_table('dwh_fact_events_archive');
CALL apply_table_partitions_to_target_table('dwh_fact_events');

DROP PROCEDURE IF EXISTS `apply_table_partitions_to_target_table`;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `do_tables_to_new`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `do_tables_to_new`(p_greater_than_or_equal_date_id int, p_less_than_date_id int, p_table_name varchar(256))
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
		/*Gets the select fields as an intersect of both the existing and the new table (in the case that the new table has a new column and missing a deprecated column*/
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
	
END$$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS `all_tables_to_new`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `all_tables_to_new`()
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
	
END$$

DELIMITER ;

use kalturadw_ds;

DROP TABLE IF EXISTS tables_to_new;

CREATE TABLE tables_to_new AS
SELECT partition_name, table_name, partition_expression column_name, IF(@last_partition_description<=partition_description,@last_partition_description, 0) greater_than_or_equal_date_id, @last_partition_description:=partition_description AS less_than_date_id, 0 is_copied
FROM information_schema.PARTITIONS p
WHERE p.table_name = 'dwh_fact_events_archive'
ORDER BY table_name, partition_ordinal_position;

SET SESSION binlog_format = 'STATEMENT';

CALL kalturadw.all_tables_to_new();

SET SESSION binlog_format = 'ROW';

use kalturadw_ds;

DROP TABLE IF EXISTS tables_to_new;
CREATE TABLE tables_to_new AS
SELECT partition_name, table_name, partition_expression column_name, @last_partition_description last_p,
IF(@last_partition_description<=partition_description,@last_partition_description, 0) greater_than_or_equal_date_id, @last_partition_description:=partition_description AS less_than_date_id, 0 is_copied
FROM information_schema.PARTITIONS p
WHERE p.table_name = 'dwh_fact_events'
ORDER BY table_name, partition_ordinal_position;

SET SESSION binlog_format = 'STATEMENT';

CALL kalturadw.all_tables_to_new();

SET SESSION binlog_format = 'ROW';

ALTER TABLE kalturadw_ds.ds_events ADD context_id int(11);
ALTER TABLE kalturadw_ds.ds_events ADD user_id int(11);
ALTER TABLE kalturadw_ds.ds_events ADD application_id int(11);
ALTER TABLE kalturadw_ds.ds_events ADD feature_type int(11);

RENAME TABLE kalturadw.dwh_fact_events TO kalturadw.dwh_fact_events_old;
RENAME TABLE kalturadw.dwh_fact_events_new TO kalturadw.dwh_fact_events;
RENAME TABLE kalturadw.dwh_fact_events_archive TO kalturadw.dwh_fact_events_archive_old;
RENAME TABLE kalturadw.dwh_fact_events_archive_new TO kalturadw.dwh_fact_events_archive;

DROP TABLE IF EXISTS kalturadw.dwh_fact_events_old;
DROP TABLE IF EXISTS kalturadw.dwh_fact_events_archive_old;


DROP PROCEDURE IF EXISTS kalturadw.do_tables_to_new;
DROP PROCEDURE IF EXISTS kalturadw.all_tables_to_new;


/*6130*/
ALTER TABLE kalturadw_ds.aggr_name_resolver ADD COLUMN join_table VARCHAR(60) NOT NULL;
ALTER TABLE kalturadw_ds.aggr_name_resolver ADD COLUMN join_id_field VARCHAR(60) NOT NULL;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day`(p_date_val DATE,p_hour_id INT(11), p_aggr_name VARCHAR(100))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	DECLARE v_aggr_id_field VARCHAR(100);
	DECLARE extra VARCHAR(100);
	DECLARE v_from_archive DATE;
	DECLARE v_ignore DATE;
	DECLARE v_table_name VARCHAR(100);
	DECLARE v_join_table VARCHAR(100);
	DECLARE v_join_condition VARCHAR(200);
    		
	SELECT DATE(NOW() - INTERVAL archive_delete_days_back DAY), DATE(archive_last_partition)
	INTO v_ignore, v_from_archive
	FROM kalturadw_ds.retention_policy
	WHERE table_name = 'dwh_fact_events';	
	
	IF (p_date_val >= v_ignore) THEN 
	
		SELECT aggr_table, aggr_id_field
		INTO  v_aggr_table, v_aggr_id_field
		FROM kalturadw_ds.aggr_name_resolver
		WHERE aggr_name = p_aggr_name;	
		
	        SET extra = CONCAT('pre_aggregation_',p_aggr_name);
        	IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME=extra) THEN
	            SET @ss = CONCAT('CALL ',extra,'(''', p_date_val,''',',p_hour_id,');'); 
        	    PREPARE stmt1 FROM  @ss;
	            EXECUTE stmt1;
	            DEALLOCATE PREPARE stmt1;
	        END IF ;
        
        	IF (v_aggr_table <> '') THEN 
			SET @s = CONCAT('delete from ',v_aggr_table,' where date_id = DATE(''',p_date_val,''')*1 and hour_id = ',p_hour_id);
			PREPARE stmt FROM  @s;
			EXECUTE stmt;
			DEALLOCATE PREPARE stmt;	
		END IF;
		
		SET @s = CONCAT('INSERT INTO aggr_managment(aggr_name, date_id, hour_id, data_insert_time)
				VALUES(''',p_aggr_name,''',''',DATE(p_date_val)*1,''',',p_hour_id,',NOW())
				ON DUPLICATE KEY UPDATE data_insert_time = values(data_insert_time)');
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	
		IF (p_date_val >= v_from_archive) THEN 
			SET v_table_name = 'dwh_fact_events';
		ELSE
			SET v_table_name = 'dwh_fact_events_archive';
		END IF;
		
		SELECT aggr_table, CONCAT(
						IF(aggr_id_field <> '', CONCAT(',', aggr_id_field),'') ,
						IF(dim_id_field <> '', 	CONCAT(', e.', REPLACE(dim_id_field,',',', e.')), '')
					  )
		INTO  v_aggr_table, v_aggr_id_field
		FROM kalturadw_ds.aggr_name_resolver
		WHERE aggr_name = p_aggr_name;
		
		SELECT IF(join_table <> '' , CONCAT(',', join_table), ''), IF(join_table <> '', CONCAT(' AND ev.' ,join_id_field,'=',join_table,'.',join_id_field), '')
		INTO v_join_table, v_join_condition
		FROM kalturadw_ds.aggr_name_resolver
		WHERE aggr_name = p_aggr_name;
		
		
		SET @s = CONCAT('UPDATE aggr_managment SET start_time = NOW()
				WHERE aggr_name = ''',p_aggr_name,''' AND date_id = ''',DATE(p_date_val)*1,''' AND hour_id = ',p_hour_id);
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		IF ( v_aggr_table <> '' ) THEN
			SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
				(partner_id
				,date_id
				,hour_id
				',REPLACE(v_aggr_id_field,'e.',''),' 
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
			SELECT  ev.partner_id,ev.event_date_id, event_hour_id',v_aggr_id_field,',
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
			FROM ',v_table_name,' as ev USE INDEX (event_hour_id_event_date_id_partner_id), dwh_dim_entries e',v_join_table,
				' WHERE ev.event_type_id BETWEEN 2 AND 40 
				AND ev.event_date_id  = DATE(''',p_date_val,''')*1
				AND ev.event_hour_id = ',p_hour_id,'
				AND e.entry_media_type_id IN (1,2,5,6)  /* allow only video & audio & mix */
			AND e.entry_id = ev.entry_id ' ,v_join_condition, 
			' GROUP BY partner_id,event_date_id, event_hour_id',v_aggr_id_field,';');
		
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
					
		SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
				(partner_id
				,date_id
				,hour_id
				',REPLACE(v_aggr_id_field,'e.',''),'
				,sum_time_viewed
				,count_time_viewed)
				SELECT partner_id, event_date_id, event_hour_id',v_aggr_id_field,',
				SUM(duration / 60 / 4 * (v_25+v_50+v_75+v_100)) sum_time_viewed,
				COUNT(DISTINCT s_play) count_time_viewed
				FROM(
				SELECT ev.partner_id, ev.event_date_id, ev.event_hour_id',v_aggr_id_field,', ev.session_id,
					MAX(duration) duration,
					COUNT(DISTINCT IF(ev.event_type_id IN (4),1,NULL)) v_25,
					COUNT(DISTINCT IF(ev.event_type_id IN (5),1,NULL)) v_50,
					COUNT(DISTINCT IF(ev.event_type_id IN (6),1,NULL)) v_75,
					COUNT(DISTINCT IF(ev.event_type_id IN (7),1,NULL)) v_100,
					MAX(IF(event_type_id IN (3),session_id,NULL)) s_play
				FROM ',v_table_name,' as ev USE INDEX (event_hour_id_event_date_id_partner_id), dwh_dim_entries e',v_join_table,
				' WHERE ev.event_date_id  = DATE(''',p_date_val,''')*1
					AND ev.event_hour_id = ',p_hour_id,'
					AND e.entry_media_type_id IN (1,2,5,6)  /* allow only video & audio & mix */
					AND e.entry_id = ev.entry_id
					AND ev.event_type_id IN(3,4,5,6,7) /* time viewed only when player reaches 25,50,75,100 */ ',v_join_condition,
				' GROUP BY ev.partner_id, ev.event_date_id, ev.event_hour_id , ev.entry_id',v_aggr_id_field,',ev.session_id) e
				GROUP BY partner_id, event_date_id, event_hour_id',v_aggr_id_field,'
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
		
	END IF; 
	
	SET @s = CONCAT('UPDATE aggr_managment SET end_time = NOW() WHERE aggr_name = ''',p_aggr_name,''' AND date_id = ''',DATE(p_date_val)*1,''' AND hour_id =',p_hour_id);
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
		
END$$

DELIMITER ;

/*6131*/
USE `kalturadw`;

DROP TABLE IF EXISTS kalturadw.`dwh_hourly_events_context_entry_user_app`;

CREATE TABLE kalturadw.`dwh_hourly_events_context_entry_user_app` (
  `partner_id` INT NOT NULL DEFAULT -1,
  `date_id` INT NOT NULL,
  `hour_id` INT NOT NULL,
  `entry_id` VARCHAR(20) NOT NULL DEFAULT -1,
  `user_id` INT NOT NULL DEFAULT -1,
  `context_id` INT NOT NULL DEFAULT -1,
  `application_id` INT NOT NULL DEFAULT -1,
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
  PRIMARY KEY `partner_id` (`partner_id`,`date_id`,`hour_id`,`entry_id`,`user_id`,`context_id`,`application_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8
PARTITION BY RANGE (date_id)
(PARTITION p_201207 VALUES LESS THAN (20120801) ENGINE = INNODB);

CALL kalturadw.add_monthly_partition_for_table('dwh_hourly_events_context_entry_user_app');

INSERT INTO kalturadw_ds.aggr_name_resolver (aggr_name, aggr_table, aggr_id_field, dim_id_field, aggr_type, join_table, join_id_field)
VALUES ('users', 'dwh_hourly_events_context_entry_user_app', 'user_id,context_id,application_id', 'entry_id', 'events', 'dwh_dim_user_reports_allowed_partners', 'partner_id');

UPDATE kalturadw_ds.staging_areas  SET post_transfer_aggregations = REPLACE(post_transfer_aggregations, ')',',\'users\')') WHERE process_id in (1,3) AND source_table = 'ds_events';


/*6132*/
USE `kalturadw`;

DROP TABLE IF EXISTS kalturadw.`dwh_hourly_events_context_app`;

CREATE TABLE kalturadw.`dwh_hourly_events_context_app` (
  `partner_id` INT NOT NULL DEFAULT -1,
  `date_id` INT NOT NULL,
  `hour_id` INT NOT NULL,
  `context_id` INT NOT NULL DEFAULT -1,
  `application_id` INT NOT NULL DEFAULT -1,
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
  PRIMARY KEY `partner_id` (`partner_id`,`date_id`,`hour_id`,`context_id`,`application_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8
PARTITION BY RANGE (date_id)
(PARTITION p_201207 VALUES LESS THAN (20120801) ENGINE = INNODB);

CALL kalturadw.add_monthly_partition_for_table('dwh_hourly_events_context_app');

INSERT INTO kalturadw_ds.aggr_name_resolver (aggr_name, aggr_table, aggr_id_field, dim_id_field, aggr_type, join_table, join_id_field)
VALUES ('context', 'dwh_hourly_events_context_app', 'context_id,application_id', '', 'events', 'dwh_dim_user_reports_allowed_partners', 'partner_id');

UPDATE kalturadw_ds.staging_areas  SET post_transfer_aggregations = REPLACE(post_transfer_aggregations, ')',',\'context\')') WHERE process_id in (1,3);

/*6133*/
USE `kalturadw`;

DROP TABLE IF EXISTS `dwh_dim_entries_new`;

CREATE TABLE `dwh_dim_entries_new` (
  `entry_id` varchar(20) NOT NULL DEFAULT '',
  `kshow_id` varchar(20) DEFAULT NULL,
  `kuser_id` int(11) DEFAULT '-1',
  `entry_name` varchar(256) DEFAULT NULL,
  `entry_type_id` smallint(6) DEFAULT NULL,
  `entry_media_type_id` smallint(6) DEFAULT NULL,
  `data` varchar(48) DEFAULT NULL,
  `thumbnail` varchar(48) DEFAULT NULL,
  `views` int(11) DEFAULT '0',
  `votes` int(11) DEFAULT '0',
  `comments` int(11) DEFAULT '0',
  `favorites` int(11) DEFAULT '0',
  `total_rank` int(11) DEFAULT '0',
  `rank` int(11) DEFAULT '0',
  `tags` text,
  `anonymous` tinyint(4) DEFAULT NULL,
  `entry_status_id` smallint(6) DEFAULT '-1',
  `entry_media_source_id` smallint(6) DEFAULT '-1',
  `entry_source_id` varchar(48) DEFAULT '-1',
  `source_link` varchar(1024) DEFAULT NULL,
  `entry_license_type_id` smallint(6) DEFAULT '-1',
  `credit` varchar(1024) DEFAULT NULL,
  `length_in_msecs` int(11) DEFAULT '0',
  `height` int(11) DEFAULT NULL,
  `width` int(11) DEFAULT NULL,
  `conversion_quality` varchar(50) DEFAULT NULL,
  `storage_size` bigint(20) DEFAULT NULL,
  `editor_type_id` smallint(6) DEFAULT '-1',
  `puser_id` varchar(64) DEFAULT NULL,
  `is_admin_content` tinyint(4) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `created_date_id` int(11) DEFAULT '-1',
  `created_hour_id` tinyint(4) DEFAULT '-1',
  `updated_at` datetime DEFAULT NULL,
  `updated_date_id` int(11) DEFAULT '-1',
  `updated_hour_id` tinyint(4) DEFAULT '-1',
  `operational_measures_updated_at` datetime DEFAULT NULL,
  `partner_id` int(11) DEFAULT '-1',
  `display_in_search` tinyint(4) DEFAULT NULL,
  `subp_id` int(11) DEFAULT '-1',
  `custom_data` text,
  `screen_name` varchar(20) DEFAULT NULL,
  `site_url` varchar(256) DEFAULT NULL,
  `permissions` int(11) DEFAULT NULL,
  `group_id` varchar(64) DEFAULT NULL,
  `plays` int(11) DEFAULT '0',
  `partner_data` varchar(4096) DEFAULT NULL,
  `int_id` int(11) NOT NULL,
  `indexed_custom_data_1` int(11) DEFAULT NULL,
  `description` text,
  `media_date` datetime DEFAULT NULL,
  `admin_tags` text,
  `moderation_status` tinyint(4) DEFAULT '-1',
  `moderation_count` int(11) DEFAULT NULL,
  `modified_at` datetime DEFAULT NULL,
  `modified_date_id` int(11) DEFAULT '-1',
  `modified_hour_id` tinyint(4) DEFAULT '-1',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  `access_control_id` int(11) DEFAULT NULL,
  `conversion_profile_id` int(11) DEFAULT NULL,
  `categories` varchar(4096) DEFAULT NULL,
  `categories_ids` varchar(1024) DEFAULT NULL,
  `flavor_params_ids` varchar(512) DEFAULT NULL,
  `start_date` datetime DEFAULT NULL,
  `start_date_id` int(11) DEFAULT NULL,
  `start_hour_id` tinyint(4) DEFAULT NULL,
  `end_date` datetime DEFAULT NULL,
  `end_date_id` int(11) DEFAULT NULL,
  `end_hour_id` tinyint(4) DEFAULT NULL,
  `prev_kuser_id` int(11) DEFAULT NULL,
  `kuser_updated_date_id` int(11) DEFAULT '-1'
) ENGINE=INNODB DEFAULT CHARSET=utf8;

INSERT INTO kalturadw.dwh_dim_entries_new (`entry_id`,`kshow_id`,`kuser_id`,`entry_name`,`entry_type_id`,`entry_media_type_id`,`data`,`thumbnail`,`views`,`votes`,`comments`,`favorites`,`total_rank`,`rank`,`tags`,`anonymous`,`entry_status_id`,`entry_media_source_id`,`entry_source_id`,`source_link`,`entry_license_type_id`,`credit`,`length_in_msecs`,`height`,`width`,`conversion_quality`,`storage_size`,`editor_type_id`,`puser_id`,`is_admin_content`,`created_at`,`created_date_id`,`created_hour_id`,`updated_at`,`updated_date_id`,`updated_hour_id`,`operational_measures_updated_at`,`partner_id`,`display_in_search`,`subp_id`,`custom_data`,`screen_name`,`site_url`,`permissions`,`group_id`,`plays`,`partner_data`,`int_id`,`indexed_custom_data_1`,`description`,`media_date`,`admin_tags`,`moderation_status`,`moderation_count`,`modified_at`,`modified_date_id`,`modified_hour_id`,`dwh_creation_date`,`dwh_update_date`,`ri_ind`,`access_control_id`,`conversion_profile_id`,`categories`,`categories_ids`,`flavor_params_ids`,`start_date`,`start_date_id`,`start_hour_id`,`end_date`,`end_date_id`,`end_hour_id`)
SELECT `entry_id`,`kshow_id`,`kuser_id`,`entry_name`,`entry_type_id`,`entry_media_type_id`,`data`,`thumbnail`,`views`,`votes`,`comments`,`favorites`,`total_rank`,`rank`,`tags`,`anonymous`,`entry_status_id`,`entry_media_source_id`,`entry_source_id`,`source_link`,`entry_license_type_id`,`credit`,`length_in_msecs`,`height`,`width`,`conversion_quality`,`storage_size`,`editor_type_id`,`puser_id`,`is_admin_content`,`created_at`,`created_date_id`,`created_hour_id`,`updated_at`,`updated_date_id`,`updated_hour_id`,`operational_measures_updated_at`,`partner_id`,`display_in_search`,`subp_id`,`custom_data`,`screen_name`,`site_url`,`permissions`,`group_id`,`plays`,`partner_data`,`int_id`,`indexed_custom_data_1`,`description`,`media_date`,`admin_tags`,`moderation_status`,`moderation_count`,`modified_at`,`modified_date_id`,`modified_hour_id`,`dwh_creation_date`,`dwh_update_date`,`ri_ind`,`access_control_id`,`conversion_profile_id`,`categories`,`categories_ids`,`flavor_params_ids`,`start_date`,`start_date_id`,`start_hour_id`,`end_date`,`end_date_id`,`end_hour_id` 
FROM kalturadw.dwh_dim_entries;

ALTER TABLE kalturadw.dwh_dim_entries_new
  ADD PRIMARY KEY (`entry_id`), 
  ADD KEY `partner_id_created_media_type_source` (`partner_id`,`created_at`,`entry_media_type_id`,`entry_media_source_id`),
  ADD KEY `created_at` (`created_at`),
  ADD KEY `updated_at` (`updated_at`),
  ADD KEY `modified_at` (`modified_at`),
  ADD KEY `operational_measures_updated_at` (`operational_measures_updated_at`);
  
DROP TABLE kalturadw.dwh_dim_entries;
RENAME TABLE kalturadw.dwh_dim_entries_new TO kalturadw.dwh_dim_entries;

DELIMITER $$

DROP TRIGGER /*!50032 IF EXISTS */ `dwh_dim_entries_setcreationtime_oninsert`$$

CREATE
    /*!50017 DEFINER = 'root'@'localhost' */
    TRIGGER `dwh_dim_entries_setcreationtime_oninsert` BEFORE INSERT ON `dwh_dim_entries` 
    FOR EACH ROW SET new.dwh_creation_date = NOW();
$$

DELIMITER ;

USE `kalturadw`;

DROP TABLE IF EXISTS kalturadw.`dwh_hourly_user_usage`;

CREATE TABLE kalturadw.`dwh_hourly_user_usage` (
  `partner_id` INT(11) NOT NULL,
  `kuser_id` INT(11) NOT NULL,
  `date_id` INT(11) NOT NULL,
  `hour_id` INT(11) NOT NULL,
  `added_storage_kb`  DECIMAL(19,4) DEFAULT 0.0000,
  `deleted_storage_kb`  DECIMAL(19,4) DEFAULT 0.0000,
  `total_storage_kb` DECIMAL(19,4) ,
  `added_entries`  INT(11) DEFAULT 0,
  `deleted_entries`  INT(11) DEFAULT 0,
  `total_entries` INT(11) ,
  `added_msecs`  INT(11) DEFAULT 0,
  `deleted_msecs`  INT(11) DEFAULT 0,
  `total_msecs` INT(11) ,
  PRIMARY KEY (`partner_id`, `kuser_id`,`date_id`, `hour_id`),
  KEY (`date_id`, `hour_id`)
) ENGINE=INNODB DEFAULT CHARSET=utf8
PARTITION BY RANGE (date_id)
(PARTITION p_201207 VALUES LESS THAN (20120801) ENGINE = INNODB);
 
 CALL kalturadw.add_monthly_partition_for_table('dwh_hourly_user_usage');

 
 DELIMITER $

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `add_partitions`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `add_partitions`()
BEGIN
	CALL add_daily_partition_for_table('dwh_fact_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_session_events');
	CALL add_daily_partition_for_table('dwh_fact_fms_sessions');
	CALL add_daily_partition_for_table('dwh_fact_bandwidth_usage');
	CALL add_daily_partition_for_table('dwh_fact_api_calls');
    CALL add_daily_partition_for_table('dwh_fact_incomplete_api_calls');
    CALL add_daily_partition_for_table('dwh_fact_errors');
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
	CALL add_monthly_partition_for_table('dwh_hourly_api_calls');
    CALL add_monthly_partition_for_table('dwh_hourly_errors');
	CALL add_monthly_partition_for_table('dwh_hourly_events_context_entry_user_app');
	CALL add_monthly_partition_for_table('dwh_hourly_events_context_app');
	CALL add_monthly_partition_for_table('dwh_hourly_user_usage');
END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_user_usage`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_user_usage`(p_date_id INT(11))
BEGIN

    DECLARE v_date DATETIME;
    SET v_date = DATE(p_date_id);
	
    UPDATE aggr_managment SET start_time = NOW() WHERE aggr_name = 'user_storage_usage' AND date_id = p_date_id;
    
    DROP TABLE IF EXISTS temp_aggr_storage;
    CREATE TEMPORARY TABLE temp_aggr_storage(
        partner_id          INT(11) NOT NULL,
        kuser_id            INT(11) NOT NULL,
        added_storage_kb    DECIMAL(19,4) NOT NULL DEFAULT 0.0000,
        deleted_storage_kb  DECIMAL(19,4) NOT NULL DEFAULT 0.0000
    ) ENGINE = MEMORY;
    
    ALTER TABLE temp_aggr_storage ADD INDEX index_1 (kuser_id);  
    
    INSERT INTO     temp_aggr_storage (partner_id, kuser_id, added_storage_kb, deleted_storage_kb)
    SELECT         e.partner_id, e.kuser_id, SUM(if(f.entry_additional_size_kb > 0,entry_additional_size_kb,0)),SUM(IF(f.entry_additional_size_kb < 0,entry_additional_size_kb*-1,0))
    FROM         dwh_fact_entries_sizes f, dwh_dim_entries e
    WHERE        entry_size_date_id=p_date_id
    AND          f.entry_id = e.entry_id
    AND          e.entry_type_id IN (1,2,7,10)
    GROUP BY     e.kuser_id;
    
    DROP TABLE IF EXISTS entries_prev_owner;
    CREATE TEMPORARY TABLE entries_prev_owner AS
    SELECT partner_id, entry_id, prev_kuser_id, kuser_id 
    FROM dwh_dim_entries
    WHERE prev_kuser_id IS NOT NULL
	AND updated_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
    AND kuser_updated_date_id = p_date_id
    AND created_date_id <> p_date_id
    AND entry_type_id IN (1,2,7,10);
 
    ALTER TABLE entries_prev_owner ADD INDEX index_1 (kuser_id);
    
    INSERT INTO  temp_aggr_storage (partner_id, kuser_id, added_storage_kb, deleted_storage_kb)
    SELECT       o.partner_id, o.prev_kuser_id, 0, SUM(f.entry_additional_size_kb)
    FROM         dwh_fact_entries_sizes f, entries_prev_owner o
    WHERE        f.entry_id = o.entry_id
    AND          f.entry_size_date_id < p_date_id
    GROUP BY     o.prev_kuser_id;
    
    
    INSERT INTO  temp_aggr_storage (partner_id, kuser_id, added_storage_kb, deleted_storage_kb)
    SELECT       o.partner_id, o.kuser_id, SUM(f.entry_additional_size_kb), 0
    FROM         dwh_fact_entries_sizes f, entries_prev_owner o
    WHERE        f.entry_id = o.entry_id
    AND          f.entry_size_date_id < p_date_id
    GROUP BY     o.kuser_id;
    
    DROP TABLE IF EXISTS temp_aggr_entries;
    CREATE TEMPORARY TABLE temp_aggr_entries(
        partner_id          INT(11) NOT NULL,
        kuser_id             INT(11) NOT NULL,
        added_entries    INT(11) NOT NULL DEFAULT 0,
        deleted_entries  INT(11) NOT NULL DEFAULT 0,
        added_msecs INT(11) NOT NULL DEFAULT 0,
        deleted_msecs INT(11) NOT NULL DEFAULT 0
        
    ) ENGINE = MEMORY; 
    
    ALTER TABLE temp_aggr_entries ADD INDEX index_1 (`kuser_id`);
    
    INSERT INTO temp_aggr_entries(partner_id, kuser_id, added_entries, deleted_entries, added_msecs, deleted_msecs)
    SELECT partner_id, kuser_id,
    SUM(IF(entry_status_id IN (0,1,2,4) AND (created_date_id = p_date_id OR kuser_updated_date_id = p_date_id),1,0)),
    SUM(IF(entry_status_id = 3 AND (created_date_id <> p_date_id AND kuser_updated_date_id <> p_date_id),1,0)),
    SUM(IF(entry_status_id IN (0,1,2,4) AND (created_date_id = p_date_id OR kuser_updated_date_id = p_date_id),length_in_msecs,0)),
    SUM(IF(entry_status_id = 3 AND (created_date_id <> p_date_id AND kuser_updated_date_id <> p_date_id),length_in_msecs,0))
    FROM dwh_dim_entries e
    WHERE updated_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
    AND e.entry_type_id IN (1,2,7,10)
    GROUP BY kuser_id;
    
    INSERT INTO temp_aggr_entries(partner_id, kuser_id, added_entries, deleted_entries, added_msecs, deleted_msecs)
    SELECT o.partner_id, o.prev_kuser_id, 0, COUNT(*), 0, SUM(length_in_msecs)
    FROM entries_prev_owner o, dwh_dim_entries e
    WHERE o.entry_id = e.entry_id
    GROUP BY o.prev_kuser_id;
   
    
    DELETE FROM dwh_hourly_user_usage USING temp_aggr_storage, dwh_hourly_user_usage 
    WHERE dwh_hourly_user_usage.partner_id = temp_aggr_storage.partner_id 
    AND dwh_hourly_user_usage.kuser_id = temp_aggr_storage.kuser_id 
    AND dwh_hourly_user_usage.date_id = p_date_id;
    
    DROP TABLE IF EXISTS latest_total;
    CREATE TEMPORARY TABLE latest_total(
        partner_id          INT(11) NOT NULL,
        kuser_id             INT(11) NOT NULL,
        total_storage_kb    DECIMAL(19,4) NOT NULL DEFAULT 0,
        total_entries  INT(11) NOT NULL DEFAULT 0,
        total_msecs INT(11) NOT NULL DEFAULT 0
        
    ) ENGINE = MEMORY; 
    ALTER TABLE latest_total ADD INDEX index_1 (kuser_id);
        
    
    INSERT INTO latest_total (partner_id, kuser_id, total_storage_kb, total_entries, total_msecs)
    SELECT u.partner_id, u.kuser_id, IFNULL(u.total_storage_kb,0), IFNULL(u.total_entries,0), IFNULL(u.total_msecs,0)
    FROM dwh_hourly_user_usage u JOIN (SELECT kuser_id, MAX(date_id) AS date_id FROM dwh_hourly_user_usage GROUP BY kuser_id) MAX
          ON u.kuser_id = max.kuser_id AND u.date_id = max.date_id; 
          
    INSERT INTO dwh_hourly_user_usage (partner_id, kuser_id, date_id, hour_id, added_storage_kb, deleted_storage_kb, total_storage_kb, added_entries, deleted_entries, total_entries, added_msecs, deleted_msecs, total_msecs)
    SELECT      aggr.partner_id, aggr.kuser_id, p_date_id, 0, SUM(added_storage_kb), SUM(deleted_storage_kb), SUM(added_storage_kb) - SUM(deleted_storage_kb) + IFNULL(latest_total.total_storage_kb,0),
                0, 0, IFNULL(latest_total.total_entries,0), 0, 0,  IFNULL(latest_total.total_msecs,0)
    FROM        temp_aggr_storage aggr LEFT JOIN latest_total ON aggr.kuser_id = latest_total.kuser_id
    WHERE added_storage_kb <> 0 OR deleted_storage_kb <> 0
    GROUP BY    aggr.kuser_id;
        
    INSERT INTO dwh_hourly_user_usage (partner_id, kuser_id, date_id, hour_id, added_storage_kb, deleted_storage_kb, total_storage_kb, added_entries, deleted_entries, total_entries, added_msecs, deleted_msecs, total_msecs)
    SELECT         aggr.partner_id, aggr.kuser_id, p_date_id, 0, 0, 0, IFNULL(latest_total.total_storage_kb,0), SUM(added_entries), SUM(deleted_entries), SUM(added_entries) - SUM(deleted_entries) + IFNULL(latest_total.total_entries,0),
            SUM(added_msecs), SUM(deleted_msecs), SUM(added_msecs) - SUM(deleted_msecs) + IFNULL(latest_total.total_msecs,0)
    FROM         temp_aggr_entries aggr LEFT JOIN latest_total ON aggr.kuser_id = latest_total.kuser_id
    WHERE added_entries <> 0 OR added_msecs <> 0 OR deleted_entries <> 0 OR deleted_msecs <> 0
    GROUP BY     aggr.kuser_id
    ON DUPLICATE KEY UPDATE added_entries = VALUES(added_entries), deleted_entries = VALUES(deleted_entries), total_entries=VALUES(total_entries), 
                            added_msecs=VALUES(added_msecs), deleted_msecs=VALUES(deleted_msecs), total_msecs=VALUES(total_msecs);
    
    
    UPDATE aggr_managment SET end_time = NOW() WHERE aggr_name = 'user_storage_usage' AND date_id = p_date_id; 
 
END$$

DELIMITER ;


/*6134*/
DELIMITER $$

USE `kalturadw`$$

DROP FUNCTION IF EXISTS `calc_partner_storage_data_time_range`$$

CREATE DEFINER=`root`@`localhost` FUNCTION `calc_partner_storage_data_time_range`(p_start_date_id INT, p_end_date_id INT ,p_partner_id INT ) RETURNS DECIMAL(19,4)
    DETERMINISTIC
BEGIN	
	DECLARE total_billable_storage_mb DECIMAL (19,4);
  	
    SELECT MAX(aggr_storage_mb)
    INTO total_billable_storage_mb
    FROM dwh_hourly_partner_usage aggr_p
    WHERE 
    IF (p_start_date_id = p_end_date_id, date_id = p_start_date_id,
	FLOOR(date_id/100) BETWEEN FLOOR(p_start_date_id/100) AND FLOOR(p_end_date_id/100))
    AND aggr_p.partner_id = p_partner_id
    AND aggr_p.hour_id = 0
    AND aggr_p.bandwidth_source_id = 1;
    
	RETURN total_billable_storage_mb;
END$$


DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_partner_usage_data`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `calc_partner_usage_data`(p_date_id INT(11),p_partner_id INT,p_total BOOL)
BEGIN
    IF(p_total) THEN
    
	    SELECT 
		month_id,
	    SUM(free.avg_continuous_aggr_storage_mb) avg_continuous_aggr_storage_mb,
	    SUM(free.sum_partner_bandwidth_kb) sum_partner_bandwidth_kb
	    FROM 
	    (SELECT
		FLOOR(date_id/100) month_id,
		SUM(aggr_storage_mb/IF(FLOOR(date_id/100)=FLOOR(LEAST(p_date_id,DATE(NOW())*1)),DAY(LEAST(p_date_id,DATE(NOW())*1)),DAY(LAST_DAY(date_id)))) avg_continuous_aggr_storage_mb,
		SUM(count_bandwidth_kb) sum_partner_bandwidth_kb
	    FROM dwh_hourly_partner_usage 
	    WHERE partner_id=p_partner_id AND hour_id = 0
	    AND date_id <= LEAST(p_date_id,DATE(NOW())*1)
	    GROUP BY month_id) AS free;
     ELSE
	
	SELECT
		FLOOR(date_id/100) month_id,
		SUM(aggr_storage_mb)/DAY(LEAST(p_date_id,DATE(NOW())*1)) avg_continuous_aggr_storage_mb,
		SUM(count_bandwidth_kb) sum_partner_bandwidth_kb
	    FROM dwh_hourly_partner_usage 
	    WHERE partner_id=p_partner_id AND hour_id = 0
	    AND FLOOR(date_id/100) = FLOOR(LEAST(p_date_id,DATE(NOW())*1)/100);
     END IF;
END$$

DELIMITER ;

USE `kalturadw`;

INSERT IGNORE INTO dwh_hourly_partner_usage (date_id, hour_id, partner_id, bandwidth_source_id, count_storage_mb, aggr_storage_mb)
SELECT 
	all_time.day_id date_id, 0 hour_id, 
	partner_id, 1 bandwidth_source_id, 0 count_storage_mb, 
	SUM(count_storage_mb)  aggr_storage_mb
FROM dwh_hourly_partner_usage u, dwh_dim_time all_time
WHERE 
	all_time.day_id <= DATE(NOW())*1 AND all_time.day_id >= date_id
AND count_storage_mb <> 0
AND hour_id = 0
	GROUP BY all_time.day_id , partner_id;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_storage`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_partner_storage`(date_val DATE)
BEGIN
	DELETE FROM kalturadw.dwh_hourly_partner_usage WHERE date_id = DATE(date_val)*1 AND IFNULL(count_bandwidth_kb,0) = 0 AND (IFNULL(count_storage_mb,0) > 0 OR IFNULL(aggr_storage_mb,0) > 0);
    UPDATE kalturadw.dwh_hourly_partner_usage SET count_storage_mb = NULL, aggr_storage_mb=NULL WHERE date_id = DATE(date_val)*1 AND IFNULL(count_bandwidth_kb,0) > 0;
	
	DROP TABLE IF EXISTS temp_aggr_storage;
	CREATE TEMPORARY TABLE temp_aggr_storage(
		partner_id      	INT(11) NOT NULL,
		date_id     		INT(11) NOT NULL,
		hour_id	 		    TINYINT(4) NOT NULL,
		count_storage_mb	DECIMAL(19,4) NOT NULL
	) ENGINE = MEMORY;
      
	INSERT INTO 	temp_aggr_storage (partner_id, date_id, hour_id, count_storage_mb)
   	SELECT 		partner_id, MAX(entry_size_date_id), 0 hour_id, SUM(entry_additional_size_kb)/1024 count_storage_mb
	FROM 		dwh_fact_entries_sizes
	WHERE		entry_size_date_id=DATE(date_val)*1
	GROUP BY 	partner_id;
	
	INSERT INTO 	kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, count_storage_mb, aggr_storage_mb)
	SELECT		partner_id, DATE(date_val)*1, 0 hour_id, 1, 0, IFNULL(aggr_storage_mb,0)
	FROM        kalturadw.dwh_hourly_partner_usage
	WHERE       date_id = DATE(date_val - INTERVAL 1 DAY)*1
	AND         bandwidth_source_id = 1
	ON DUPLICATE KEY UPDATE count_storage_mb=VALUES(count_storage_mb), aggr_storage_mb = VALUES(aggr_storage_mb);
	
	INSERT INTO 	kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, count_storage_mb, aggr_storage_mb)
	SELECT		aggr.partner_id, aggr.date_id, aggr.hour_id, 1, aggr.count_storage_mb, aggr.count_storage_mb
	FROM		temp_aggr_storage aggr 
	ON DUPLICATE KEY UPDATE count_storage_mb=VALUES(count_storage_mb), aggr_storage_mb=aggr_storage_mb + VALUES(aggr_storage_mb) ;

	
END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_entries_sizes`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_entries_sizes`(p_date_id INT(11))
BEGIN
                DECLARE v_date DATETIME;
                SET v_date = DATE(p_date_id);
                UPDATE aggr_managment SET start_time = NOW() WHERE aggr_name = 'storage_usage' AND date_id = p_date_id;
                
                DELETE FROM kalturadw.dwh_fact_entries_sizes WHERE entry_size_date_id = p_date_id;
                
                DROP TABLE IF EXISTS today_file_sync_subset; 
                
                CREATE TEMPORARY TABLE today_file_sync_subset AS
                SELECT DISTINCT s.id, s.partner_id, IFNULL(a.entry_id, object_id) entry_id, object_id, object_type, object_sub_type, IFNULL(file_size, 0) file_size
                FROM kalturadw.dwh_dim_file_sync s LEFT OUTER JOIN kalturadw.dwh_dim_flavor_asset a
                ON (object_type = 4 AND s.object_id = a.id AND a.entry_id IS NOT NULL AND a.ri_ind =0 AND s.partner_id = a.partner_id)
                WHERE s.ready_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
                AND object_type IN (1,4)
                AND original = 1
                AND s.STATUS IN (2,3)
                AND s.partner_id NOT IN ( -1  , -2  , 0 , 99 );
                
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
                
                INSERT INTO today_sizes
                                SELECT s.partner_id, IFNULL(a.entry_id, object_id) entry_id, object_id, object_type, object_sub_type, 0 file_size
                                FROM kalturadw.dwh_dim_file_sync s LEFT OUTER JOIN kalturadw.dwh_dim_flavor_asset a
                                ON (object_type = 4 AND s.object_id = a.id AND a.entry_id IS NOT NULL AND a.ri_ind =0 AND s.partner_id = a.partner_id)
                                WHERE s.updated_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
                                AND object_type IN (1,4)
                                AND original = 1
                                AND s.STATUS IN (3)
                                AND s.partner_id NOT IN ( -1  , -2  , 0 , 99 )
                ON DUPLICATE KEY UPDATE
                                file_size = 0;       
                
				
                DROP TABLE IF EXISTS deleted_flavors;
                
                CREATE TEMPORARY TABLE deleted_flavors AS 
                SELECT DISTINCT partner_id, entry_id, id
                FROM kalturadw.dwh_dim_flavor_asset FORCE INDEX (deleted_at)
                WHERE STATUS = 3 AND deleted_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
                AND partner_id NOT IN (100  , -1  , -2  , 0 , 99 );
                                
                BEGIN
                                DECLARE v_deleted_flavor_partner_id INT;
                                DECLARE v_deleted_flavor_entry_id VARCHAR(60);
                                DECLARE v_deleted_flavor_id VARCHAR(60);
                                DECLARE done INT DEFAULT 0;
                                DECLARE deleted_flavors_cursor CURSOR FOR 
                                SELECT partner_id, entry_id, id  FROM deleted_flavors;
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
                                                                WHERE object_id = v_deleted_flavor_id AND object_type = 4 AND ready_at < v_date AND file_size > 0
                                                ON DUPLICATE KEY UPDATE
                                                                file_size = VALUES(file_size);
                                END LOOP;
                                CLOSE deleted_flavors_cursor;
                END;
                
                
                
                DROP TABLE IF EXISTS today_deleted_entries;
                CREATE TEMPORARY TABLE today_deleted_entries AS 
                SELECT entry_id, partner_id FROM kalturadw.dwh_dim_entries
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
                AND f.ready_at < v_date
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
                SELECT t.partner_id, t.entry_id, ROUND(SUM(t.file_size - IFNULL(Y.file_size, 0))/1024, 3) entry_additional_size_kb, v_date, p_date_id 
                FROM today_sizes t LEFT OUTER JOIN yesterday_sizes Y 
                ON t.object_id = Y.object_id
                AND t.partner_id = Y.partner_id
                AND t.object_type = Y.object_type
                AND t.object_sub_type = Y.object_sub_type
                AND t.file_size <> Y.file_size
                GROUP BY t.partner_id, t.entry_id
                HAVING entry_additional_size_kb <> 0
                ON DUPLICATE KEY UPDATE 
                                entry_additional_size_kb = VALUES(entry_additional_size_kb);
                
                
                DROP TABLE IF EXISTS deleted_entries;
                CREATE TEMPORARY TABLE deleted_entries AS
                                SELECT es.partner_id partner_id, es.entry_id entry_id, v_date entry_size_date, p_date_id entry_size_date_id, -SUM(entry_additional_size_kb) entry_additional_size_kb
                                FROM today_deleted_entries e, kalturadw.dwh_fact_entries_sizes es
                                WHERE e.entry_id = es.entry_id 
                                                AND e.partner_id = es.partner_id 
                                                AND es.entry_size_date_id < p_date_id
                                GROUP BY es.partner_id, es.entry_id
                                HAVING SUM(entry_additional_size_kb) > 0;
                
                INSERT INTO kalturadw.dwh_fact_entries_sizes (partner_id, entry_id, entry_size_date, entry_size_date_id, entry_additional_size_kb)
                                SELECT partner_id, entry_id, entry_size_date, entry_size_date_id, entry_additional_size_kb FROM deleted_entries
                ON DUPLICATE KEY UPDATE 
                                entry_additional_size_kb = VALUES(entry_additional_size_kb);
                
                CALL kalturadw.calc_aggr_day_partner_storage(v_date);
                UPDATE aggr_managment SET end_time = NOW() WHERE aggr_name = 'storage_usage' AND date_id = p_date_id;
END$$

DELIMITER ;


USE `kalturadw`;

DROP TABLE IF EXISTS `dwh_dim_file_sync_new`;

CREATE TABLE `dwh_dim_file_sync_new` (
  `id` int (11),
	`partner_id` int (11),
	`object_type` int (4),
	`object_id` varchar (60),
	`version` varchar (60),
	`object_sub_type` tinyint (4),
	`dc` varchar (6),
	`original` tinyint (4),
	`created_at` datetime ,
	`updated_at` datetime ,
	`ready_at` datetime ,
	`sync_time` int (11),
	`status` tinyint (4),
	`file_type` tinyint (4),
	`linked_id` int (11),
	`link_count` int (11),
	`file_root` varchar (192),
	`file_path` varchar (384),
	`file_size` bigint (20),  
	`dwh_creation_date` TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
	`dwh_update_date` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
	`ri_ind` TINYINT(4) NOT NULL DEFAULT '0'
) ENGINE=MYISAM; 

INSERT INTO kalturadw.dwh_dim_file_sync_new 
SELECT * FROM kalturadw.dwh_dim_file_sync;

ALTER TABLE kalturadw.dwh_dim_file_sync_new
  ADD PRIMARY KEY (`id`), 
  ADD UNIQUE KEY `unique_key` (`object_type`,`object_id`,`object_sub_type`,`version`,`dc`),
  ADD KEY `updated_at` (`updated_at`),
  ADD KEY `ready_at` (`ready_at`),
  ADD KEY `dwh_update_date` (`dwh_update_date`);
  
DROP TABLE kalturadw.dwh_dim_file_sync;
RENAME TABLE kalturadw.dwh_dim_file_sync_new TO kalturadw.dwh_dim_file_sync;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_partner_billing_data`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `calc_partner_billing_data`(p_date_id INT(11),p_partner_id INT)
BEGIN
	SELECT
        FLOOR(date_id/100) month_id,
        SUM(aggr_storage_mb)/(IF(FLOOR(date_id/100)=FLOOR(LEAST(p_date_id,DATE(NOW())*1)/100),DAY(LEAST(p_date_id,DATE(NOW())*1)),DAY(LAST_DAY(date_id)))) avg_continuous_aggr_storage_mb,
        SUM(count_bandwidth_kb) sum_partner_bandwidth_kb
    FROM dwh_hourly_partner_usage 
    WHERE partner_id=p_partner_id AND hour_id = 0
    AND date_id <= LEAST(p_date_id,DATE(NOW())*1)
    GROUP BY month_id
	WITH ROLLUP;	
END$$

DELIMITER ;	

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
				IFNULL(SUM(aggr_storage_mb),0)/DAY(LAST_DAY(p_month_id*100+1)) actual_storage_mb,
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

/*6135*/
USE kalturadw;

ALTER TABLE kalturadw.dwh_hourly_partner_usage
ADD COLUMN added_storage_mb DECIMAL(19,4) DEFAULT 0 AFTER count_storage_mb,
ADD COLUMN deleted_storage_mb DECIMAL(19,4) DEFAULT 0 AFTER added_storage_mb;

UPDATE kalturadw.dwh_hourly_partner_usage SET added_storage_mb = IF(count_storage_mb > 0, count_storage_mb, 0), deleted_storage_mb = IF(count_storage_mb < 0, count_storage_mb*-1, 0);

ALTER TABLE kalturadw.dwh_hourly_partner_usage
DROP COLUMN count_storage_mb;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_storage`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_partner_storage`(date_val DATE)
BEGIN
    DELETE FROM kalturadw.dwh_hourly_partner_usage WHERE date_id = DATE(date_val)*1 AND IFNULL(count_bandwidth_kb,0) = 0 AND bandwidth_source_id = 1;
    UPDATE kalturadw.dwh_hourly_partner_usage SET added_storage_mb = 0, deleted_storage_mb = 0, aggr_storage_mb=NULL WHERE date_id = DATE(date_val)*1 AND IFNULL(count_bandwidth_kb,0) > 0;
	
	DROP TABLE IF EXISTS temp_aggr_storage;
	CREATE TEMPORARY TABLE temp_aggr_storage(
		partner_id      	INT(11) NOT NULL,
		date_id     		INT(11) NOT NULL,
		hour_id	 		TINYINT(4) NOT NULL,
		added_storage_mb	DECIMAL(19,4) NOT NULL,
		deleted_storage_mb      DECIMAL(19,4) NOT NULL
	) ENGINE = MEMORY;
      
	INSERT INTO 	temp_aggr_storage (partner_id, date_id, hour_id, added_storage_mb, deleted_storage_mb)
   	SELECT 		partner_id, MAX(entry_size_date_id), 0 hour_id, SUM(IF(entry_additional_size_kb>0,entry_additional_size_kb,0))/1024 added_storage_mb, SUM(IF(entry_additional_size_kb<0,entry_additional_size_kb*-1,0))/1024 deleted_storage_mb 
	FROM 		dwh_fact_entries_sizes
	WHERE		entry_size_date_id=DATE(date_val)*1
	GROUP BY 	partner_id;
	
	INSERT INTO 	kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, added_storage_mb, deleted_storage_mb, aggr_storage_mb)
	SELECT		partner_id, DATE(date_val)*1, 0 hour_id, 1, 0, 0, aggr_storage_mb
	FROM        kalturadw.dwh_hourly_partner_usage
	WHERE       date_id = DATE(date_val - INTERVAL 1 DAY)*1
	ON DUPLICATE KEY UPDATE added_storage_mb=VALUES(added_storage_mb), deleted_storage_mb=VALUES(deleted_storage_mb), aggr_storage_mb = VALUES(aggr_storage_mb);
	
	INSERT INTO 	kalturadw.dwh_hourly_partner_usage (partner_id, date_id, hour_id, bandwidth_source_id, added_storage_mb, deleted_storage_mb, aggr_storage_mb)
	SELECT		aggr.partner_id, aggr.date_id, aggr.hour_id, 1, aggr.added_storage_mb, aggr.deleted_storage_mb, aggr.added_storage_mb - aggr.deleted_storage_mb
	FROM		temp_aggr_storage aggr 
	ON DUPLICATE KEY UPDATE added_storage_mb=VALUES(added_storage_mb), deleted_storage_mb=VALUES(deleted_storage_mb), aggr_storage_mb=IFNULL(aggr_storage_mb,0) + VALUES(aggr_storage_mb) ;
	
END$$

DELIMITER ;













	








