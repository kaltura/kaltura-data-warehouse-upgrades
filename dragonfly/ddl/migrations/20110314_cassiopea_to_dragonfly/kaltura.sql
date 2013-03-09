use kaltura;

create index updated_at_index
on kaltura.media_info(updated_at);

create index updated_at_index
on kaltura.flavor_params_conversion_profile(updated_at);

create index updated_at_index
on kaltura.conversion_profile_2(updated_at);

create index updated_at_index
on kaltura.flavor_params_output(updated_at);

create index updated_at_index
on kaltura.file_sync(updated_at);

create index updated_at_index
on kaltura.flavor_asset(updated_at);

create index updated_at_index
on kaltura.flavor_params(updated_at);

DELIMITER $$

DROP PROCEDURE IF EXISTS `update_kusers`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `update_kusers`()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE new_kuser_id CHAR(50);
    DECLARE new_storage_size INT;
    DECLARE updated_kusers CURSOR FOR SELECT kuser_id, storage_kb FROM kaltura.temp_updated_kusers_storage_usage;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    OPEN updated_kusers;
    
    SET SESSION sql_log_bin = 1;
    REPEAT
    FETCH updated_kusers INTO new_kuser_id, new_storage_size;
    UPDATE kuser SET kuser.storage_size = new_storage_size WHERE kuser.id = new_kuser_id;
    UNTIL done END REPEAT;
    SET SESSION sql_log_bin = 0;
    CLOSE updated_kusers;
    END$$

DELIMITER ;

USE `kaltura`;

DROP TABLE IF EXISTS `temp_updated_kusers_storage_usage`;

CREATE TABLE `temp_updated_kusers_storage_usage` (
  `kuser_id` INT(11) NOT NULL,
  `storage_kb` INT(11)
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

DELIMITER $$

DROP PROCEDURE IF EXISTS `update_entries`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `update_entries`()
BEGIN
    DECLARE done INT DEFAULT 0;
    DECLARE entry_id CHAR(50);
    DECLARE new_views, new_plays INT;
    DECLARE updated_entries CURSOR FOR SELECT id, plays, views FROM temp_entry_update;
    DECLARE CONTINUE HANDLER FOR NOT FOUND SET done = 1;
    OPEN updated_entries;
    
    SET SESSION sql_log_bin = 1;
    REPEAT
    FETCH updated_entries INTO entry_id, new_plays, new_views;
    UPDATE entry SET entry.plays = new_plays, entry.views = new_views WHERE entry.id = entry_id;
    UNTIL done END REPEAT;
    SET SESSION sql_log_bin = 0;
    CLOSE updated_entries;
    END$$

DELIMITER ;