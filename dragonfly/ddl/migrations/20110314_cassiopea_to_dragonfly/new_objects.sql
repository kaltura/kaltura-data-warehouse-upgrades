use kalturadw;

DROP TABLE IF EXISTS `dwh_daily_usage_reports`;

CREATE TABLE `dwh_daily_usage_reports` (
  `measure` varchar(50) DEFAULT NULL,
  `classification` varchar(50) DEFAULT NULL,
  `date` date DEFAULT NULL,
  `yesterday` decimal(15,2) DEFAULT NULL,
  `the_day_before` decimal(15,2) DEFAULT NULL,
  `diff` decimal(15,2) DEFAULT NULL,
  `last_5_days_avg` decimal(15,2) DEFAULT NULL,
  `last_30_days_avg` decimal(15,2) DEFAULT NULL,
  `outer_order` int(11) DEFAULT NULL,
  `inner_order` int(11) DEFAULT NULL,
  UNIQUE KEY `m_c_d_key` (`measure`,`classification`,`date`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_asset_status` */

DROP TABLE IF EXISTS `dwh_dim_asset_status`;

CREATE TABLE `dwh_dim_asset_status` (
  `asset_status_id` smallint(6) NOT NULL,
  `asset_status_name` varchar(50) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`asset_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_audio_codec` */

DROP TABLE IF EXISTS `dwh_dim_audio_codec`;

CREATE TABLE `dwh_dim_audio_codec` (
  `audio_codec_id` int(11) NOT NULL AUTO_INCREMENT,
  `audio_codec` varchar(381) DEFAULT NULL,
  PRIMARY KEY (`audio_codec_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_bandwidth_source` */

DROP TABLE IF EXISTS `dwh_dim_bandwidth_source`;

CREATE TABLE `dwh_dim_bandwidth_source` (
  `bandwidth_source_id` int(11) NOT NULL DEFAULT '0',
  `bandwidth_source_name` varchar(50) DEFAULT NULL,
  `dwh_creation_date` datetime DEFAULT NULL,
  `dwh_update_date` datetime DEFAULT NULL,
  `ri_ind` tinyint(4) DEFAULT '0',
  PRIMARY KEY (`bandwidth_source_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_container_format` */

DROP TABLE IF EXISTS `dwh_dim_container_format`;

CREATE TABLE `dwh_dim_container_format` (
  `container_format_id` int(11) NOT NULL AUTO_INCREMENT,
  `container_format` varchar(381) DEFAULT NULL,
  PRIMARY KEY (`container_format_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_conversion_profile` */

DROP TABLE IF EXISTS `dwh_dim_conversion_profile`;

CREATE TABLE `dwh_dim_conversion_profile` (
  `id` int(11) NOT NULL DEFAULT '0',
  `partner_id` int(11) DEFAULT NULL,
  `name` varchar(384) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `description` varchar(3072) DEFAULT NULL,
  `clip_start` int(11) DEFAULT NULL,
  `clip_duration` int(11) DEFAULT NULL,
  `input_tags_map` varchar(3069) DEFAULT NULL,
  `creation_mode` smallint(6) DEFAULT NULL,
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_creation_mode` */

DROP TABLE IF EXISTS `dwh_dim_creation_mode`;

CREATE TABLE `dwh_dim_creation_mode` (
  `creation_mode_id` smallint(6) NOT NULL,
  `creation_mode_name` varchar(50) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`creation_mode_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_file_ext` */

DROP TABLE IF EXISTS `dwh_dim_file_ext`;

CREATE TABLE `dwh_dim_file_ext` (
  `file_ext_id` int(11) NOT NULL AUTO_INCREMENT,
  `file_ext` varchar(11) DEFAULT NULL,
  PRIMARY KEY (`file_ext_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_file_sync` */

DROP TABLE IF EXISTS `dwh_dim_file_sync`;

CREATE TABLE `dwh_dim_file_sync` (
  `id` int(11) NOT NULL DEFAULT '0',
  `partner_id` int(11) DEFAULT NULL,
  `object_type` int(4) DEFAULT NULL,
  `object_id` varchar(60) DEFAULT NULL,
  `version` varchar(60) DEFAULT NULL,
  `object_sub_type` tinyint(4) DEFAULT NULL,
  `dc` varchar(6) DEFAULT NULL,
  `original` tinyint(4) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `ready_at` datetime DEFAULT NULL,
  `sync_time` int(11) DEFAULT NULL,
  `status` tinyint(4) DEFAULT NULL,
  `file_type` tinyint(4) DEFAULT NULL,
  `linked_id` int(11) DEFAULT NULL,
  `link_count` int(11) DEFAULT NULL,
  `file_root` varchar(192) DEFAULT NULL,
  `file_path` varchar(384) DEFAULT NULL,
  `file_size` bigint(20) DEFAULT NULL,
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_key` (`object_type`,`object_id`,`object_sub_type`,`version`,`dc`),
  KEY `updated_at` (`updated_at`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_file_sync_object_type` */

DROP TABLE IF EXISTS `dwh_dim_file_sync_object_type`;

CREATE TABLE `dwh_dim_file_sync_object_type` (
  `file_sync_object_type_id` smallint(6) NOT NULL,
  `file_sync_object_type_name` varchar(50) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`file_sync_object_type_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_file_sync_status` */

DROP TABLE IF EXISTS `dwh_dim_file_sync_status`;

CREATE TABLE `dwh_dim_file_sync_status` (
  `file_sync_status_id` smallint(6) NOT NULL,
  `file_sync_status_name` varchar(50) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`file_sync_status_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_flavor_asset` */

DROP TABLE IF EXISTS `dwh_dim_flavor_asset`;

CREATE TABLE `dwh_dim_flavor_asset` (
  `dwh_id` int(11) NOT NULL AUTO_INCREMENT,
  `id` varchar(60) NOT NULL DEFAULT '',
  `int_id` int(11) DEFAULT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `tags` blob,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `entry_id` varchar(60) DEFAULT NULL,
  `flavor_params_id` int(11) DEFAULT NULL,
  `status` tinyint(4) DEFAULT NULL,
  `version` varchar(60) NOT NULL,
  `description` varchar(765) DEFAULT NULL,
  `width` int(11) DEFAULT NULL,
  `height` int(11) DEFAULT NULL,
  `bitrate` int(11) DEFAULT NULL,
  `frame_rate` float DEFAULT NULL,
  `size` int(11) DEFAULT NULL,
  `is_original` int(11) DEFAULT NULL,
  `file_ext_id` int(11) DEFAULT NULL,
  `container_format_id` int(11) DEFAULT NULL,
  `video_codec_id` int(11) DEFAULT NULL,
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`dwh_id`),
  UNIQUE KEY `id_version` (`id`,`version`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_flavor_format` */

DROP TABLE IF EXISTS `dwh_dim_flavor_format`;

CREATE TABLE `dwh_dim_flavor_format` (
  `flavor_format_id` int(11) NOT NULL AUTO_INCREMENT,
  `flavor_format` varchar(60) DEFAULT NULL,
  PRIMARY KEY (`flavor_format_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_flavor_params` */

DROP TABLE IF EXISTS `dwh_dim_flavor_params`;

CREATE TABLE `dwh_dim_flavor_params` (
  `dwh_id` int(11) NOT NULL AUTO_INCREMENT,
  `id` int(11) NOT NULL,
  `version` int(11) NOT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `name` varchar(384) DEFAULT NULL,
  `tags` blob,
  `description` varchar(3072) DEFAULT NULL,
  `ready_behavior` tinyint(4) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `is_default` tinyint(4) DEFAULT NULL,
  `flavor_format_id` int(11) DEFAULT NULL,
  `video_codec_id` int(11) DEFAULT NULL,
  `video_bitrate` int(11) DEFAULT NULL,
  `audio_codec_id` int(11) DEFAULT NULL,
  `audio_bitrate` int(11) DEFAULT NULL,
  `audio_channels` tinyint(4) DEFAULT NULL,
  `audio_sample_rate` int(11) DEFAULT NULL,
  `audio_resolution` int(11) DEFAULT NULL,
  `width` int(11) DEFAULT NULL,
  `height` int(11) DEFAULT NULL,
  `frame_rate` float DEFAULT NULL,
  `gop_size` int(11) DEFAULT NULL,
  `two_pass` int(11) DEFAULT NULL,
  `conversion_engines` varchar(3072) DEFAULT NULL,
  `conversion_engines_extra_params` varchar(3072) DEFAULT NULL,
  `view_order` int(11) DEFAULT NULL,
  `bypass_by_extension` varchar(96) DEFAULT NULL,
  `creation_mode` smallint(6) DEFAULT NULL,
  `deinterlice` int(11) DEFAULT NULL,
  `rotate` int(11) DEFAULT NULL,
  `operators` blob,
  `engine_version` smallint(6) DEFAULT NULL,
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`dwh_id`),
  UNIQUE KEY `id_version` (`id`,`version`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_flavor_params_conversion_profile` */

DROP TABLE IF EXISTS `dwh_dim_flavor_params_conversion_profile`;

CREATE TABLE `dwh_dim_flavor_params_conversion_profile` (
  `id` int(11) NOT NULL DEFAULT '0',
  `flavor_params_id` int(11) DEFAULT NULL,
  `conversion_profile_id` int(11) DEFAULT NULL,
  `ready_behavior` tinyint(4) DEFAULT NULL,
  `force_none_complied` int(11) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_flavor_params_output` */

DROP TABLE IF EXISTS `dwh_dim_flavor_params_output`;

CREATE TABLE `dwh_dim_flavor_params_output` (
  `id` int(11) NOT NULL,
  `flavor_params_id` int(11) DEFAULT NULL,
  `flavor_params_version` int(11) DEFAULT NULL,
  `partner_id` int(11) DEFAULT NULL,
  `entry_id` varchar(60) DEFAULT NULL,
  `flavor_asset_id` varchar(60) DEFAULT NULL,
  `flavor_asset_version` varchar(60) DEFAULT NULL,
  `name` varchar(384) DEFAULT NULL,
  `tags` blob,
  `description` varchar(3072) DEFAULT NULL,
  `ready_behavior` tinyint(4) DEFAULT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `deleted_at` datetime DEFAULT NULL,
  `is_default` tinyint(4) DEFAULT NULL,
  `flavor_format_id` int(11) DEFAULT NULL,
  `video_codec_id` int(11) DEFAULT NULL,
  `video_bitrate` int(11) DEFAULT NULL,
  `audio_codec_id` int(11) DEFAULT NULL,
  `audio_bitrate` int(11) DEFAULT NULL,
  `audio_channels` tinyint(4) DEFAULT NULL,
  `audio_sample_rate` int(11) DEFAULT NULL,
  `audio_resolution` int(11) DEFAULT NULL,
  `width` int(11) DEFAULT NULL,
  `height` int(11) DEFAULT NULL,
  `frame_rate` float DEFAULT NULL,
  `gop_size` int(11) DEFAULT NULL,
  `two_pass` int(11) DEFAULT NULL,
  `conversion_engines` varchar(3072) DEFAULT NULL,
  `conversion_engines_extra_params` varchar(3072) DEFAULT NULL,
  `custom_data` blob,
  `command_lines` varchar(6141) DEFAULT NULL,
  `file_ext` varchar(12) DEFAULT NULL,
  `deinterlice` int(11) DEFAULT NULL,
  `rotate` int(11) DEFAULT NULL,
  `operators` blob,
  `engine_version` smallint(6) DEFAULT NULL,
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_media_info` */

DROP TABLE IF EXISTS `dwh_dim_media_info`;

CREATE TABLE `dwh_dim_media_info` (
  `id` int(11) NOT NULL,
  `created_at` datetime DEFAULT NULL,
  `updated_at` datetime DEFAULT NULL,
  `flavor_asset_id` varchar(60) DEFAULT NULL,
  `file_size` int(11) DEFAULT NULL,
  `container_format_id` int(11) DEFAULT NULL,
  `container_id` varchar(381) DEFAULT NULL,
  `container_profile` varchar(381) DEFAULT NULL,
  `container_duration` int(11) DEFAULT NULL,
  `container_bit_rate` int(11) DEFAULT NULL,
  `video_format_id` int(11) DEFAULT NULL,
  `video_codec_id` int(11) DEFAULT NULL,
  `video_duration` int(11) DEFAULT NULL,
  `video_bit_rate` int(11) DEFAULT NULL,
  `video_bit_rate_mode` tinyint(4) DEFAULT NULL,
  `video_width` int(11) DEFAULT NULL,
  `video_height` int(11) DEFAULT NULL,
  `video_frame_rate` float DEFAULT NULL,
  `video_dar` float DEFAULT NULL,
  `video_rotation` int(11) DEFAULT NULL,
  `audio_format_id` int(11) DEFAULT NULL,
  `audio_codec_id` int(11) DEFAULT NULL,
  `audio_duration` int(11) DEFAULT NULL,
  `audio_bit_rate` int(11) DEFAULT NULL,
  `audio_bit_rate_mode` tinyint(4) DEFAULT NULL,
  `audio_channels` tinyint(4) DEFAULT NULL,
  `audio_sampling_rate` int(11) DEFAULT NULL,
  `audio_resolution` int(11) DEFAULT NULL,
  `writing_lib` varchar(381) DEFAULT NULL,
  `custom_data` blob,
  `raw_data` blob,
  `multi_stream_info` varchar(3069) DEFAULT NULL,
  `flavor_asset_version` varchar(60) DEFAULT NULL,
  `scan_type` int(11) DEFAULT NULL,
  `multi_stream` varchar(765) DEFAULT NULL,
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


/*Table structure for table `dwh_dim_ready_behavior` */

DROP TABLE IF EXISTS `dwh_dim_ready_behavior`;

CREATE TABLE `dwh_dim_ready_behavior` (
  `ready_behavior_id` smallint(6) NOT NULL,
  `ready_behavior_name` varchar(50) DEFAULT 'missing value',
  `dwh_creation_date` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00',
  `dwh_update_date` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `ri_ind` tinyint(4) NOT NULL DEFAULT '0',
  PRIMARY KEY (`ready_behavior_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_referrer` */

DROP TABLE IF EXISTS `dwh_dim_referrer`;

CREATE TABLE `dwh_dim_referrer` (
  `referrer_id` int(11) NOT NULL AUTO_INCREMENT,
  `referrer` varchar(255) DEFAULT NULL,
  `dwh_insertion_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`referrer_id`),
  UNIQUE KEY `referrer` (`referrer`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;


INSERT IGNORE INTO kalturadw.dwh_dim_referrer(referrer)
SELECT DISTINCT IFNULL(referrer,'')
FROM kalturadw.dwh_fact_events;

/*Table structure for table `dwh_dim_user_agent` */

DROP TABLE IF EXISTS `dwh_dim_user_agent`;

CREATE TABLE `dwh_dim_user_agent` (
  `user_agent_id` int(11) NOT NULL AUTO_INCREMENT,
  `user_agent` varchar(4096) DEFAULT NULL,
  PRIMARY KEY (`user_agent_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*Table structure for table `dwh_dim_video_codec` */

DROP TABLE IF EXISTS `dwh_dim_video_codec`;

CREATE TABLE `dwh_dim_video_codec` (
  `video_codec_id` int(11) NOT NULL AUTO_INCREMENT,
  `video_codec` varchar(381) DEFAULT NULL,
  PRIMARY KEY (`video_codec_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

/*
SQLyog Community v8.7 
MySQL - 5.1.37-log : Database - kalturadw
*********************************************************************
*/


/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
USE `kalturadw`;

/*Table structure for table `dwh_fact_bandwidth_usage` */

DROP TABLE IF EXISTS `dwh_fact_bandwidth_usage`;

CREATE TABLE `dwh_fact_bandwidth_usage` (
  `file_id` INT(11) NOT NULL,
  `partner_id` INT(11) NOT NULL DEFAULT '-1',
  `activity_date_id` INT(11) DEFAULT '-1',
  `activity_hour_id` TINYINT(4) DEFAULT '-1',
  `bandwidth_source_id` BIGINT(20) DEFAULT NULL,
  `url` VARCHAR(2000) DEFAULT NULL,
  `bandwidth_bytes` BIGINT(20) DEFAULT '0',
  KEY `partner_id` (`partner_id`),
  KEY `file_id` (`file_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (activity_date_id)
(PARTITION p_20080531 VALUES LESS THAN (20080601) ENGINE = MyISAM,
PARTITION p_20080630 VALUES LESS THAN (20080701) ENGINE = MyISAM,
PARTITION p_20080731 VALUES LESS THAN (20080801) ENGINE = MyISAM,
PARTITION p_20080831 VALUES LESS THAN (20080901) ENGINE = MyISAM,
PARTITION p_20080930 VALUES LESS THAN (20081001) ENGINE = MyISAM,
PARTITION p_20081031 VALUES LESS THAN (20081101) ENGINE = MyISAM,
PARTITION p_20081130 VALUES LESS THAN (20081201) ENGINE = MyISAM,
PARTITION p_20081231 VALUES LESS THAN (20090101) ENGINE = MyISAM,
PARTITION p_20090131 VALUES LESS THAN (20090201) ENGINE = MyISAM,
PARTITION p_20090228 VALUES LESS THAN (20090301) ENGINE = MyISAM,
PARTITION p_20090331 VALUES LESS THAN (20090401) ENGINE = MyISAM,
PARTITION p_20090430 VALUES LESS THAN (20090501) ENGINE = MyISAM,
PARTITION p_20090531 VALUES LESS THAN (20090601) ENGINE = MyISAM,
PARTITION p_20090630 VALUES LESS THAN (20090701) ENGINE = MyISAM,
PARTITION p_20090731 VALUES LESS THAN (20090801) ENGINE = MyISAM,
PARTITION p_20090831 VALUES LESS THAN (20090901) ENGINE = MyISAM,
PARTITION p_20090930 VALUES LESS THAN (20091001) ENGINE = MyISAM,
PARTITION p_20091031 VALUES LESS THAN (20091101) ENGINE = MyISAM,
PARTITION p_20091130 VALUES LESS THAN (20091201) ENGINE = MyISAM,
PARTITION p_20091231 VALUES LESS THAN (20100101) ENGINE = MyISAM,
PARTITION p_20100131 VALUES LESS THAN (20100201) ENGINE = MyISAM,
PARTITION p_20100228 VALUES LESS THAN (20100301) ENGINE = MyISAM,
PARTITION p_20100331 VALUES LESS THAN (20100401) ENGINE = MyISAM,
PARTITION p_20100430 VALUES LESS THAN (20100501) ENGINE = MyISAM,
PARTITION p_20100531 VALUES LESS THAN (20100601) ENGINE = MyISAM,
PARTITION p_20100630 VALUES LESS THAN (20100701) ENGINE = MyISAM,
PARTITION p_20100731 VALUES LESS THAN (20100801) ENGINE = MyISAM,
PARTITION p_20100831 VALUES LESS THAN (20100901) ENGINE = MyISAM,
PARTITION p_20100930 VALUES LESS THAN (20101001) ENGINE = MyISAM,
PARTITION p_20101031 VALUES LESS THAN (20101101) ENGINE = MyISAM,
PARTITION p_20101130 VALUES LESS THAN (20101201) ENGINE = MyISAM,
PARTITION p_20101231 VALUES LESS THAN (20110101) ENGINE = MyISAM) */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;


/*Table structure for table `dwh_fact_entries_sizes` */

DROP TABLE IF EXISTS `dwh_fact_entries_sizes`;

CREATE TABLE `dwh_fact_entries_sizes` (
  `partner_id` int(11) NOT NULL,
  `entry_id` varchar(20) NOT NULL,
  `entry_size_date` datetime NOT NULL,
  `entry_size_date_id` int(11) NOT NULL,
  `entry_additional_size_kb` decimal(15,3) NOT NULL,
  PRIMARY KEY (`entry_size_date_id`, `partner_id`,`entry_id`),
  KEY entry_id (`entry_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (entry_size_date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

/*Table structure for table `dwh_hourly_events_country` */

DROP TABLE IF EXISTS `dwh_hourly_events_country`;

CREATE TABLE `dwh_hourly_events_country` (
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
  KEY `country_id` (`country_id`,`partner_id`,`date_id`,`hour_id`,`location_id`),
  KEY `date_id` (`date_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

/*Table structure for table `dwh_hourly_events_domain` */

DROP TABLE IF EXISTS `dwh_hourly_events_domain`;

CREATE TABLE `dwh_hourly_events_domain` (
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
  KEY `domain_id` (`domain_id`,`partner_id`,`date_id`,`hour_id`),
  KEY `date_id` (`date_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

/*Table structure for table `dwh_hourly_events_domain_referrer` */

DROP TABLE IF EXISTS `dwh_hourly_events_domain_referrer`;

CREATE TABLE `dwh_hourly_events_domain_referrer` (
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
  PRIMARY KEY (`partner_id`, `domain_id`, `date_id`, `hour_id`, `referrer_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

 /*Table structure for table `dwh_hourly_events_entry` */

DROP TABLE IF EXISTS `dwh_hourly_events_entry`;

CREATE TABLE `dwh_hourly_events_entry` (
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
  KEY `entry_id` (`entry_id`,`partner_id`,`date_id`,`hour_id`),
  KEY `date_id` (`date_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

/*Table structure for table `dwh_hourly_events_uid` */

DROP TABLE IF EXISTS `dwh_hourly_events_uid`;

CREATE TABLE `dwh_hourly_events_uid` (
  `partner_id` int(11) NOT NULL DEFAULT '0',
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` int(11) NOT NULL DEFAULT '0',
  `kuser_id` varchar(64) NOT NULL DEFAULT '0',
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
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`kuser_id`),
  KEY `uid` (`kuser_id`,`partner_id`,`date_id`,`hour_id`),
  KEY `date_id` (`date_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

/*Table structure for table `dwh_hourly_events_widget` */

DROP TABLE IF EXISTS `dwh_hourly_events_widget`;

CREATE TABLE `dwh_hourly_events_widget` (
  `partner_id` int(11) NOT NULL DEFAULT '0',
  `date_id` int(11) NOT NULL DEFAULT '0',
  `hour_id` int(11) NOT NULL DEFAULT '0',
  `widget_id` varchar(32) NOT NULL DEFAULT '',
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
  `count_widget_loads` int(11) DEFAULT NULL,
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
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`,`widget_id`),
  KEY `widget_id` (`widget_id`,`partner_id`,`date_id`,`hour_id`),
  KEY `date_id` (`date_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

/*Table structure for table `dwh_hourly_partner` */

DROP TABLE IF EXISTS `dwh_hourly_partner`;

CREATE TABLE `dwh_hourly_partner` (
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
  `count_media` int(11) DEFAULT NULL,
  `count_video` int(11) DEFAULT NULL,
  `count_image` int(11) DEFAULT NULL,
  `count_audio` int(11) DEFAULT NULL,
  `count_mix` int(11) DEFAULT NULL,
  `count_mix_non_empty` int(11) DEFAULT NULL,
  `count_playlist` int(11) DEFAULT NULL,
  `count_bandwidth` bigint(20) DEFAULT NULL,
  `count_storage` bigint(20) DEFAULT NULL,
  `count_users` int(11) DEFAULT NULL,
  `count_widgets` int(11) DEFAULT NULL,
  `flag_active_site` tinyint(4) DEFAULT '0',
  `flag_active_publisher` tinyint(4) DEFAULT '0',
  `aggr_storage` bigint(20) DEFAULT NULL,
  `aggr_bandwidth` bigint(20) DEFAULT NULL,
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
  `count_streaming` bigint(20) DEFAULT '0',
  `aggr_streaming` bigint(20) DEFAULT '0',
  PRIMARY KEY (`partner_id`,`date_id`,`hour_id`),
  KEY `date_id` (`date_id`)
) ENGINE=MyISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY RANGE (date_id)
(PARTITION p_201001 VALUES LESS THAN (20100201) ENGINE = MyISAM,
 PARTITION p_201002 VALUES LESS THAN (20100301) ENGINE = MyISAM,
 PARTITION p_201003 VALUES LESS THAN (20100401) ENGINE = MyISAM,
 PARTITION p_201004 VALUES LESS THAN (20100501) ENGINE = MyISAM,
 PARTITION p_201005 VALUES LESS THAN (20100601) ENGINE = MyISAM,
 PARTITION p_201006 VALUES LESS THAN (20100701) ENGINE = MyISAM,
 PARTITION p_201007 VALUES LESS THAN (20100801) ENGINE = MyISAM,
 PARTITION p_201008 VALUES LESS THAN (20100901) ENGINE = MyISAM,
 PARTITION p_201009 VALUES LESS THAN (20101001) ENGINE = MyISAM,
 PARTITION p_201010 VALUES LESS THAN (20101101) ENGINE = MyISAM,
 PARTITION p_201011 VALUES LESS THAN (20101201) ENGINE = MyISAM,
 PARTITION p_201012 VALUES LESS THAN (20110101) ENGINE = MyISAM,
 PARTITION p_201101 VALUES LESS THAN (20110201) ENGINE = MyISAM,
 PARTITION p_201102 VALUES LESS THAN (20110301) ENGINE = MyISAM,
 PARTITION p_201103 VALUES LESS THAN (20110401) ENGINE = MyISAM,
 PARTITION p_201104 VALUES LESS THAN (20110501) ENGINE = MyISAM) */;

/* Function  structure for function  `calc_time_shift` */

DROP FUNCTION IF EXISTS `calc_time_shift`;
DELIMITER $$

CREATE DEFINER=`etl`@`localhost` FUNCTION `calc_time_shift`(date_id INT, hour_id INT, time_shift INT) RETURNS int(11)
    NO SQL
BEGIN
	RETURN DATE_FORMAT((date_id + INTERVAL hour_id HOUR + INTERVAL time_shift HOUR), '%Y%m%d')*1;
    END $$

DROP PROCEDURE IF EXISTS `add_daily_partition_for_table`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `add_daily_partition_for_table`(table_name VARCHAR(40))
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
END$$

DELIMITER ;

/* Procedure structure for procedure `add_partitions` */

DROP PROCEDURE IF EXISTS  `add_partitions` ;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `add_partitions`()
BEGIN
	CALL add_partition_for_fact_table('dwh_fact_events');
	CALL add_partition_for_fact_table('dwh_fact_fms_session_events');
	CALL add_partition_for_fact_table('dwh_fact_fms_sessions');
	CALL add_daily_partition_for_table('dwh_fact_bandwidth_usage');
	CALL add_partition_for_table('dwh_fact_entries_sizes');
	CALL add_partition_for_table('dwh_hourly_events_entry');
	CALL add_partition_for_table('dwh_hourly_events_domain');
	CALL add_partition_for_table('dwh_hourly_events_country');
	CALL add_partition_for_table('dwh_hourly_events_widget');
	CALL add_partition_for_table('dwh_hourly_events_uid');	
	CALL add_partition_for_table('dwh_hourly_events_domain_referrer');	
	CALL add_partition_for_table('dwh_hourly_partner');		
END$$
DELIMITER ;


CALL add_partitions();
/* Procedure structure for procedure `calc_aggr_day` */

/* insert data to hourly aggr */
truncate dwh_hourly_events_country;

INSERT INTO dwh_hourly_events_country
SELECT 	partner_id, 
	date_id, 
    0 hour_id,
	country_id, 
	location_id, 
	sum_time_viewed, 
	count_time_viewed, 
	count_plays, 
	count_loads, 
	count_plays_25, 
	count_plays_50, 
	count_plays_75, 
	count_plays_100, 
	count_edit, 
	count_viral, 
	count_download, 
	count_report, 
	count_buf_start, 
	count_buf_end, 
	count_open_full_screen, 
	count_close_full_screen, 
	count_replay, 
	count_seek, 
	count_open_upload, 
	count_save_publish, 
	count_close_editor, 
	count_pre_bumper_played, 
	count_post_bumper_played, 
	count_bumper_clicked, 
	count_preroll_started, 
	count_midroll_started, 
	count_postroll_started, 
	count_overlay_started, 
	count_preroll_clicked, 
	count_midroll_clicked, 
	count_postroll_clicked, 
	count_overlay_clicked, 
	count_preroll_25, 
	count_preroll_50, 
	count_preroll_75, 
	count_midroll_25, 
	count_midroll_50, 
	count_midroll_75, 
	count_postroll_25, 
	count_postroll_50, 
	count_postroll_75
    FROM 
	kalturadw.dwh_aggr_events_country
    WHERE 
    date_id IN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment 
    WHERE is_calculated = 1
    AND aggr_name = 'country')
    ;
    
truncate dwh_hourly_events_domain;
INSERT INTO dwh_hourly_events_domain 
SELECT 	partner_id, 
	date_id, 
	0 hour_id,
    domain_id, 
	sum_time_viewed, 
	count_time_viewed, 
	count_plays, 
	count_loads, 
	count_plays_25, 
	count_plays_50, 
	count_plays_75, 
	count_plays_100, 
	count_edit, 
	count_viral, 
	count_download, 
	count_report, 
	count_buf_start, 
	count_buf_end, 
	count_open_full_screen, 
	count_close_full_screen, 
	count_replay, 
	count_seek, 
	count_open_upload, 
	count_save_publish, 
	count_close_editor, 
	count_pre_bumper_played, 
	count_post_bumper_played, 
	count_bumper_clicked, 
	count_preroll_started, 
	count_midroll_started, 
	count_postroll_started, 
	count_overlay_started, 
	count_preroll_clicked, 
	count_midroll_clicked, 
	count_postroll_clicked, 
	count_overlay_clicked, 
	count_preroll_25, 
	count_preroll_50, 
	count_preroll_75, 
	count_midroll_25, 
	count_midroll_50, 
	count_midroll_75, 
	count_postroll_25, 
	count_postroll_50, 
	count_postroll_75 
	FROM 
	kalturadw.dwh_aggr_events_domain
    WHERE 
    date_id IN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment 
    WHERE is_calculated = 1
    AND aggr_name = 'domain');
    
    
truncate dwh_hourly_events_entry;
INSERT INTO dwh_hourly_events_entry 
SELECT 	partner_id, 
	date_id, 
	0 hour_id,
    entry_id, 
	sum_time_viewed, 
	count_time_viewed, 
	count_plays, 
	count_loads, 
	count_plays_25, 
	count_plays_50, 
	count_plays_75, 
	count_plays_100, 
	count_edit, 
	count_viral, 
	count_download, 
	count_report, 
	count_buf_start, 
	count_buf_end, 
	count_open_full_screen, 
	count_close_full_screen, 
	count_replay, 
	count_seek, 
	count_open_upload, 
	count_save_publish, 
	count_close_editor, 
	count_pre_bumper_played, 
	count_post_bumper_played, 
	count_bumper_clicked, 
	count_preroll_started, 
	count_midroll_started, 
	count_postroll_started, 
	count_overlay_started, 
	count_preroll_clicked, 
	count_midroll_clicked, 
	count_postroll_clicked, 
	count_overlay_clicked, 
	count_preroll_25, 
	count_preroll_50, 
	count_preroll_75, 
	count_midroll_25, 
	count_midroll_50, 
	count_midroll_75, 
	count_postroll_25, 
	count_postroll_50, 
	count_postroll_75	 
	FROM 
	kalturadw.dwh_aggr_events_entry 
     WHERE 
    date_id IN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment 
    WHERE is_calculated = 1
    AND aggr_name = 'entry')
    ;

truncate dwh_hourly_events_uid;    
INSERT INTO dwh_hourly_events_uid 
SELECT 	partner_id, 
	date_id, 
	0 hour_id,
    kuser_id, 
	sum_time_viewed, 
	count_time_viewed, 
	count_plays, 
	count_loads, 
	count_plays_25, 
	count_plays_50, 
	count_plays_75, 
	count_plays_100, 
	count_edit, 
	count_viral, 
	count_download, 
	count_report, 
	count_buf_start, 
	count_buf_end, 
	count_open_full_screen, 
	count_close_full_screen, 
	count_replay, 
	count_seek, 
	count_open_upload, 
	count_save_publish, 
	count_close_editor, 
	count_pre_bumper_played, 
	count_post_bumper_played, 
	count_bumper_clicked, 
	count_preroll_started, 
	count_midroll_started, 
	count_postroll_started, 
	count_overlay_started, 
	count_preroll_clicked, 
	count_midroll_clicked, 
	count_postroll_clicked, 
	count_overlay_clicked, 
	count_preroll_25, 
	count_preroll_50, 
	count_preroll_75, 
	count_midroll_25, 
	count_midroll_50, 
	count_midroll_75, 
	count_postroll_25, 
	count_postroll_50, 
	count_postroll_75	 
	FROM 
	kalturadw.dwh_aggr_events_uid
  WHERE 
    date_id IN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment 
    WHERE is_calculated = 1
    AND aggr_name = 'uid')
    ;
    
truncate dwh_hourly_events_widget;
INSERT INTO dwh_hourly_events_widget 
SELECT 	partner_id, 
	date_id, 
    0 hour_id,
	widget_id, 
	sum_time_viewed, 
	count_time_viewed, 
	count_plays, 
	count_loads, 
	count_plays_25, 
	count_plays_50, 
	count_plays_75, 
	count_plays_100, 
	count_edit, 
	count_viral, 
	count_download, 
	count_report, 
	count_widget_loads, 
	count_buf_start, 
	count_buf_end, 
	count_open_full_screen, 
	count_close_full_screen, 
	count_replay, 
	count_seek, 
	count_open_upload, 
	count_save_publish, 
	count_close_editor, 
	count_pre_bumper_played, 
	count_post_bumper_played, 
	count_bumper_clicked, 
	count_preroll_started, 
	count_midroll_started, 
	count_postroll_started, 
	count_overlay_started, 
	count_preroll_clicked, 
	count_midroll_clicked, 
	count_postroll_clicked, 
	count_overlay_clicked, 
	count_preroll_25, 
	count_preroll_50, 
	count_preroll_75, 
	count_midroll_25, 
	count_midroll_50, 
	count_midroll_75, 
	count_postroll_25, 
	count_postroll_50, 
	count_postroll_75	 
	FROM 
	kalturadw.dwh_aggr_events_widget
   WHERE 
    date_id IN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment 
    WHERE is_calculated = 1
    AND aggr_name = 'widget')
    ;
    
truncate dwh_hourly_partner;
INSERT INTO dwh_hourly_partner 
SELECT 	partner_id, 
	date_id, 
	0 hour_id,
    sum_time_viewed, 
	count_time_viewed, 
	count_plays, 
	count_loads, 
	count_plays_25, 
	count_plays_50, 
	count_plays_75, 
	count_plays_100, 
	count_edit, 
	count_viral, 
	count_download, 
	count_report, 
	count_media, 
	count_video, 
	count_image, 
	count_audio, 
	count_mix, 
	count_mix_non_empty, 
	count_playlist, 
	count_bandwidth, 
	count_storage, 
	count_users, 
	count_widgets, 
	flag_active_site, 
	flag_active_publisher, 
	aggr_storage, 
	aggr_bandwidth, 
	count_buf_start, 
	count_buf_end, 
	count_open_full_screen, 
	count_close_full_screen, 
	count_replay, 
	count_seek, 
	count_open_upload, 
	count_save_publish, 
	count_close_editor, 
	count_pre_bumper_played, 
	count_post_bumper_played, 
	count_bumper_clicked, 
	count_preroll_started, 
	count_midroll_started, 
	count_postroll_started, 
	count_overlay_started, 
	count_preroll_clicked, 
	count_midroll_clicked, 
	count_postroll_clicked, 
	count_overlay_clicked, 
	count_preroll_25, 
	count_preroll_50, 
	count_preroll_75, 
	count_midroll_25, 
	count_midroll_50, 
	count_midroll_75, 
	count_postroll_25, 
	count_postroll_50, 
	count_postroll_75, 
	count_streaming, 
	aggr_streaming
	FROM 
	kalturadw.dwh_aggr_partner 
    WHERE 
    date_id IN (SELECT DISTINCT aggr_day_int FROM kalturadw.aggr_managment 
    WHERE is_calculated = 1
    AND aggr_name = 'partner')
    ;   
	


DROP PROCEDURE IF EXISTS  `calc_aggr_day`;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day`(p_date_val DATE,p_aggr_name VARCHAR(100))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	DECLARE v_hourly_aggr_table VARCHAR(100);
	DECLARE v_aggr_id_field VARCHAR(100);
	DECLARE v_aggr_id_field_str VARCHAR(100);
	DECLARE v_aggr_join_stmt VARCHAR(200);
	DECLARE extra VARCHAR(100);
	
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
	WHERE aggr_name = ''',p_aggr_name,''' AND aggr_day = ''',p_date_val,'''');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	IF ( v_aggr_table <> "" ) THEN
		SET @s = CONCAT('INSERT INTO ',v_aggr_table,'
			(partner_id
			,date_id
		,hour_id
			',v_aggr_id_field_str,' 
			,sum_time_viewed 
			,count_time_viewed 
			,count_plays 
			,count_loads 
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
		SELECT  partner_id,date_id,hour_id',v_aggr_id_field_str,',
		SUM(time_viewed) sum_time_viewed,
		COUNT(time_viewed) count_time_viewed,
		SUM(count_plays) count_plays,
		SUM(count_loads) count_loads,
		SUM(count_plays_25) count_plays_25,
		SUM(count_plays_50) count_plays_50,
		SUM(count_plays_75) count_plays_75,
		SUM(count_plays_100) count_plays_100,
		SUM(count_edit) count_edit,
		SUM(count_viral) count_viral,
		SUM(count_download) count_download,
		SUM(count_report) count_report,
		SUM(count_buf_start) count_buf_start,
		SUM(count_buf_end) count_buf_end,
	    SUM(count_open_full_screen) count_open_full_screen,
	    SUM(count_close_full_screen) count_close_full_screen,
	    SUM(count_replay) count_replay,
	    SUM(count_seek) count_seek,
	    SUM(count_open_upload) count_open_upload,
	    SUM(count_save_publish) count_save_publish,
	    SUM(count_close_editor) count_close_editor,
	    SUM(count_pre_bumper_played) count_pre_bumper_played,
	    SUM(count_post_bumper_played) count_post_bumper_played,
	    SUM(count_bumper_clicked) count_bumper_clicked,
	    SUM(count_preroll_started) count_preroll_started,
	    SUM(count_midroll_started) count_midroll_started,
	    SUM(count_postroll_started) count_postroll_started,
	    SUM(count_overlay_started) count_overlay_started,
	    SUM(count_preroll_clicked) count_preroll_clicked,
	    SUM(count_midroll_clicked) count_midroll_clicked,
	    SUM(count_postroll_clicked) count_postroll_clicked,
	    SUM(count_overlay_clicked) count_overlay_clicked,
	    SUM(count_preroll_25) count_preroll_25,
	    SUM(count_preroll_50) count_preroll_50,
	    SUM(count_preroll_75) count_preroll_75,
	    SUM(count_midroll_25) count_midroll_25,
	    SUM(count_midroll_50) count_midroll_50,
	    SUM(count_midroll_75) count_midroll_75,
	    SUM(count_postroll_25) count_postroll_25,
	    SUM(count_postroll_50) count_postroll_50,
	    SUM(count_postroll_75) count_postroll_75
		FROM (
			SELECT ev.partner_id,MIN(DATE(ev.event_time)*1) date_id, MIN(HOUR(ev.event_time)) hour_id',v_aggr_id_field_str,',ev.session_id,
				MAX(IF(ev.event_type_id IN(4,5,6,7),current_point,NULL))/60000  time_viewed,
				COUNT(IF(ev.event_type_id = 2, 1,NULL)) count_loads,
				COUNT(IF(ev.event_type_id = 3, 1,NULL)) count_plays,
				COUNT(IF(ev.event_type_id = 4, 1,NULL)) count_plays_25,
				COUNT(IF(ev.event_type_id = 5, 1,NULL)) count_plays_50,
				COUNT(IF(ev.event_type_id = 6, 1,NULL)) count_plays_75,
				COUNT(IF(ev.event_type_id = 7, 1,NULL)) count_plays_100,
				COUNT(IF(ev.event_type_id = 8, 1,NULL)) count_edit ,
				COUNT(IF(ev.event_type_id = 9, 1,NULL)) count_viral ,
				COUNT(IF(ev.event_type_id = 10, 1,NULL)) count_download ,
				COUNT(IF(ev.event_type_id = 11, 1,NULL)) count_report,
				COUNT(IF(ev.event_type_id = 12, 1,NULL)) count_buf_start ,
				COUNT(IF(ev.event_type_id = 13, 1,NULL)) count_buf_end	,            
		    COUNT(IF(ev.event_type_id = 14, 1,NULL)) count_open_full_screen	,            
		    COUNT(IF(ev.event_type_id = 15, 1,NULL)) count_close_full_screen,            
		    COUNT(IF(ev.event_type_id = 16, 1,NULL)) count_replay	,            
		    COUNT(IF(ev.event_type_id = 17, 1,NULL)) count_seek	,            
		    COUNT(IF(ev.event_type_id = 18, 1,NULL)) count_open_upload	,            
		    COUNT(IF(ev.event_type_id = 19, 1,NULL)) count_save_publish	,            
		    COUNT(IF(ev.event_type_id = 20, 1,NULL)) count_close_editor	,            
				COUNT(IF(ev.event_type_id = 21, 1,NULL)) count_pre_bumper_played , 
				COUNT(IF(ev.event_type_id = 22, 1,NULL)) count_post_bumper_played	, 
				COUNT(IF(ev.event_type_id = 23, 1,NULL)) count_bumper_clicked 	, 
				COUNT(IF(ev.event_type_id = 24, 1,NULL)) count_preroll_started 	, 
				COUNT(IF(ev.event_type_id = 25, 1,NULL)) count_midroll_started 	, 
				COUNT(IF(ev.event_type_id = 26, 1,NULL)) count_postroll_started, 
				COUNT(IF(ev.event_type_id = 27, 1,NULL)) count_overlay_started, 
				COUNT(IF(ev.event_type_id = 28, 1,NULL)) count_preroll_clicked,
		    COUNT(IF(ev.event_type_id = 29, 1,NULL)) count_midroll_clicked , 
				COUNT(IF(ev.event_type_id = 30, 1,NULL)) count_postroll_clicked	, 
				COUNT(IF(ev.event_type_id = 31, 1,NULL)) count_overlay_clicked 	, 
				COUNT(IF(ev.event_type_id = 32, 1,NULL)) count_preroll_25 	, 
				COUNT(IF(ev.event_type_id = 33, 1,NULL)) count_preroll_50 	, 
				COUNT(IF(ev.event_type_id = 34, 1,NULL)) count_preroll_75, 
				COUNT(IF(ev.event_type_id = 35, 1,NULL)) count_midroll_25, 
				COUNT(IF(ev.event_type_id = 36, 1,NULL)) count_midroll_50	,
				COUNT(IF(ev.event_type_id = 37, 1,NULL)) count_midroll_75	, 
				COUNT(IF(ev.event_type_id = 38, 1,NULL)) count_postroll_25 	, 
				COUNT(IF(ev.event_type_id = 39, 1,NULL)) count_postroll_50 	, 
				COUNT(IF(ev.event_type_id = 40, 1,NULL)) count_postroll_75 	
			FROM dwh_fact_events as ev ',v_aggr_join_stmt,' 
			WHERE ev.event_type_id BETWEEN 2 AND 40 
				AND ev.event_date_id  = DATE(''',p_date_val,''')*1
		    AND ev.event_time BETWEEN DATE(''',p_date_val,''') AND DATE(''',p_date_val,''') + INTERVAL 1 DAY
				AND ev.entry_media_type_id IN (1,5,6)  /* allow only video & audio & mix */
			GROUP BY ev.partner_id',v_aggr_id_field_str,',ev.session_id
		) AS a
		GROUP BY partner_id,date_id, hour_id',v_aggr_id_field_str,';');
		
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		
		SET extra = CONCAT('post_aggregation_',p_aggr_name);
		IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.ROUTINES WHERE ROUTINE_NAME=extra) THEN
			
			SET @ss = CONCAT('CALL ',extra,'(''', p_date_val,''');'); 
			PREPARE stmt1 FROM  @ss;
			EXECUTE stmt1;
			DEALLOCATE PREPARE stmt1;
		END IF ;
	END IF;
    
  
	SET @s = CONCAT('UPDATE aggr_managment SET is_calculated = 1,end_time = NOW()
	WHERE aggr_name = ''',p_aggr_name,''' AND aggr_day = ''',p_date_val,'''');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
    END$$

DELIMITER ;

/* Procedure structure for procedure `calc_entries_sizes` */

DELIMITER $$

DROP PROCEDURE IF EXISTS `calc_entries_sizes`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_entries_sizes`(p_date_id INT(11))
BEGIN
	DECLARE v_date DATETIME;
	SET v_date = DATE(p_date_id);
	UPDATE aggr_managment SET start_time = NOW() WHERE aggr_name = 'storage_usage' AND aggr_day_int = p_date_id;
	
	DROP TABLE IF EXISTS today_file_sync_subset; 
	
	CREATE TEMPORARY TABLE today_file_sync_subset AS
	SELECT s.id, s.partner_id, IFNULL(a.entry_id, object_id) entry_id, object_id, object_type, object_sub_type, IFNULL(file_size, 0) file_size
	FROM kalturadw.dwh_dim_file_sync s LEFT OUTER JOIN kalturadw.dwh_dim_flavor_asset a
	ON (object_type = 4 AND s.object_id = a.id AND a.entry_id IS NOT NULL)
	WHERE s.updated_at BETWEEN v_date AND v_date + INTERVAL 1 DAY
	AND object_type IN (1,4)
	AND original = 1
	AND s.STATUS IN (2,3)
	AND s.partner_id NOT IN (100 /*Default Partner*/ , -1 /*Batch Partner*/ , -2 /*Admin Console*/, 0/* Common Partner Content*/, 99/*Template Partner*/);
	
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
	DROP TABLE IF EXISTS yesterday_file_sync_subset; 
	
	CREATE TEMPORARY TABLE yesterday_file_sync_subset AS
	SELECT f.id, f.partner_id, f.object_id, f.object_type, f.object_sub_type, IFNULL(f.file_size, 0) file_size
	FROM today_file_sync_max_version_ids today, kalturadw.dwh_dim_file_sync f
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
		AND e.partner_id NOT IN (100 /*Default Partner*/ , -1 /*Batch Partner*/ , -2 /*Admin Console*/, 0/* Common Partner Content*/, 99/*Template Partner*/)
		AND e.entry_type_id = 1
		AND e.entry_status_id = 3
		AND es.entry_size_date_id < p_date_id
		GROUP BY es.partner_id, es.entry_id
		HAVING SUM(entry_additional_size_kb) > 0;
	
	INSERT INTO kalturadw.dwh_fact_entries_sizes (partner_id, entry_id, entry_size_date, entry_size_date_id, entry_additional_size_kb)
		SELECT partner_id, entry_id, entry_size_date, entry_size_date_id, entry_additional_size_kb FROM deleted_entries
	ON DUPLICATE KEY UPDATE 
		entry_additional_size_kb = VALUES(entry_additional_size_kb);
	
	UPDATE aggr_managment SET is_calculated = 1, end_time = NOW() WHERE aggr_name = 'storage_usage' AND aggr_day_int = p_date_id;
	UPDATE aggr_managment SET is_calculated = 0 WHERE aggr_name = 'partner_usage' AND aggr_day_int = p_date_id;
	
END$$

/* Procedure structure for procedure `calc_partner_billing_data` */

DROP PROCEDURE IF EXISTS `calc_partner_billing_data`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `calc_partner_billing_data`(p_date_id INT(11),partner_id VARCHAR(100))
BEGIN
SET @current_date_id=LEAST(p_date_id,DATE(NOW())*1);
SET @current_partner_id=partner_id;
SELECT
	FLOOR(continuous_partner_storage.date_id/100) month_id,
	SUM(continuous_aggr_storage/DAY(LAST_DAY(continuous_partner_storage.date_id))) avg_continuous_aggr_storage_mb,
	SUM(continuous_partner_storage.count_bandwidth) sum_partner_bandwidth_kb
FROM
(	
		SELECT 
			all_times.day_id date_id,
			@current_partner_id partner_id,
			aggr_p.count_bandwidth count_bandwidth,
			IF(aggr_p.aggr_storage IS NOT NULL, aggr_p.aggr_storage,
				(SELECT aggr_storage FROM dwh_hourly_partner inner_a_p 
				 WHERE 
					inner_a_p.partner_id=@current_partner_id AND 
					inner_a_p.date_id<all_times.day_id AND 
					inner_a_p.aggr_storage IS NOT NULL 
					AND inner_a_p.hour_id = 0 
					ORDER BY inner_a_p.date_id DESC LIMIT 1)) continuous_aggr_storage
		FROM 
			dwh_hourly_partner aggr_p RIGHT JOIN
			dwh_dim_time all_times
			ON (all_times.day_id=aggr_p.date_id 
				AND aggr_p.partner_id=@current_partner_id
				AND aggr_p.hour_id = 0
				)
		WHERE 	
			all_times.day_id>=20081230 AND all_times.day_id <= @current_date_id
	) continuous_partner_storage
	GROUP BY month_id
	WITH ROLLUP;	
END$$

/* Procedure structure for procedure `calc_partner_storage_data_time_range` */

DROP FUNCTION IF EXISTS `calc_partner_storage_data_time_range`$$

CREATE DEFINER=`root`@`localhost` FUNCTION `calc_partner_storage_data_time_range`(start_date_id INT, end_date_id INT ,partner_id INT ) RETURNS INT(11)
    DETERMINISTIC
BEGIN	
	DECLARE avg_cont_aggr_storage INT;
	SET @current_partner_id=partner_id;
	SET @current_start_date_id=start_date_id;
	SET @current_end_date_id=LEAST(end_date_id, DATE(NOW())*1);

	SELECT	SUM(continuous_aggr_storage/DAY(LAST_DAY(continuous_partner_storage.date_id))) avg_continuous_aggr_storage_mb
	INTO avg_cont_aggr_storage
	FROM (SELECT * FROM (
			SELECT 	all_times.day_id date_id,
				IF(aggr_p.aggr_storage IS NOT NULL, aggr_p.aggr_storage,
					(SELECT aggr_storage FROM dwh_hourly_partner inner_a_p 
					WHERE 	inner_a_p.partner_id=@current_partner_id AND 
						inner_a_p.date_id<all_times.day_id AND 
						inner_a_p.hour_id = 0 AND
						inner_a_p.aggr_storage IS NOT NULL ORDER BY inner_a_p.date_id DESC LIMIT 1)) continuous_aggr_storage
			FROM 	dwh_hourly_partner aggr_p RIGHT JOIN
				dwh_dim_time all_times
				ON (all_times.day_id=aggr_p.date_id 
					AND all_times.day_id>=20081230
					AND all_times.day_id<=@current_end_date_id
					AND aggr_p.partner_id=@current_partner_id)
			WHERE 	all_times.day_id>=20081230 AND all_times.day_id<=@current_end_date_id AND aggr_p.hour_id = 0) results
			WHERE date_id >= @current_start_date_id AND date_id <=@current_end_date_id
		) continuous_partner_storage;

	RETURN avg_cont_aggr_storage;

END$$

/* Procedure structure for procedure `generate_daily_usage_report` */

DROP PROCEDURE IF EXISTS `generate_daily_usage_report`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `generate_daily_usage_report`(p_date_val DATE)
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
		FROM kalturadw.dwh_hourly_partner 
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
		FROM kalturadw.dwh_hourly_partner
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
		
		UNION
		SELECT '25%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_25, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_25, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_25, 0))/5 last_5_days,
		SUM(count_plays_25)/30 last_30_days, 2 sort_order
		FROM kalturadw.dwh_hourly_partner
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
		UNION 
		SELECT '50%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_50, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_50, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_50, 0))/5 last_5_days,
		SUM(count_plays_50)/30 last_30_days, 3 sort_order
		FROM kalturadw.dwh_hourly_partner
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
	
		UNION 
	
		SELECT '75%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_75, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_75, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_75, 0))/5 last_5_days,
		SUM(count_plays_75)/30 last_30_days, 4 sort_order
		FROM kalturadw.dwh_hourly_partner
		WHERE date_id BETWEEN 30_days_ago_date_id AND yesterday_date_id
		UNION 
		SELECT '100%' classification,
		SUM(IF (date_id BETWEEN yesterday_date_id AND yesterday_date_id, count_plays_100, 0)) yesterday,
		SUM(IF (date_id BETWEEN the_day_before_yesteray_date_id AND the_day_before_yesteray_date_id, count_plays_100, 0)) the_day_before,
		SUM(IF (date_id BETWEEN 5_days_ago_date_id AND yesterday_date_id, count_plays_100, 0))/5 last_5_days,
		SUM(count_plays_100)/30 last_30_days, 5 sort_order
		FROM kalturadw.dwh_hourly_partner
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
END$$

DELIMITER ;

/* Procedure structure for procedure `recalc_aggr_day` */

DROP PROCEDURE IF EXISTS  `recalc_aggr_day` ;

DELIMITER $$

CREATE DEFINER=`root`@`localhost` PROCEDURE `recalc_aggr_day`(date_val DATE,p_aggr_name VARCHAR(100))
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	DECLARE v_aggr_id_field VARCHAR(100);
	DECLARE v_hourly_aggr_table VARCHAR(100);

	SELECT aggr_table, aggr_id_field
	INTO  v_aggr_table, v_aggr_id_field
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = p_aggr_name;	
	
	IF (v_aggr_table <> '') THEN 
		SET @s = CONCAT('delete from ',v_aggr_table,'
			where date_id = DATE(''',date_val,''')*1');
		PREPARE stmt FROM  @s;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;	
	END IF;
	
	SET @s = CONCAT('UPDATE aggr_managment SET is_calculated = 0 
	WHERE aggr_name = ''',p_aggr_name,''' AND aggr_day = ''',date_val,'''');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	CALL calc_aggr_day(date_val,p_aggr_name);
    END$$
DELIMITER ;

USE kalturadw_ds;

/*Table structure for table `cycles` */

DROP TABLE IF EXISTS `cycles`;

CREATE TABLE `cycles` (
  `cycle_id` INT(11) NOT NULL AUTO_INCREMENT,
  `status` VARCHAR(60) DEFAULT NULL,
  `prev_status` VARCHAR(60) DEFAULT NULL,
  `insert_time` DATETIME DEFAULT NULL,
  `run_time` DATETIME DEFAULT NULL,
  `transfer_time` DATETIME DEFAULT NULL,
  `process_id` INT(11) DEFAULT '1',
  PRIMARY KEY (`cycle_id`)
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

/*Table structure for table `ds_bandwidth_usage` */

DROP TABLE IF EXISTS `ds_bandwidth_usage`;

CREATE TABLE `ds_bandwidth_usage` (
  `cycle_id` INT(11) NOT NULL,
  `file_id` INT(11) NOT NULL,
  `partner_id` INT(11) NOT NULL DEFAULT '-1',
  `activity_date_id` INT(11) DEFAULT '-1',
  `activity_hour_id` TINYINT(4) DEFAULT '-1',
  `bandwidth_source_id` BIGINT(20) DEFAULT NULL,
  `url` VARCHAR(2000) DEFAULT NULL,
  `bandwidth_bytes` BIGINT(20) DEFAULT '0'
) ENGINE=MYISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY LIST (cycle_id)
(PARTITION p_0 VALUES IN (0) ENGINE = MyISAM) */;

/*Table structure for table `fms_incomplete_sessions` */

DROP TABLE IF EXISTS `fms_incomplete_sessions`;

CREATE TABLE `fms_incomplete_sessions` (
  `session_id` VARCHAR(20) DEFAULT NULL,
  `session_time` DATETIME DEFAULT NULL,
  `updated_time` DATETIME DEFAULT NULL,
  `session_date_id` INT(11) UNSIGNED DEFAULT NULL,
  `con_cs_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `con_sc_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `dis_cs_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `dis_sc_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `partner_id` INT(10) UNSIGNED DEFAULT NULL
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

/*Table structure for table `fms_stale_sessions` */

DROP TABLE IF EXISTS `fms_stale_sessions`;

CREATE TABLE `fms_stale_sessions` (
  `session_id` VARCHAR(20) DEFAULT NULL,
  `session_time` DATETIME DEFAULT NULL,
  `last_update_time` DATETIME DEFAULT NULL,
  `purge_time` DATETIME DEFAULT NULL,
  `session_date_id` INT(11) UNSIGNED DEFAULT NULL,
  `con_cs_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `con_sc_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `dis_cs_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `dis_sc_bytes` BIGINT(20) UNSIGNED DEFAULT NULL,
  `partner_id` INT(10) UNSIGNED DEFAULT NULL
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

/*Table structure for table `invalid_ds_lines` */

DROP TABLE IF EXISTS `invalid_ds_lines`;

CREATE TABLE `invalid_ds_lines` (
  `line_number` INT(11) NOT NULL DEFAULT '0',
  `file_id` INT(11) NOT NULL,
  `error_reason_code` SMALLINT(6) DEFAULT NULL,
  `ds_line` VARCHAR(4096) DEFAULT NULL,
  `insert_time` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `date_id` INT(11) DEFAULT NULL,
  `partner_id` VARCHAR(20) DEFAULT NULL,
  `cycle_id` INT(11) DEFAULT NULL,
  `process_id` INT(11) DEFAULT NULL,
  PRIMARY KEY (`file_id`,`line_number`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

/*Table structure for table `invalid_ds_lines_error_codes` */

DROP TABLE IF EXISTS `invalid_ds_lines_error_codes`;

CREATE TABLE `invalid_ds_lines_error_codes` (
  `error_code_id` SMALLINT(6) NOT NULL AUTO_INCREMENT,
  `error_code_reason` VARCHAR(255) DEFAULT NULL,
  PRIMARY KEY (`error_code_id`),
  UNIQUE KEY `error_code_reason` (`error_code_reason`)
) ENGINE=MYISAM AUTO_INCREMENT=2 DEFAULT CHARSET=utf8;


/*Table structure for table `invalid_fms_event_lines` */

DROP TABLE IF EXISTS `invalid_fms_event_lines`;

CREATE TABLE `invalid_fms_event_lines` (
  `line_id` INT(11) NOT NULL AUTO_INCREMENT,
  `line_number` INT(11) DEFAULT NULL,
  `file_id` INT(11) NOT NULL,
  `error_reason_code` SMALLINT(6) DEFAULT NULL,
  `error_reason` VARCHAR(255) DEFAULT NULL,
  `event_line` VARCHAR(1023) DEFAULT NULL,
  `insert_time` DATETIME DEFAULT NULL,
  `date_id` INT(11) DEFAULT NULL,
  `entry_id` VARCHAR(50) DEFAULT NULL,
  PRIMARY KEY (`line_id`),
  KEY `date_id_partner_id` (`date_id`,`entry_id`),
  KEY `file_reason_code` (`file_id`,`error_reason_code`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

/*Table structure for table `locks` */

DROP TABLE IF EXISTS `locks`;

CREATE TABLE `locks` (
  `lock_id` INT(11) NOT NULL,
  `lock_name` VARCHAR(30) DEFAULT NULL,
  `lock_time` DATETIME DEFAULT NULL,
  `lock_state` TINYINT(1) DEFAULT NULL,
  PRIMARY KEY (`lock_id`)
) ENGINE=MYISAM CHARSET=latin1;

INSERT INTO kalturadw_ds.LOCKS (lock_id, lock_name, lock_state) VALUES(1 ,'daily_lock',FALSE);

/*Table structure for table `ods_fms_session_events` */

DROP TABLE IF EXISTS `ods_fms_session_events`;

CREATE TABLE `ods_fms_session_events` (
  `file_id` INT(11) UNSIGNED NOT NULL,
  `event_type_id` TINYINT(3) UNSIGNED NOT NULL,
  `event_category_id` TINYINT(3) UNSIGNED NOT NULL,
  `event_time` DATETIME NOT NULL,
  `event_time_tz` VARCHAR(3) NOT NULL,
  `event_date_id` INT(11) NOT NULL,
  `event_hour_id` TINYINT(3) NOT NULL,
  `context` VARCHAR(100) DEFAULT NULL,
  `entry_id` VARCHAR(20) DEFAULT NULL,
  `partner_id` INT(10) DEFAULT NULL,
  `external_id` VARCHAR(50) DEFAULT NULL,
  `server_ip` INT(10) UNSIGNED DEFAULT NULL,
  `server_process_id` INT(10) UNSIGNED NOT NULL,
  `server_cpu_load` SMALLINT(5) UNSIGNED NOT NULL,
  `server_memory_load` SMALLINT(5) UNSIGNED NOT NULL,
  `adaptor_id` SMALLINT(5) UNSIGNED NOT NULL,
  `virtual_host_id` SMALLINT(5) UNSIGNED NOT NULL,
  `app_id` TINYINT(3) UNSIGNED NOT NULL,
  `app_instance_id` TINYINT(3) UNSIGNED NOT NULL,
  `duration_secs` INT(10) UNSIGNED NOT NULL,
  `status_id` SMALLINT(3) UNSIGNED DEFAULT NULL,
  `status_desc_id` TINYINT(3) UNSIGNED NOT NULL,
  `client_ip_str` VARCHAR(15) NOT NULL,
  `client_ip` INT(10) UNSIGNED NOT NULL,
  `client_country_id` INT(10) UNSIGNED DEFAULT '0',
  `client_location_id` INT(10) UNSIGNED DEFAULT '0',
  `client_protocol_id` TINYINT(3) UNSIGNED NOT NULL,
  `uri` VARCHAR(4000) NOT NULL,
  `uri_stem` VARCHAR(2000) DEFAULT NULL,
  `uri_query` VARCHAR(2000) DEFAULT NULL,
  `referrer` VARCHAR(4000) DEFAULT NULL,
  `user_agent` VARCHAR(2000) DEFAULT NULL,
  `session_id` VARCHAR(20) NOT NULL,
  `client_to_server_bytes` BIGINT(20) UNSIGNED NOT NULL,
  `server_to_client_bytes` BIGINT(20) UNSIGNED NOT NULL,
  `stream_name` VARCHAR(50) DEFAULT NULL,
  `stream_query` VARCHAR(50) DEFAULT NULL,
  `stream_file_name` VARCHAR(4000) DEFAULT NULL,
  `stream_type_id` TINYINT(3) UNSIGNED DEFAULT NULL,
  `stream_size_bytes` INT(11) DEFAULT NULL,
  `stream_length_secs` INT(11) DEFAULT NULL,
  `stream_position` INT(11) DEFAULT NULL,
  `client_to_server_stream_bytes` INT(10) UNSIGNED DEFAULT NULL,
  `server_to_client_stream_bytes` INT(10) UNSIGNED DEFAULT NULL,
  `server_to_client_qos_bytes` INT(10) UNSIGNED DEFAULT NULL
) ENGINE=MYISAM DEFAULT CHARSET=utf8
/*!50100 PARTITION BY LIST (file_id)
(PARTITION p_0 VALUES IN (0) ENGINE = MyISAM) */;


/* Function  structure for function  `get_error_code` */

DROP FUNCTION IF EXISTS `get_error_code`;
DELIMITER $$

CREATE DEFINER=`etl`@`localhost` FUNCTION `get_error_code`(error_code_reason_param VARCHAR(255)) RETURNS SMALLINT(6)
    NO SQL
BEGIN
	DECLARE error_code SMALLINT(6);
	INSERT IGNORE kalturadw_ds.invalid_ds_lines_error_codes (error_code_reason) VALUES(error_code_reason_param);
	SELECT error_code_id 
		INTO error_code
		FROM kalturadw_ds.invalid_ds_lines_error_codes
		WHERE error_code_reason = error_code_reason_param;
	RETURN error_code;
END $$
DELIMITER ;

/* Procedure structure for procedure `empty_cycle_partition` */

DROP PROCEDURE IF EXISTS  `empty_cycle_partition`;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `empty_cycle_partition`(p_cycle_id VARCHAR(10))
BEGIN
	DECLARE table_name VARCHAR(32);
	
	SELECT source_table INTO table_name FROM kalturadw_ds.staging_areas, kalturadw_ds.cycles
	WHERE staging_areas.process_id = cycles.process_id
	AND cycles.cycle_id = p_cycle_id;
	
	CALL kalturadw_ds.empty_ods_partition(p_cycle_id, table_name);
END$$
DELIMITER ;


/* Procedure structure for procedure `fms_sessionize` */

DROP PROCEDURE IF EXISTS  `fms_sessionize`;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `fms_sessionize`(
  partition_id INTEGER)
BEGIN
  DECLARE SESSION_DATE_IDS VARCHAR(4000);
  DECLARE FMS_STALE_SESSION_PURGE DATETIME;

  SELECT SUBDATE(NOW(),INTERVAL 3 DAY) INTO FMS_STALE_SESSION_PURGE;
  
  
  
  
  
  
  
  
  
  DROP TABLE IF EXISTS ods_temp_fms_session_aggr;
  DROP TABLE IF EXISTS ods_temp_fms_sessions;

  CREATE TEMPORARY TABLE ods_temp_fms_session_aggr (
    agg_session_id       VARCHAR(20) NOT NULL,
    agg_session_time     DATETIME    NOT NULL,
    agg_session_date_id  INT(11)     UNSIGNED,
    agg_con_cs_bytes     BIGINT      UNSIGNED,
    agg_con_sc_bytes     BIGINT      UNSIGNED,
    agg_dis_cs_bytes     BIGINT      UNSIGNED,
    agg_dis_sc_bytes     BIGINT      UNSIGNED,
    agg_partner_id       INT(10)     UNSIGNED
  ) ENGINE = MEMORY;

  CREATE TEMPORARY TABLE ods_temp_fms_sessions (
    session_id         VARCHAR(20) NOT NULL,
    session_time       DATETIME    NOT NULL,
    session_date_id    INT(11)     UNSIGNED,
    session_partner_id INT(10)     UNSIGNED,
    total_bytes        BIGINT      UNSIGNED
   ) ENGINE = MEMORY;


    
  INSERT INTO ods_temp_fms_session_aggr (agg_session_id,agg_session_time,agg_session_date_id,
              agg_con_cs_bytes,agg_con_sc_bytes,agg_dis_cs_bytes,agg_dis_sc_bytes,agg_partner_id)
  SELECT session_id,MAX(event_time),MAX(event_date_id),  
    SUM(IF(t.event_type='connect',client_to_server_bytes,0)) con_cs_bytes,
    SUM(IF(t.event_type='connect',server_to_client_bytes,0)) con_sc_bytes,
    SUM(IF(t.event_type='disconnect',client_to_server_bytes,0)) dis_cs_bytes,
    SUM(IF(t.event_type='disconnect',server_to_client_bytes,0)) dis_sc_bytes,
    MAX(partner_id) partner_id 
  FROM ods_fms_session_events e
 INNER JOIN kalturadw.dwh_dim_fms_event_type t ON e.event_type_id = t.event_type_id
  WHERE file_id = partition_id
  GROUP BY session_id;

  
  
  INSERT INTO ods_temp_fms_sessions (session_id,session_time,session_date_id,session_partner_id,total_bytes)
  SELECT agg_session_id,agg_session_time,agg_session_date_id,agg_partner_id,
  CAST(CAST(agg_dis_sc_bytes AS SIGNED)-CAST(agg_con_sc_bytes AS SIGNED)+CAST(agg_dis_cs_bytes AS SIGNED)-CAST(agg_con_cs_bytes AS SIGNED) AS UNSIGNED)
  FROM ods_temp_fms_session_aggr
  WHERE agg_partner_id IS NOT NULL AND agg_dis_cs_bytes >0 AND agg_con_cs_bytes > 0;

  
  
  INSERT INTO fms_incomplete_sessions (session_id,session_time,updated_time,session_date_id,con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,partner_id)
  SELECT agg_session_id,agg_session_time,NOW() AS agg_update_time,agg_session_date_id,
         agg_con_cs_bytes,agg_con_sc_bytes,agg_dis_cs_bytes,agg_dis_sc_bytes,agg_partner_id
  FROM ods_temp_fms_session_aggr
  WHERE agg_con_cs_bytes = 0 OR agg_dis_cs_bytes = 0 OR agg_partner_id IS NULL
  ON DUPLICATE KEY UPDATE
    
    
    
    
    session_time=GREATEST(session_time,VALUES(session_time)),
    session_date_id=GREATEST(session_date_id,VALUES(session_date_id)),
    
    con_cs_bytes=con_cs_bytes+VALUES(con_cs_bytes),
    con_sc_bytes=con_sc_bytes+VALUES(con_sc_bytes),
    dis_cs_bytes=dis_cs_bytes+VALUES(dis_cs_bytes),
    dis_sc_bytes=dis_sc_bytes+VALUES(dis_sc_bytes),
    
    partner_id=IF(partner_id IS NULL,VALUES(partner_id),partner_id),
    
    updated_time=GREATEST(updated_time,VALUES(updated_time));

    
  INSERT INTO ods_temp_fms_sessions (session_id,session_time,session_date_id,session_partner_id,total_bytes)
  SELECT session_id,session_time,session_date_id,partner_id,
  CAST(CAST(dis_sc_bytes AS SIGNED)-CAST(con_sc_bytes AS SIGNED)+CAST(dis_cs_bytes AS SIGNED)-CAST(con_cs_bytes AS SIGNED) AS UNSIGNED)
  FROM fms_incomplete_sessions
  WHERE partner_id IS NOT NULL AND dis_cs_bytes >0 AND con_cs_bytes > 0;

    
  INSERT INTO fms_stale_sessions (partner_id,session_id,session_time,session_date_id,con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,last_update_time,purge_time)
  SELECT partner_id,session_id,session_time,session_date_id,con_cs_bytes,con_sc_bytes,dis_cs_bytes,dis_sc_bytes,updated_time,NOW()
  FROM fms_incomplete_sessions
  WHERE GREATEST(session_time,updated_time) < FMS_STALE_SESSION_PURGE AND (partner_id IS NULL OR dis_cs_bytes =0 OR con_cs_bytes = 0);

  
  DELETE FROM fms_incomplete_sessions
  WHERE (partner_id IS NOT NULL AND dis_cs_bytes >0 AND con_cs_bytes > 0) OR
       
        GREATEST(session_time,updated_time) < FMS_STALE_SESSION_PURGE;

  
  INSERT INTO kalturadw.dwh_fact_fms_sessions (session_id,session_time,session_date_id,session_partner_id,total_bytes)
  SELECT session_id,session_time,session_date_id,session_partner_id,total_bytes
  FROM ods_temp_fms_sessions;

  
  SELECT CAST(GROUP_CONCAT(DISTINCT session_date_id) AS CHAR)
  INTO SESSION_DATE_IDS
  FROM ods_temp_Fms_sessions;

  IF LENGTH(SESSION_DATE_IDS) > 0 THEN
    CALL mark_for_reaggregation(SESSION_DATE_IDS,'fms_sessions');
  END IF;

END$$
DELIMITER ;

/* Procedure structure for procedure `insert_invalid_ds_line` */

DROP PROCEDURE IF EXISTS  `insert_invalid_ds_line`;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `insert_invalid_ds_line`(line_number_param INT(11), 
									file_id_param INT(11), 
									error_reason_param VARCHAR(255), 
									ds_line_param VARCHAR(4096), 
									date_id_param INT(11),
									partner_id_param INT(11), 
									cycle_id_param INT(11), 
									process_id_param INT(11))
BEGIN
	INSERT INTO invalid_ds_lines (line_number, file_id, error_reason_code, ds_line, date_id, partner_id, cycle_id, process_id)
	VALUES (line_number_param, file_id_param, get_error_code(error_reason_param), ds_line_param, date_id_param, partner_id_param, cycle_id_param, process_id_param);
END$$
DELIMITER ;


/* Procedure structure for procedure `set_cycle_status` */

DROP PROCEDURE IF EXISTS  `set_cycle_status`;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `set_cycle_status`(
	p_cycle_id INT(20),
	new_cycle_status VARCHAR(20)
    )
BEGIN
	UPDATE kalturadw_ds.cycles c
	SET c.prev_status = c.STATUS
	    ,c.STATUS = new_cycle_status
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
    END$$
DELIMITER ;


/* Procedure structure for procedure `transfer_ods_partition` */

DROP PROCEDURE IF EXISTS  `transfer_ods_partition`;

DELIMITER $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `transfer_ods_partition`(
	staging_area_id INTEGER, partition_number VARCHAR(10)
)
BEGIN
DECLARE src_table VARCHAR(45);
DECLARE tgt_table VARCHAR(45);
DECLARE dup_clause VARCHAR(4000);
DECLARE partition_field VARCHAR(45);
DECLARE select_fields VARCHAR(4000);
DECLARE post_transfer_sp_val VARCHAR(4000);
DECLARE aggr_date VARCHAR(4000);
DECLARE aggr_names VARCHAR(4000);
DECLARE s VARCHAR(4000);

SELECT source_table,target_table,IFNULL(on_duplicate_clause,''),staging_partition_field,post_transfer_sp, aggr_date_field, post_transfer_aggregations
INTO src_table,tgt_table,dup_clause,partition_field,post_transfer_sp_val,aggr_date, aggr_names
FROM staging_areas
WHERE id=staging_area_id;

IF ((LENGTH(AGGR_DATE) > 0) && (LENGTH(aggr_names) > 0)) THEN
SELECT CONCAT('update kalturadw.aggr_managment a, (select distinct ',aggr_date,

	        ' from ',src_table,
	        ' where ',partition_field,' = ',partition_number,') ds'
		' set a.is_calculated=0 where a.aggr_day_int = ds.', aggr_date,
		' AND aggr_name in', aggr_names) INTO s;
	SET @s = s;
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END IF;

SELECT GROUP_CONCAT(column_name ORDER BY ordinal_position)
INTO select_fields
FROM information_schema.COLUMNS
WHERE CONCAT(table_schema,'.',table_name) = tgt_table;

	SELECT CONCAT('insert into ',tgt_table, ' (',select_fields,') ',
 ' select ',select_fields,
			 ' from ',src_table,
			 ' where ',partition_field,'  = ',partition_number,
			 ' ',dup_clause ) INTO s;
	
SET @s = s;
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;

IF LENGTH(POST_TRANSFER_SP_VAL)>0 THEN
SET @s = CONCAT('call ',post_transfer_sp_val,'(',partition_number,')');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END IF;

END$$
DELIMITER ;

USE `kalturadw_bisources`;

/*Table structure for table `bisources_asset_status` */

DROP TABLE IF EXISTS `bisources_asset_status`;

CREATE TABLE `bisources_asset_status` (
  `asset_status_id` SMALLINT(6) NOT NULL,
  `asset_status_name` VARCHAR(50) DEFAULT 'missing value',
  PRIMARY KEY (`asset_status_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

/*Table structure for table `bisources_bandwidth_source` */

DROP TABLE IF EXISTS `bisources_bandwidth_source`;

CREATE TABLE `bisources_bandwidth_source` (
  `bandwidth_source_id` INT(11) NOT NULL DEFAULT '0',
  `bandwidth_source_name` VARCHAR(50) DEFAULT NULL,
  PRIMARY KEY (`bandwidth_source_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

INSERT INTO kalturadw_bisources.bisources_bandwidth_source VALUES (1, 'WWW'),(2, 'LLN'),(3,'LEVEL3'), (4,'AKAMAI'), (5, 'FMS Streaming');
INSERT INTO kalturadw.bisources_tables VALUES('bandwidth_source',1);

/*Table structure for table `bisources_creation_mode` */

DROP TABLE IF EXISTS `bisources_creation_mode`;

CREATE TABLE `bisources_creation_mode` (
  `creation_mode_id` SMALLINT(6) NOT NULL,
  `creation_mode_name` VARCHAR(50) DEFAULT 'missing value',
  PRIMARY KEY (`creation_mode_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

/*Table structure for table `bisources_file_sync_object_type` */

DROP TABLE IF EXISTS `bisources_file_sync_object_type`;

CREATE TABLE `bisources_file_sync_object_type` (
  `file_sync_object_type_id` SMALLINT(6) NOT NULL,
  `file_sync_object_type_name` VARCHAR(50) DEFAULT 'missing value',
  PRIMARY KEY (`file_sync_object_type_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

/*Table structure for table `bisources_file_sync_status` */

DROP TABLE IF EXISTS `bisources_file_sync_status`;

CREATE TABLE `bisources_file_sync_status` (
  `file_sync_status_id` SMALLINT(6) NOT NULL,
  `file_sync_status_name` VARCHAR(50) DEFAULT 'missing value',
  PRIMARY KEY (`file_sync_status_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

/*Table structure for table `bisources_ready_behavior` */

DROP TABLE IF EXISTS `bisources_ready_behavior`;

CREATE TABLE `bisources_ready_behavior` (
  `ready_behavior_id` SMALLINT(6) NOT NULL,
  `ready_behavior_name` VARCHAR(50) DEFAULT 'missing value',
  PRIMARY KEY (`ready_behavior_id`)
) ENGINE=MYISAM DEFAULT CHARSET=utf8;

DELIMITER $$

USE `kalturadw_ds`$$

DROP PROCEDURE IF EXISTS `create_updated_entries`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `create_updated_entries`(max_date DATE)
BEGIN
	TRUNCATE TABLE kalturadw_ds.updated_entries;
	
	UPDATE kalturadw.aggr_managment SET start_time = NOW() WHERE is_calculated = 0 AND aggr_day < max_date AND aggr_name = 'plays_views';
	
	INSERT INTO kalturadw_ds.updated_entries SELECT entries.entry_id, SUM(count_loads)+IFNULL(old_entries.views,0) views, SUM(count_plays)+IFNULL(old_entries.plays,0) plays FROM 
	(SELECT DISTINCT entry_id 
		FROM kalturadw.dwh_hourly_events_entry e
		INNER JOIN kalturadw.aggr_managment m ON (e.date_id = m.aggr_day_int)
		WHERE is_calculated = 0 
		  AND m.aggr_day < max_date
		  AND m.aggr_name = 'plays_views') entries
	INNER JOIN
	kalturadw.dwh_hourly_events_entry
	ON (dwh_hourly_events_entry.entry_id = entries.entry_id)
	LEFT OUTER JOIN
	kalturadw.entry_plays_views_before_08_2009 AS old_entries
	ON (entries.entry_id = old_entries.entry_id)
	GROUP BY entries.entry_id;
    END$$

DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_partner`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_partner`(date_val DATE)
BEGIN
	SET @s = CONCAT('UPDATE aggr_managment SET start_time = NOW()
	WHERE aggr_name = ''partner_usage'' AND aggr_day = ''',date_val,'''');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	CALL calc_aggr_day_partner_bandwidth(date_val);
	CALL calc_aggr_day_partner_storage(date_val);
	CALL calc_aggr_day_partner_streaming(date_val);
	CALL calc_aggr_day_partner_usage_totals(date_val);
	
	SET @s = CONCAT('UPDATE aggr_managment SET is_calculated = 1,end_time = NOW()
	WHERE aggr_name = ''partner_usage'' AND aggr_day = ''',date_val,'''');
	
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_bandwidth`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_partner_bandwidth`(date_val DATE)
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	SELECT aggr_table INTO  v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'partner';
	
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		date_id, 
            hour_id,
    		count_bandwidth)
   		SELECT partner_id, max(activity_date_id), 0 hour_id,
			SUM(bandwidth_bytes)/1024 count_bandwidth
		FROM dwh_fact_bandwidth_usage 
		WHERE activity_date_id=DATE(''',date_val,''')*1
		GROUP BY partner_id
    	ON DUPLICATE KEY UPDATE
    		count_bandwidth=VALUES(count_bandwidth);
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_storage`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_partner_storage`(date_val DATE)
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	SELECT aggr_table INTO v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'partner';
      
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		date_id, 
		hour_id,
    		count_storage)
   		SELECT partner_id, max(entry_size_date_id), 0 hour_id, SUM(entry_additional_size_kb)/1024 count_storage
		FROM dwh_fact_entries_sizes
		WHERE entry_size_date_id =DATE(''',date_val,''')*1
		GROUP BY partner_id
		ON DUPLICATE KEY UPDATE
    		count_storage=VALUES(count_storage);
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
END$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_streaming`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_partner_streaming`(date_val DATE)
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	
	SELECT aggr_table INTO v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'partner';
       
	SET @s = CONCAT('
    	INSERT INTO kalturadw.',v_aggr_table,'
    		(partner_id, 
    		date_id, 
		hour_id,
		count_streaming) /* KB */
   		SELECT 	session_partner_id, 
			session_date_id,
			0 hour_id,
			SUM(total_bytes) count_streaming /* KB */
		FROM kalturadw.dwh_fact_fms_sessions 
		WHERE session_date_id=DATE(''',date_val,''')*1
		GROUP BY session_partner_id, session_date_id
    	ON DUPLICATE KEY UPDATE
            count_streaming=VALUES(count_streaming);
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END$$

DROP PROCEDURE IF EXISTS `calc_aggr_day_partner_usage_totals`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `calc_aggr_day_partner_usage_totals`(date_val DATE)
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	
	SELECT aggr_table INTO  v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'partner';
	
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		date_id, 
            hour_id,
    		aggr_storage ,  /* MB */ 
		aggr_bandwidth, /* KB */
		aggr_streaming) /* KB */
		SELECT 
			a.partner_id,
			a.date_id,
            a.hour_id,
			SUM(b.count_storage) aggr_storage,
			SUM(b.count_bandwidth) aggr_bandwidth,
			SUM(b.count_streaming) aggr_streaming
		FROM dwh_hourly_partner a , dwh_hourly_partner b 
		WHERE 
			a.partner_id=b.partner_id
			AND a.date_id=DATE(''',date_val,''')*1
			AND a.date_id >=b.date_id
            AND a.hour_id = 0 AND b.hour_id = 0
		GROUP BY
			a.date_id,
            a.hour_id,
			a.partner_id
		ON DUPLICATE KEY UPDATE
			aggr_storage=VALUES(aggr_storage),
			aggr_bandwidth=VALUES(aggr_bandwidth),
			aggr_streaming=VALUES(aggr_streaming);
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;	
END$$

DELIMITER ;

/*
SQLyog Community v8.7 
MySQL - 5.1.47 : Database - kalturadw_ds
*********************************************************************
*/


/*!40101 SET NAMES utf8 */;

/*!40101 SET SQL_MODE=''*/;

/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
USE `kalturadw_ds`;

/*Table structure for table `updated_kusers_storage_usage` */

DROP TABLE IF EXISTS `updated_kusers_storage_usage`;

CREATE TABLE `updated_kusers_storage_usage` (
  `kuser_id` INT(11) NOT NULL,
  `storage_kb` INT(11)
) ENGINE=MYISAM DEFAULT CHARSET=latin1;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

USE kalturadw_ds;

DROP TABLE IF EXISTS pentaho_sequences;

CREATE TABLE pentaho_sequences (
	seq_id INT(11),
	job_name VARCHAR(250) NOT NULL,
	job_number INT(11),
	is_active BOOLEAN, 
	UNIQUE(seq_id, job_number));

INSERT INTO pentaho_sequences VALUES(1,'dimensions/refresh_bisources_tables.ktr',1,TRUE);	
INSERT INTO pentaho_sequences VALUES(2,'dimensions/update_partners.ktr',1,TRUE);
INSERT INTO pentaho_sequences VALUES(3,'dimensions/update_entries.ktr',1,TRUE),(3,'dimensions/update_flavor_asset.ktr',2,TRUE),(3,'dimensions/update_file_sync.ktr',3,TRUE),(3,'dimensions/update_media_info.ktr',4,TRUE),(3,'dimensions/update_flavor_params.ktr',5,TRUE),(3,'dimensions/update_flavor_params_output.ktr',6,TRUE);
INSERT INTO pentaho_sequences VALUES(4,'dimensions/update_locations_for_kusers.ktr',1,TRUE),(4,'dimensions/update_kusers.ktr',2,TRUE);
INSERT INTO pentaho_sequences VALUES(5,'dimensions/update_ui_conf.ktr',1,TRUE);	
INSERT INTO pentaho_sequences VALUES(6,'dimensions/update_widget.ktr',1,TRUE);
INSERT INTO pentaho_sequences VALUES(7,'dimensions/update_convertsion_profile.ktr',1,TRUE);
INSERT INTO pentaho_sequences VALUES(8,'dimensions/update_flavor_params_conversion_profile.ktr',1,TRUE);

DELIMITER $$

USE `kalturadw_ds`$$

DROP PROCEDURE IF EXISTS `create_updated_kusers_storage_usage`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `create_updated_kusers_storage_usage`(max_date DATE)
BEGIN
	TRUNCATE TABLE kalturadw_ds.updated_kusers_storage_usage;
	
	UPDATE kalturadw.aggr_managment SET start_time = NOW() WHERE is_calculated = 0 AND aggr_day < DATE(max_date) AND aggr_name = 'storage_usage_kuser_sync';
	
	INSERT INTO kalturadw_ds.updated_kusers_storage_usage 
	SELECT u.kuser_id , SUM(s.entry_additional_size_kb) storage_kb FROM 
		(SELECT DISTINCT entry_id FROM kalturadw.dwh_fact_entries_sizes s, kalturadw.aggr_managment m
			WHERE s.entry_size_date_id = m.aggr_day_int
			AND m.aggr_name = 'storage_usage_kuser_sync'
			AND m.aggr_day < max_date 
			AND m.is_calculated = 0) updated_entries, 
		kalturadw.dwh_fact_entries_sizes s, 
		kalturadw.dwh_dim_entries u
	WHERE s.entry_id = u.entry_id 
	AND u.entry_id = updated_entries.entry_id
	GROUP BY u.kuser_id;
END$$
DELIMITER ;

DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `post_aggregation_widget`$$

CREATE DEFINER=`root`@`localhost` PROCEDURE `post_aggregation_widget`(date_val DATE)
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
    	SELECT  
    		partner_id,event_date_id,HOUR(event_time),widget_id,
    		SUM(IF(event_type_id=1,1,NULL)) count_widget_loads
		FROM dwh_fact_events  ev
		WHERE event_type_id IN (1) 
			AND event_date_id = DATE(''',date_val,''')*1
		GROUP BY partner_id,DATE(event_time)*1,HOUR(event_time), widget_id
    	ON DUPLICATE KEY UPDATE
    		count_widget_loads=VALUES(count_widget_loads);
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END$$

DROP PROCEDURE IF EXISTS `post_aggregation_partner`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `post_aggregation_partner`(date_val DATE)
BEGIN
	DECLARE v_aggr_table VARCHAR(100);
	
	SELECT aggr_table INTO v_aggr_table
	FROM kalturadw_ds.aggr_name_resolver
	WHERE aggr_name = 'partner';
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		date_id, 
            hour_id,
    		count_video, 
    		count_image, 
    		count_audio, 
    		count_mix,
    		count_playlist)
    	SELECT  
    		partner_id,date_id,hour_id,
    		SUM(count_video) sum_count_video,
    		SUM(count_image) sum_count_image,
    		SUM(count_audio) sum_count_audio,
    		SUM(count_mix) sum_count_mix,
    		SUM(count_playlist) sum_playlist
    	FROM (
    		SELECT partner_id,en.created_date_id date_id,HOUR(en.created_at) hour_id,
    			COUNT(IF(entry_media_type_id = 1, 1,NULL)) count_video,
    			COUNT(IF(entry_media_type_id = 2, 1,NULL)) count_image,
    			COUNT(IF(entry_media_type_id = 5, 1,NULL)) count_audio,
    			COUNT(IF(entry_media_type_id = 6, 1,NULL)) count_mix,
    			COUNT(IF(entry_type_id = 5, 1,NULL)) count_playlist
    		FROM dwh_dim_entries  en 
    		WHERE (en.entry_media_type_id IN (1,2,5,6) OR en.entry_type_id IN (5) ) 
    			AND en.created_date_id=DATE(''',date_val,''')*1
    		GROUP BY partner_id,en.created_date_id, HOUR(en.created_at)
    	) AS a
    	GROUP BY partner_id,date_id, hour_id
    	ON DUPLICATE KEY UPDATE
    		count_video=VALUES(count_video), 
    		count_image=VALUES(count_image),
    		count_audio=VALUES(count_audio),
    		count_mix=VALUES(count_mix),
    		count_playlist=VALUES(count_playlist);
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
    		(partner_id, 
    		date_id, 
            hour_id,
    		count_users)
    	SELECT  
    		partner_id,ku.created_date_id, HOUR(ku.created_at),
    		COUNT(1)
    	FROM dwh_dim_kusers  ku
    	WHERE 
    		ku.created_date_id=DATE(''',date_val,''')*1
   		GROUP BY partner_id,ku.created_date_id, HOUR(ku.created_at)
    	ON DUPLICATE KEY UPDATE
    		count_users=VALUES(count_users) ;
        ');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
	 
	SET @s = CONCAT('
    	INSERT INTO ',v_aggr_table,'
   		(partner_id, 
    		date_id,
            hour_id,
    		count_widgets)
    	SELECT  
    		partner_id,wd.created_date_id,HOUR(wd.created_at),
    		COUNT(1)
        FROM dwh_dim_widget  wd
    	WHERE 
    		wd.created_date_id=DATE(''',date_val,''')*1
   		GROUP BY partner_id,wd.created_date_id,HOUR(wd.created_at)
    	ON DUPLICATE KEY UPDATE
    		count_widgets=VALUES(count_widgets) ;
    	');
	PREPARE stmt FROM  @s;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;

INSERT INTO kalturadw_ds.processes (id, process_name) VALUE (4, 'bandwidth_usage_AKAMAI');

INSERT INTO `kalturadw_ds`.`staging_areas` 
	(`id`, 
	`process_id`, 
	`source_table`, 
	`target_table`, 
	`on_duplicate_clause`, 
	`staging_partition_field`, 
	`post_transfer_sp`
	)
	VALUES
	(4,	 4,
	 'ds_bandwidth_usage',
	 'kalturadw.dwh_fact_bandwidth_usage',
	 NULL,
	 'cycle_id',
	 NULL
	);

INSERT INTO kalturadw_ds.processes (id, process_name) VALUE (5, 'bandwidth_usage_LLN');

INSERT INTO `kalturadw_ds`.`staging_areas` 
	(`id`, 
	`process_id`, 
	`source_table`, 
	`target_table`, 
	`on_duplicate_clause`, 
	`staging_partition_field`, 
	`post_transfer_sp`
	)
	VALUES
	(5,	 5,
	 'ds_bandwidth_usage',
	 'kalturadw.dwh_fact_bandwidth_usage',
	 NULL,
	 'cycle_id',
	 NULL
	);
	
INSERT INTO kalturadw_ds.processes (id, process_name) VALUE (6, 'bandwidth_usage_LEVEL3');

INSERT INTO `kalturadw_ds`.`staging_areas` 
	(`id`, 
	`process_id`, 
	`source_table`, 
	`target_table`, 
	`on_duplicate_clause`, 
	`staging_partition_field`, 
	`post_transfer_sp`
	)
	VALUES
	(6,	 6,
	 'ds_bandwidth_usage',
	 'kalturadw.dwh_fact_bandwidth_usage',
	 NULL,
	 'cycle_id',
	 NULL
	);
	
INSERT INTO kalturadw_ds.processes (id, process_name) VALUE (7, 'bandwidth_usage_WWW');

INSERT INTO `kalturadw_ds`.`staging_areas` 
	(`id`, 
	`process_id`, 
	`source_table`, 
	`target_table`, 
	`on_duplicate_clause`, 
	`staging_partition_field`, 
	`post_transfer_sp`
	)
	VALUES
	(7,	 7,
	 'ds_bandwidth_usage',
	 'kalturadw.dwh_fact_bandwidth_usage',
	 NULL,
	 'cycle_id',
	 NULL
	);
	
DELIMITER $$

USE `kalturadw`$$

DROP PROCEDURE IF EXISTS `kalturadw`.`add_partition_for_fact_table`$$
CREATE DEFINER=`etl`@`localhost` PROCEDURE  `kalturadw`.`add_partition_for_fact_table`(table_name varchar(100))
BEGIN
DECLARE p_name,p_value VARCHAR(100);
DECLARE p_date,_current_date DATETIME;
DECLARE p_continue BOOL;

SELECT NOW()
  INTO _current_date;

SET p_continue = TRUE;

WHILE (p_continue) DO
  SELECT EXTRACT( YEAR_MONTH FROM FROM_DAYS(MAX(partition_description))) n,
         TO_DAYS(FROM_DAYS(MAX(partition_description)) + INTERVAL 1 MONTH ) v,
    FROM_DAYS(MAX(partition_description))
  INTO p_name,p_value, p_date
  FROM `information_schema`.`partitions`
  WHERE `partitions`.`TABLE_NAME` = table_name;
  IF (_current_date > p_date - INTERVAL 1 MONTH AND p_name is not null) THEN
   SET @s = CONCAT('alter table kalturadw.',table_name,' ADD PARTITION (partition p_' ,p_name ,' values less than (', p_value ,'))');
   PREPARE stmt FROM  @s;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;
  ELSE
   SET p_continue = FALSE;
  END IF;
END WHILE;
END $$


DROP PROCEDURE IF EXISTS `kalturadw`.`add_partition_for_table`$$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `kalturadw`.`add_partition_for_table`(table_name VARCHAR(40))
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

  IF (_current_date > p_date - INTERVAL 1 MONTH AND p_name is not null) THEN
   SET @s = CONCAT('alter table kalturadw.' , table_name , ' ADD PARTITION (partition p_' ,p_name ,' values less than (', p_value ,'))');
   PREPARE stmt FROM  @s;
   EXECUTE stmt;
   DEALLOCATE PREPARE stmt;
  ELSE
   SET p_continue = FALSE;
  END IF;
END WHILE;
END$$

DROP PROCEDURE IF EXISTS `kalturadw_ds`.`add_ods_partition` $$
CREATE PROCEDURE  `kalturadw_ds`.`add_ods_partition`(
 partition_number VARCHAR(10),
table_name VARCHAR(32)
)
BEGIN
 SET @s = CONCAT('alter table kalturadw_ds.',table_name,' ADD PARTITION (partition p_' ,
   partition_number ,' values in (', partition_number ,'))');
 PREPARE stmt FROM  @s;
 EXECUTE stmt;
 DEALLOCATE PREPARE stmt;
END $$

DELIMITER $$

DROP PROCEDURE IF EXISTS `kalturadw_ds`.`empty_ods_partition` $$
CREATE PROCEDURE  `kalturadw_ds`.`empty_ods_partition`(
 partition_number VARCHAR(10),
table_name VARCHAR(32)
)
BEGIN
 CALL drop_ods_partition(partition_number,table_name);
 CALL add_ods_partition(partition_number,table_name);
END $$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS `kalturadw_ds`.`drop_ods_partition` $$
CREATE  PROCEDURE  `kalturadw_ds`.`drop_ods_partition`(
 partition_number VARCHAR(10),
table_name VARCHAR(32)
 )
BEGIN
 SET @s = CONCAT('alter table kalturadw_ds.',table_name,' drop PARTITION  p_' ,
   partition_number );
 PREPARE stmt FROM  @s;
 EXECUTE stmt;
 DEALLOCATE PREPARE stmt;
END $$

DELIMITER ;

USE `kalturadw_ds`;

DELIMITER $$

DROP PROCEDURE IF EXISTS `kalturadw_ds`.`mark_as_aggregated` $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `mark_as_aggregated`( max_date VARCHAR(4000), aggr_name VARCHAR(50))
BEGIN
 SET @s = CONCAT('update kalturadw.aggr_managment set is_calculated=1, end_time=now() ',
   'where aggr_day < ''',max_date,''' ',
            'and is_calculated = 0 ',            
   'and (aggr_name = ''',aggr_name,''' or ''all''=''',aggr_name,''');');
 PREPARE stmt FROM @s;
 EXECUTE stmt;
 DEALLOCATE PREPARE stmt;
END$$

DELIMITER ;

DELIMITER $$

DROP PROCEDURE IF EXISTS `kalturadw_ds`.`mark_for_reaggregation` $$

CREATE DEFINER=`etl`@`localhost` PROCEDURE `mark_for_reaggregation`( date_id_list varchar(4000), aggr_name varchar(50))
BEGIN
 SET @smark4reagg  = CONCAT('update kalturadw.aggr_managment set is_calculated=0,start_time=null,end_time=null ',
   'where aggr_day_int in (',date_id_list,') ',
   'and (aggr_name = ''',aggr_name,''' or ''all''=''',aggr_name,''');');
 PREPARE stmtmark FROM @smark4reagg ;
 EXECUTE stmtmark;
 DEALLOCATE PREPARE stmtmark;
    END$$
DELIMITER ;