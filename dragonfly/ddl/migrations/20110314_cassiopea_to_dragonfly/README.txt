# Update DB objects
mysql -uetl -petl -h@kaltura_op_db_host@ < kaltura.sql
mysql -uetl -petl -h@kaltura_dwh_host@ < new_objects.sql
mysql -uetl -petl -h@kaltura_dwh_host@ < migrated_objects.sql

# Only after you set kettle.properties with the new values run this script in order to update the DB for the etl_logs upgrade
/bin/bash register_old_files_parsed_by_etl_logs.sh

## Migrate daily_etl_execution_sequence and kettle.properties

cp -r ./pentaho-plugins/MySQLInserter32/MySQLInserter /usr/local/pentaho/pdi/plugins/steps/
cp -r ./pentaho-plugins/MappingFieldRunner32/MappingFieldRunner /usr/local/pentaho/pdi/plugins/steps/