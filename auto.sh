cd ~/kaltura-svn_to_git_sync/git-repositories/kaltura-data-warehouse-upgrades
svn export --force svn+ssh://kelev.kaltura.com/usr/local/kalprod/dwh/site-specific/Cassiopea/CE cassiopea
svn export --force svn+ssh://kelev.kaltura.com/usr/local/kalprod/dwh/site-specific/Dragonfly/CE dragonfly
svn export --force svn+ssh://kelev.kaltura.com/usr/local/kalprod/dwh/site-specific/Eagle/CE eagle
svn export --force svn+ssh://kelev.kaltura.com/usr/local/kalprod/dwh/site-specific/Falcon/CE falcon
git add *
git commit -m "svn to git sync"
git push origin master
echo "completed syncing dwh-upgrades"
