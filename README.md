# README

NOTES

-This module is used for syncronizing data between couchdb at site A with couchdb at site B. Again it will syncronize data at site A, from couchdb to mysql database at that site.
-Therefore, in order for syncing data from couchdb to couchdb, you have to add the sites through the user interface of this module. Two sites will have to be added, thus the site at which
 module is being installed, and the other site. The other site will probably be the molecular laboratory centre were the testing of the samples will be done.


* Ruby version
	ruby  2.5.1
	rails 5.2.1

* Database creation
	rake db:migrate -- it creates two tables (sites and site_sync_frequncies) in the "lims_db" database, the "lims_db_database" is the database which is created by the nlims_controller module
	rake db:seed	-- it load sites data into the sites table which is found in the lims_db database, the seed load from a file named sites.yml which is found in the public folder,
			   please make sure the file is present.



* Database initialization

* How to run the test suite

* Services (job queues, cache servers, search engines, etc.)

* Deployment instructions

* ...
