# This file should contain all the record creation needed to seed the database with its default values.
# The data can then be loaded with the rails db:seed command (or created alongside the database with db:setup).
#
# Examples:
#
#   movies = Movie.create([{ name: 'Star Wars' }, { name: 'Lord of the Rings' }])
#   Character.create(name: 'Luke', movie: movies.first)

sites = YAML.load_file("#{Rails.root}/public/sites.yml")
sites.each do |key,site|
	site.each do |s|
		Site.create(
				name: s['facility'],
				district: key,
				x: s['longitude'],
				y: s['latitude'],
				region: s['region'],
				description: s['facility_type'],
				enabled: false,
				sync_status: false,
				site_code: '',
				application_port: '0000',
				host_address: '',
				couch_password: '',
				couch_username: ''
			)
	end
end