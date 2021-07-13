class HomeController < ApplicationController
    
    def home
        @sites = Site.where(:enabled => true)
        @en_sites = Site.where(:enabled => false)
        @last = []
        @sites.each do |st|
            next if st.sync_status == 1
            site_id = st.id           
            res = SiteSyncFrequency.find_by_sql("SELECT * FROM site_sync_frequencies WHERE site='#{site_id}'").last
            if res
             @last.push(st.id => res.updated_at)
            end
            
        end
       
    end

    def get_site_details
        id = params[:id]
        site = Site.where(:id => id)

        render plain: site.to_json and return
    end


    def edit_site_details
        site = params[:site]
        port = params[:port]
        ip = params[:ip]
        site_code = params[:site_code]
        username = params[:username]
        password = params[:password]

        res = Site.where(id: site).update_all(application_port: port, host_address: ip, enabled: true, site_code: site_code,couch_username: username, couch_password: password)
        if res == 1
            render plain: true and return
        end
    end


    def disable
        site = params[:site]
        res = Site.where(id: site).update_all(enabled: false);
        if res == 1
            render plain: true and return
        end
    end

    def save_new_site
        site = params[:site]
        code = params[:code]
        district = params[:district]
        region = params[:region]
        description = params[:description]
        latitude = params[:latitude]
        longitude = params[:longitude]
        application_port = params[:applicationport]
        host_address = params[:hostaddress]
        couch_username = params[:couchusername]
        couch_password = params[:couchpassword]
        # couch_port = params[:couchport]
        # ip_address = params[:ipaddress]host_address

        new_site = Site.new(name: site, site_code: code, district: district, region: region, description: description, y: latitude, x: longitude, application_port: application_port, host_address: host_address, couch_username: couch_username, couch_password: couch_password, sync_status: false, enabled: false)
        new_site.save
        render plain: true and return
    end

end
