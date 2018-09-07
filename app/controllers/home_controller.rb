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

        res = Site.where(id: site).update_all(application_port: port, host_address: ip, enabled: true, site_code: site_code)
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

end
