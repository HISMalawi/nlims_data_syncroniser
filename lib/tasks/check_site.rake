require 'sync.rb'
namespace :check_site do
  desc "TODO"
  task ping_site: :environment do
    sites = Site.where(enabled: true);
  
    sites.each do |site|      
      if Sync.up?(site.host_address)       
        Site.where(id: site.id).update_all(sync_status: 1)
        sf = SiteSyncFrequency.new
        sf.site = site.id        
        sf.status = 1
        sf.save()
      else
        Site.where(id: site.id).update_all(sync_status: 0)
        sf = SiteSyncFrequency.new
        sf.site = site.id       
        sf.status = 0
        sf.save()
      end
    end
  end

end
