require 'sync.rb'
class PingSiteJob
  include SuckerPunch::Job

  def perform
    
    begin
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
      PingSiteJob.perform_in(0)
    rescue
      
      PingSiteJob.perform_in(0)
    end   
  end
end
