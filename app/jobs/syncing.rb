require "rest-client"
class Sync
  include SuckerPunch::Job
  workers 1
 
  def perform
  
    begin
        settings = YAML.load_file("#{Rails.root}/config/application.yml")
        couchdb_acc = YAML.load_file("#{Rails.root}/config/couchdb.yml")[Rails.env]
        site_name = settings['site_name']
        r_host = ""
        r_port = ""
        r_username = ""
        r_password = ""
        remote_address = ""
        local_address = ""

            rs = Site.where(:enabled => true)   
            db_name = couchdb_acc['prefix'].to_s + "_" + "order" + "_" + couchdb_acc['suffix'].to_s           
            rs.each do |r| 
                host = r.host_address
                port = r.application_port
                c_username = r.couch_username
                c_password = r.couch_password
                
                if r.name == site_name 
                    username = couchdb_acc['username']
                    password = couchdb_acc['password'] 
                    local_address = "http://#{username}:#{password}@#{host}:#{port}/#{db_name}"                     
                else
                    username = c_username
                    password = c_password
                    r_host = host
                    r_port = port
                    remote_address = "http://#{username}:#{password}@#{r_host}:#{r_port}/#{db_name}"                  
                end
            end
          puts "---------------------"
          puts remote_address

            `curl -X POST http://localhost:5984/_replicate -d '{"source":"#{local_address}","target":"#{remote_address}","create_target":  true, "continuous":false}' -H "Content-Type: application/json"`
            `curl -X POST http://#{r_host}:#{r_port}/_replicate -d '{"source":"#{remote_address}","target":"#{local_address}","create_target":  true, "continuous":false}' -H "Content-Type: application/json"`
         
            puts r_host
            
        Sync.perform_in(6)
    rescue      
        Sync.perform_in(6)
    end   
  end
 
end
