        settings = YAML.load_file("#{Rails.root}/config/application.yml")
        couchdb_acc = YAML.load_file("#{Rails.root}/config/couchdb.yml")[Rails.env]
        site_name = settings['site_name']
        r_host = ""
        r_port = ""
        l_host = ""
        l_port = "" 
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
                    username =  c_username
                    password = c_password 
                    l_host = host
                    l_port = port 
                    local_address = "http://#{username}:#{password}@#{host}:#{port}/#{db_name}"                     
                else
                    username = c_username
                    password = c_password
                    r_host = host
                    r_port = port
                    remote_address = "http://#{username}:#{password}@#{r_host}:#{r_port}/#{db_name}"                  
                end
            end
        
            `curl -X POST http://#{l_host }:#{l_port}/_replicate -d '{"source":"#{local_address}","target":"#{remote_address}","create_target":  true, "continuous":true}' -H "Content-Type: application/json"`
           # `curl -X POST http://#{r_host}:#{r_port}/_replicate -d '{"source":"#{remote_address}","target":"#{local_address}","create_target":  true, "continuous":true}' -H "Content-Type: application/json"`
         
