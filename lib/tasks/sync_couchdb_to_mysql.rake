namespace :nlims do
    desc "TODO"
    task sync_from_couchdb_to_mysql: :environment do
        if !File.exists?("#{Rails.root}/tmp/couch_seq_number")
            FileUtils.touch "#{Rails.root}/tmp/couch_seq_number"
            seq = "0"
            File.open("#{Rails.root}/tmp/couch_seq_number",'w'){ |f|
              f.write(seq)
            }
          end
        
          config = YAML.load_file("#{Rails.root}/config/couchdb.yml") [Rails.env]
          username = config['username']
          password = config['password']
          db_name = config['prefix'].to_s +  "_order_" +  config['suffix'].to_s
          ip = config['host']
          port = config['port']
          protocol = config['protocol']
          seq = File.read("#{Rails.root}/tmp/couch_seq_number")
          
          res = JSON.parse(RestClient.get("#{protocol}://#{username}:#{password}@#{ip}:#{port}/#{db_name}/_changes?include_docs=true&limit=30&since=#{seq}"))
          docs = res['results']
          
          docs.each do |document|
            puts "-------------------------"       
            
            tracking_number = document['doc']['tracking_number']
            puts tracking_number
            ss = OrderService.check_order(tracking_number)
          
            next if !document['deleted'].blank?
            couch_id =  document['doc']['_id']
            if OrderService.check_order(tracking_number) == true                 
              OrderService.update_order(document,tracking_number)
            else                
             
              OrderService.create_order(document,tracking_number,couch_id)         
            end
             File.open("#{Rails.root}/tmp/couch_seq_number",'w'){ |f|
              f.write(document['seq'])
             } 
          end        
    end  


    desc "TODO"
    task sync_from_couchdb_to_couchdb: :environment do
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
            `curl -X POST http://#{r_host}:#{r_port}/_replicate -d '{"source":"#{remote_address}","target":"#{local_address}","create_target":  true, "continuous":true}' -H "Content-Type: application/json"`
         
    end  



  end