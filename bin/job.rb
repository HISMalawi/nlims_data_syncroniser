require 'rest-client'
require 'order_service'


      if !File.exists?("#{Rails.root}/log/nlims_couch_seq_number")
        FileUtils.touch "#{Rails.root}/log/nlims_couch_seq_number"
        seq = "0"
        File.open("#{Rails.root}/log/nlims_couch_seq_number",'w'){ |f|
          f.write(seq)
        }
      end
    
      config = YAML.load_file("#{Rails.root}/config/couchdb.yml")[Rails.env]
      username = config['username']
      password = config['password']
      db_name = config['prefix'].to_s +  "_order_" +  config['suffix'].to_s
      ip = config['host']
      port = config['port']
      protocol = config['protocol']
      begin
        seq = File.read("#{Rails.root}/log/nlims_couch_seq_number")
        res = JSON.parse(RestClient.get("#{protocol}://#{username}:#{password}@#{ip}:#{port}/#{db_name}/_changes?include_docs=true&limit=3000&since=#{seq}"))
        docs = res['results']
          #puts "hello------------ got Some docs!"
          #puts docs
          #puts docs.length
          docs.each do |document|
            #puts "-------------------------"
            puts "processing #{document['id']} #{document['seq']} / #{res['last_seq']} "
            tracking_number = document['doc']['tracking_number']
            couch_id =  document['doc']['_id']

  	        if OrderService.check_order(tracking_number) == true         
              OrderService.update_order(document,tracking_number)
            else       
              OrderService.create_order(document,tracking_number,couch_id)         
      	    end
        
            File.open("#{Rails.root}/log/nlims_couch_seq_number",'w'){ |f|
             f.write(document['seq'])
            }
          end
      end until docs.empty?