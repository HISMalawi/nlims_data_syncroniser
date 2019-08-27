require 'rest-client'
require 'order_service'


      if !File.exists?("#{Rails.root}/tmp/couch_seq_number")
        FileUtils.touch "#{Rails.root}/tmp/couch_seq_number"
        seq = "0"
        File.open("#{Rails.root}/tmp/couch_seq_number",'w'){ |f|
          f.write(seq)
        }
      end
    
      config = YAML.load_file("#{Rails.root}/config/couchdb.yml")[Rails.env]
      username = config['username']
      password = config['password']
      db_name = config['prefix'].to_s +  "_" +  config['suffix'].to_s
      ip = config['host']
      port = config['port']
      protocol = config['protocol']
      seq = File.read("#{Rails.root}/tmp/couch_seq_number")
      res = JSON.parse(RestClient.get("#{protocol}://#{username}:#{password}@#{ip}:#{port}/#{db_name}/_changes?include_docs=true&limit=3000&since=#{seq}"))
      docs = res['results']
      seq  = res['last_seq']  
          
      puts "hello--------------"
      puts docs
      puts docs.length
      docs.each do |document|
        puts "-------------------------"
        puts document
        tracking_number = document['doc']['tracking_number']
        couch_id =  document['doc']['_id']

  	    if OrderService.check_order(tracking_number) == true         
          OrderService.update_order(document,tracking_number)
        else       
          OrderService.create_order(document,tracking_number,couch_id)         
      	end
        
      File.open("#{Rails.root}/tmp/couch_seq_number",'w'){ |f|
        f.write(seq)
      }
      end

