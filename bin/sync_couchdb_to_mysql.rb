 require "order_service.rb"
 
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
  res = JSON.parse(RestClient.get("#{protocol}://#{username}:#{password}@#{ip}:#{port}/#{db_name}/_changes?include_docs=true&limit=3000&since=#{seq}"))
  docs = res['results']
  
  docs.each do |document|
    puts "-------------------------"       
    
    tracking_number = document['doc']['tracking_number']
    puts tracking_number
    #next if tracking_number.include?("XLLH")
    next if !document['deleted'].blank?
    couch_id =  document['doc']['_id']
    if OrderService.check_order(tracking_number) == true                 
        if OrderService.check_data_anomalies(document) == true    
           OrderService.update_order(document,tracking_number)
        end
    else                
        if OrderService.check_data_anomalies(document) == true
  	    OrderService.create_order(document,tracking_number,couch_id)
	end         
    end
     File.open("#{Rails.root}/tmp/couch_seq_number",'w'){ |f|
      f.write(document['seq'])
     } 
  end
