require 'rest-client'
require 'order_service.rb'
class CouchdbMysqlSynchroniser
  include SuckerPunch::Job
  workers 1
 
  def perform
    begin
      if !File.exists?("#{Rails.root}/tmp/couch_seq_number")
        FileUtils.touch "#{Rails.root}/tmp/couch_seq_number"
        seq = "0"
        File.open("#{Rails.root}/tmp/couch_seq_number",'w'){ |f|
          f.write(seq)
        }
      end
      username = "root"
      password = "amin9090!"
      db_name = "nlims_order_repo"
      ip = "localhost"
      port = "5984"
      protocol = "http"
      seq = File.read("#{Rails.root}/tmp/couch_seq_number")
      res = JSON.parse(RestClient.get("#{protocol}://#{username}:#{password}@#{ip}:#{port}/#{db_name}/_changes?include_docs=true&limit=30&since=#{seq}"))
      docs = res['results']
      seq  = res['last_seq']  
      File.open("#{Rails.root}/tmp/couch_seq_number",'w'){ |f|
          f.write(seq)
      }     
      docs.each do |document|
             
        tracking_number = document['doc']['tracking_number']
        couch_id =  document['doc']['_id']
        if OrderService.check_order(tracking_number) == true         
          OrderService.update_order(document,tracking_number)
        else       
          puts "amin--------------------"
          OrderService.create_order(document,tracking_number,couch_id)         
        end
      end

      CouchdbMysqlSynchroniser.perform_in(0)
    rescue      
      CouchdbMysqlSynchroniser.perform_in(0)
    end   
  end
 
end
