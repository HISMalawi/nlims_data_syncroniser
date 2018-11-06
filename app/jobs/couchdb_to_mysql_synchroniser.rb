require 'rest-client'
require 'order_service.rb'
class CouchdbMysqlSynchroniser
  include SuckerPunch::Job
  workers 1
 
  def perform
  
    begin
      username = "root"
      password = "amin9090!"
      db_name = "nlims_order_repo"
      ip = "localhost"
      port = "5984"
      protocol = "http"


      res = JSON.parse(RestClient.get("#{protocol}://#{username}:#{password}@#{ip}:#{port}/#{db_name}/_changes?include_docs=true&limit=30"))
      docs = res['results']
      seq  = res['last_seq']

      docs.each do |document|
        tracking_number = document['id']
        if OrderService.check_order(tracking_number) == true         
          OrderService.update_order(document,tracking_number)
        else       
          OrderService.create_order(document,tracking_number)         
        end
      end

      CouchdbMysqlSynchroniser.perform_in(7)
    rescue      
      CouchdbMysqlSynchroniser.perform_in(7)
    end   
  end
 
end
