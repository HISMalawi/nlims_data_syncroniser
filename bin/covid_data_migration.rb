# Script to migrate Covid19 data into NLIMS
# Developer: Precious Bondwe

# Gets source data
# 1 Checks if a record has a tracking number. (Tracking number is a pointer to show that the record exists in NLIMS)
# 1.1 If tracking number exists, checks if the record has a result
# 1.1.1 If the record has a result
#       It updates MLIMS with a result
# 1.1.2 If the record does not have a result
#       It updates the record status in NLIMS
# 1.2 If tracking record does not have a tracking number
#     It creates an order in NLIMS, and retrieves the tracking number
#     It updates the record with the Tracking number in above
# 1.2.1 If the record has a result
#       It updates MLIMS with a result
# 1.2.2 If the record does not have a result
#       It updates the record status in NLIMS
# Logs the activity


require 'io/console'
require 'json'
require 'rubygems'
require 'net/http'
require 'uri'
require 'yaml'
require 'rest_client'


def load_defaults()
  covid_conf = YAML.load_file("#{Rails.root}/config/covid.yml")

  @source_host = covid_conf['source']['host']
  @source_username = covid_conf['source']['user']
  @source_password = covid_conf['source']['password']
  @source_database = covid_conf['source']['database']
  @source_target_lab = covid_conf['source']['testing_lab']

  @target_host = covid_conf['target']['host']
  @target_prefix = covid_conf['target']['prefix']
  @target_port = covid_conf['target']['port']
  @target_protocol = covid_conf['target']['protocol']
  @token = covid_conf['target']['token']
  @username = covid_conf['target']['username']
  @password = covid_conf['target']['password']
  @user = covid_conf['target']['other_user']
  @pass = covid_conf['target']['other_password']

  @logger_debug = Logger.new("migration_log_debug#{Time.parse(DateTime.now.to_s)}.txt")

  #mapping of statuses: Covid19-Data => NLIMS 
  @statuses = {
              'Inputted' => 'started',
              'Rejected' => 'test_rejected',
              'In-Queue' => 'drawn',
              'in-Process' => 'started',
              'Tested' => 'completed',
              'Dispatched' => 'verified',
              'In-Transit' => 'completed'
            }
  #load health facility names from excel file
  @health_facilities = load_facilities
  @districts = load_districts
  @lab_codes = load_labcodes

  @records_to_exclude = get_already_processed_records

end

def load_districts
  # Method to create a load districts
  # input: nothing 
  # output: list of districts
  # Developer: Precious Bondwe

  district_list = {}

  source_conn = Mysql2::Client.new(:host => @source_host,
                                 :username => @source_username,
                                 :password => @source_password,
                                 :database => @source_database)
  
  districts = source_conn.query("SELECT ID, name from districts")
  districts.each do |row|
    district_list.store("#{row["ID"]}", "#{row["name"]}")
  end

  return district_list
end

def load_labcodes
  # Method to create a load labcodes
  # input: nothing 
  # output: list of lab codes
  # Developer: Precious Bondwe

  lab_codes = {}

  source_conn = Mysql2::Client.new(:host => @source_host,
                                 :username => @source_username,
                                 :password => @source_password,
                                 :database => @source_database)
  
  codes = source_conn.query("SELECT labcode, labname from labcodes")
  codes.each do |row|
    lab_codes.store("#{row["labcode"]}", "#{row["labname"]}")
  end

  return lab_codes


end

def create_log_file
  # Method to create a log file for migration
  # input: nothing 
  # output: Nothing
  # Developer: Precious Bondwe

  file_path = "#{Rails.root}/log/migration_log.xlsx"

  if not File.exists?(file_path)
    #if file does not exist, create a new one with headers
    workbook = RubyXL::Workbook.new
    worksheet = workbook.add_worksheet('data_log')
    
    worksheet.add_cell(0,0,'LabID')
    worksheet.add_cell(0,1,'TrackingNo')
    worksheet.add_cell(0,2,'Result')
    worksheet.add_cell(0,3,'Status')
    worksheet.add_cell(0,4,'CreateOrder')
    worksheet.add_cell(0,5,'UpdateTrackingNo')
    worksheet.add_cell(0,6,'UpdateResult') 
    worksheet.add_cell(0,7,'UpdateStatus')

    workbook.write(file_path)
  end

end

def log_results(rep)
  # Method to log current activity
  # input: hash containing the message to log 
  # output: Nothing
  # Developer: Precious Bondwe
  
  # logging results to file
  # Actions taking place on a record
  # 1 => Create Order in NLIMS
  # 2 => Update Tracking number in EID/VL
  # 3 => Update Result in NLIMS
  # 4 => Update status in NLIMS
  # Structure of Report (LabID, Tracking NUmber, Result, Status, actions {})

  file_path = "#{Rails.root}/log/migration_log.xlsx"
  if not File.exists?(file_path)
    #if file does not exist, create a new one with headers
    create_log_file
  else
    workbook = RubyXL::Parser.parse("#{file_path}")
    worksheet = workbook['data_log']
    total_rows = worksheet.count

    # worksheet.insert_row(total_rows)
    worksheet.add_cell(total_rows,0,"#{rep["labid"]}")
    worksheet.add_cell(total_rows,1,"#{rep["trackingno"]}")
    worksheet.add_cell(total_rows,2,"#{rep["result"]}")
    worksheet.add_cell(total_rows,3,"#{rep["status"]}")
    worksheet.add_cell(total_rows,4,"#{rep["createorder"]}")
    worksheet.add_cell(total_rows,5,"#{rep["updatetrackingno"]}")
    worksheet.add_cell(total_rows,6,"#{rep["updateresult"]}") 
    worksheet.add_cell(total_rows,7,"#{rep["updatestatus"]}")

    workbook.write(file_path)
  end

end

def load_facilities
  # Method to load facilities from the Malawi Health Facilities File
  # input: Nothing
  # Output: returns hash with facility codes and names
  # Developer: Precious Bondwe

  facilities = {}

  file_path = "#{Rails.root}/config/malawi_health_facilities.xlsx"
  workbook = RubyXL::Parser.parse("#{file_path}")
  worksheet = workbook[0]
  
  x = 0
  worksheet.each do |row|
    if x == 0 # skip the headers
      x += 1
    else
      facilities.store("#{row.cells[0].value}","#{row.cells[2].value}")
      x += 1
    end  
  end
  return facilities
end

def get_already_processed_records
  # Method to load facilities from the Malawi Health Facilities File
  # input: Nothing
  # Output: returns hash with facility codes and names
  # Developer: Precious Bondwe

  processed_records = []

  file_path = "#{Rails.root}/log/migration_log.xlsx"

  if not File.exists?(file_path)
    #if file does not exist, create a new one with headers
    create_log_file
  else
  
    workbook = RubyXL::Parser.parse("#{file_path}")
    worksheet = workbook[0]
    
    x = 0
    worksheet.each do |row|
      if x == 0 # skip the headers
        x += 1
      else
        processed_records << "#{row.cells[0].value}"
        x += 1
      end 
    end 
  end
  return processed_records
end

def validate_token(m_token)
  # Method to check if the token is valid
  # input: current token
  # Output: returns a new token if the one provided is expired, or returns the same one if it is not expired
  # Developer: Precious Bondwe

  headers = {
    content_type: "application/json",
    token: m_token
  }

  token_data = {"token" => m_token}
  data = JSON.generate(token_data)

  url = "#{@target_protocol}://#{@target_host}:#{@target_port}#{@target_prefix}check_token_validity"

  result = JSON.parse(RestClient.get(url,headers))
  
  if (result["error"] == false)
    @token = m_token
    return @token
  else
    headers = {
    content_type: "text/plain",
    token: m_token
    }
    url = "#{@target_protocol}://#{@target_host}:#{@target_port}#{@target_prefix}re_authenticate/#{@username}/#{@password}"
    res = JSON.parse(RestClient.get(url,headers))

    if (res["error"] == false)
      @logger_debug.info("Re-Authenticated the token")
      @token = res["data"]["token"]
      return @token
    else
      @@logger_debug.info("Re-Authenticated the token. message: #{res["message"]}")
      exit
    end
  end

end

def send_json(json_obj, target_function)
  # Method to send a json object to the NLIMS end points
  # input: Json Object, function to execute 
  # output: the result of the action, whether it has an error or not
  # Developer: Precious Bondwe


  headers = {
      content_type: "application/json",
      token: validate_token(@token)
  }
 
  if (target_function == 'create_order')
    url = "#{@target_protocol}://#{@target_host}:#{@target_port}#{@target_prefix}create_order" 
    res = JSON.parse(RestClient.post(url,json_obj,headers))
    return res
  elsif (target_function == 'update_result')
    url = "#{@target_protocol}://#{@target_host}:#{@target_port}#{@target_prefix}update_test" 
    res = JSON.parse(RestClient.post(url,json_obj,headers))
    return res

  elsif (target_function == 'update_status')
    url = "#{@target_protocol}://#{@target_host}:#{@target_port}#{@target_prefix}update_test"  
    res = JSON.parse(RestClient.post(url,json_obj,headers))
    return res
  else
    return ''
  end

end


puts "Starting time =>>>>> #{Time.now}"

load_defaults

#connect to the database using Mysql2 adapter
source_conn = Mysql2::Client.new(:host => @source_host,
                                 :username => @source_username,
                                 :password => @source_password,
                                 :database => @source_database)

exclude_records = "'" + @records_to_exclude.join("','") + "'"


covid_data = source_conn.query("SELECT 
                                  cp.patientResidenceDistrict AS district,
                                  cp.facilitycode AS health_facility_name, 
                                  cp.firstname AS first_name,
                                  cp.surname AS last_name,
                                  cp.riskFactor1 AS reason_for_test,
                                  cp.dob AS date_of_birth,
                                  cp.gender AS gender, 
                                  cp.patientID AS national_patient_id, 
                                  cp.nameOfPersonCompletingForm AS who_order_test_last_name, 
                                  cp.nameOfPersonCompletingForm AS who_order_test_first_name,  
                                  cp.labcode AS target_lab,
                                  cs.datecollected AS date_sample_drawn,
                                  cs.LabID AS lab_id,
                                  ss.state AS test_status,
                                  cs.datespecimenreceivedatlab AS date_received, 
                                  cs.datespecimentsenttolab AS date_dispatched,
                                  rs.name as test_result,
                                  cs.dateapproved as test_result_date,
                                  cs.trackingno as tracking_number
                                FROM case_samples cs
                                  INNER JOIN case_patient cp
                                    ON cs.patientAutoID = cp.autoID
                                  INNER JOIN test_case tc
                                    ON tc.id = cp.testCase
                                  INNER JOIN samplestatus ss
                                    ON cs.status = ss.ID
                                  LEFT JOIN results rs
                                    on rs.id = cs.result
                                WHERE cs.LabID NOT IN (#{exclude_records})")

covid_data.each do |data_e|

  report = {'labid' => data_e["lab_id"], 
            'trackingno' => data_e["tracking_number"], 
            'result' => data_e["test_result"], 
            'status' => data_e["test_status"], 
            'createorder' => 'No', 
            'updatetrackingno' => 'No',
            'updateresult' => 'No', 
            'updatestatus' => 'No'
          }
  @logger_debug.info("working on this record  #{data_e["lab_id"].to_s}, #{data_e["tracking_number"].to_s}, #{data_e["test_result"].to_s} ")
 
  if (data_e["tracking_number"].blank?) #check if the record has a tracking number
    data = {
      "district" => data_e["district"],
      "health_facility_name" => @health_facilities["#{data_e["health_facility_name"]}"],
      "first_name" => data_e["first_name"],
      "last_name" => data_e["last_name"],
      "middle_name" => "", #data_e["middle_name"],
      "date_of_birth" => data_e["date_of_birth"],
      "gender" => data_e["gender"],
      "national_patient_id" => data_e["national_patient_id"],
      "phone_number" => "",
      "reason_for_test" => data_e["reason_for_test"],
      "who_order_test_last_name" => data_e["who_order_test_last_name"],
      "who_order_test_first_name" => data_e["who_order_test_first_name"],
      "who_order_test_phone_number" => "", #data_e["who_order_test_phone_number"],
      "who_order_test_id" => data_e["who_order_test_id"],
      "order_location" => data_e["health_facility_name"], #data_e["order_location"],
      "sample_type" => "Swab", #to check the type of sample that they are using for gene xpert
      "date_sample_drawn" => data_e["date_sample_drawn"],
      "tests" => ["Covid19"],
      "sample_status" => data_e["test_status"], #Check how this is done in NLIMS
      "sample_priority" => 'Routine',
      "target_lab" => @source_target_lab,
      "date_received" => data_e["date_received"],
      "date_dispatched" => data_e["date_dispatched"],
      "requesting_clinician" => "Migration Script"
      }
    dataJSON = JSON.generate(data)
    puts "sending orderdata to send_json"
    result = send_json(dataJSON, "create_order")
    puts "Created order"
    if (result["error"] == false)
      
      @logger_debug.info("Just added a new order for  #{data_e["lab_id"].to_s}, with #{ result['message']} #{result['data']}")
      puts "added new order #{ result['message']} #{result['data']}"
      
      tracking_number = result["data"]["tracking_number"]
      report["trackingno"] = tracking_number
      report["createorder"] = 'Yes'
     
      lab_id = data_e["lab_id"]

      #update tracking number
      source_conn.query("UPDATE case_samples
                        SET trackingno = '#{tracking_number}'
                        WHERE LabID = '#{lab_id}'")
                       
      report["updatetrackingno"] = 'Yes'

      @logger_debug.info("Just updated the tracking number for  #{data_e["lab_id"].to_s} with this value #{tracking_number}")
      puts "Just updated the tracking number for " + data_e["lab_id"].to_s

      if (!data_e["test_result"].blank?)
        @logger_debug.info("This #{data_e["lab_id"].to_s} has a test result ")
        result_data = {
          "tracking_number" => tracking_number,
          "test_status" => 'verified',
          "test_name" => "Covid19",
          "result_date" => data_e["result_date"],
          "who_updated" => {
                              'id':'31',
                              'phone_number':'',
                              'first_name':"#{@user}",
                              'last_name':"#{@pass}" 
                          },                         
          "results" => {
                        "Covid19":"#{data_e["test_result"]}"   
                       }  
        }

        dataJSON = JSON.generate(result_data)
        @logger_debug.info("sending reult data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
        result_update = send_json(dataJSON, "update_result")
        
        if (result_update["error"] == false)
          @logger_debug.info("Just updated the result in NLIMS for #{data_e["lab_id"].to_s}")
            puts "Just updated the result for " + data_e["lab_id"].to_s
            report["updateresult"] = 'Yes'
        else
          @logger_debug.info("Failed to update the result in NLIMS for #{data_e["lab_id"].to_s}")
            puts "Failed to update the result for " + data_e["lab_id"].to_s
            report["updateresult"] = 'Failed'
        end

      else
        @logger_debug.info("attempting updating status for  #{data_e["lab_id"].to_s}")
        status_data = {
          "tracking_number" => tracking_number,
          "test_status" => @statuses["#{data_e["test_status"]}"],
          "test_name" => "Covid19",
          "result_date" => data_e["test_result_date"],
          "who_updated" => {
                              'id':'31',
                              'phone_number':'',
                              'first_name':"#{@user}",
                              'last_name':"#{@pass}"
                           }
                       }

        dataJSON = JSON.generate(status_data)
        @logger_debug.info("sending reult data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
        status_update = send_json(dataJSON, "update_status")
        
        if (status_update["error"] == false)
          @logger_debug.info("Just updated the status for  #{data_e["lab_id"].to_s}")
          puts "Just updated the status for " + data_e["lab_id"].to_s
          report["updatestatus"] = 'Yes'
        else
          @logger_debug.info("Failed to update the status for  #{data_e["lab_id"].to_s}")
          puts "Failed to update the status for " + data_e["lab_id"].to_s
          report["updatestatus"] = 'Failed'
        end
      end
    end
  else
    if (!data_e["test_result"].blank?)
      @logger_debug.info("This #{data_e["lab_id"].to_s} has a test result ")
      res_data = {
          "tracking_number" => data_e["tracking_number"],
          "test_status" => 'verified',
          "test_name" => "Covid19",
          "result_date" => data_e["test_result_date"],
          "who_updated" => {
                                'id':'31',
                                'phone_number':'',
                                'first_name':"#{@user}",
                                'last_name':"#{@pass}"
                            },
          "results" => {
                        "Covid19":"#{data_e["test_result"]}"   
                       }  
        }

      dataJSON = JSON.generate(res_data)
      @logger_debug.info("sending result data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
      res_update = send_json(dataJSON, "update_result")
      
      if (res_update["error"] == false)
        @logger_debug.info("Just updated the result for  #{data_e["lab_id"].to_s}")
        puts "Just updated the result for " + data_e["lab_id"].to_s
        report["updateresult"] = 'Yes'
      else
        @logger_debug.info("Failed to update the result for  #{data_e["lab_id"].to_s}")
          puts "Failed to update the result for " + data_e["lab_id"].to_s
          report["updateresult"] = 'Failed'
      end
    else
      @logger_debug.info("attempting to update status for  #{data_e["lab_id"].to_s}")

      stat_data = {
          "tracking_number" => data_e["tracking_number"],
          "test_status" => @statuses["#{data_e["test_status"]}"],
          "test_name" => "Covid19",
          "result_date" => data_e["result_date"],
          "who_updated" => {
                                'id':'31',
                                'phone_number':'',
                                'first_name':"#{@user}",
                                'last_name':"#{@pass}"
                            }
        }

      dataJSON = JSON.generate(stat_data)

      @logger_debug.info("sending reult data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
      stat_update = send_json(dataJSON, "update_status")
     
      if (stat_update["error"] == false)
        @logger_debug.info("Just updated the status for  #{data_e["lab_id"].to_s}")
        puts "Just updated the status for " + data_e["lab_id"].to_s
        report["updatestatus"] = 'Yes'
      else
        @logger_debug.info("Failed to update the status for  #{data_e["lab_id"].to_s}")
        puts "Failed to update the status for " + data_e["lab_id"].to_s
        report["updatestatus"] = 'Failed'
      end
    end
  end 
 log_results(report)
end

#Print results of the migration
puts "Ending time =>>>>> #{Time.now}"

puts "Done!!"