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
  @user = covid_conf['target']['username']
  @pass = covid_conf['target']['password']

  @logger = Logger.new("migration_log_#{Time.parse(DateTime.now.to_s)}.txt")

end

def validate_token(m_token)
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
    url = "#{@target_protocol}://#{@target_host}:#{@target_port}#{@target_prefix}re_authenticate/#{@user}/#{@pass}"
    res = JSON.parse(RestClient.get(url,headers))

    if (res["error"] == false)
      @logger.info("Re-Authenticated the token")
      @token = res["data"]["token"]
      return @token
    else
      @logger.info("Re-Authenticated the token. message: #{res["message"]}")
      exit
    end
  end

end

def send_json(json_obj, target_function)

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

def log_results()
  # logging results to file
end


load_defaults

#connect to the database using Mysql2 adapter
source_conn = Mysql2::Client.new(:host => @source_host,
                                 :username => @source_username,
                                 :password => @source_password,
                                 :database => @source_database)


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
                                  cs.result_interpretation as test_result,
                                  cs.dateresultupdated as test_result_date,
                                  cs.trackingno as tracking_number
                                FROM case_samples cs
                                  INNER JOIN case_patient cp
                                    ON cs.patientAutoID = cp.autoID
                                  INNER JOIN test_case tc
                                    ON tc.id = cp.testCase
                                  INNER JOIN samplestatus ss
                                    ON cs.status = ss.ID
                                LIMIT 2")
#raise covid_data.first.to_yaml
covid_data.each do |data_e|

  @logger.info("working on this record  #{data_e["lab_id"].to_s}, #{data_e["tracking_number"].to_s}, #{data_e["test_result"].to_s} ")
  puts "working on this record " + data_e["lab_id"].to_s #for checking purposes

  if (data_e["tracking_no"].blank?) #check if the record has a tracking number
    data = {
      "district" => data_e["district"],
      "health_facility_name" => data_e["health_facility_name"],
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
    @logger.info("#{result.to_yaml}")
    
    if (result["error"] == false)
      
      @logger.info("Just added a new order for  #{data_e["lab_id"].to_s}, with #{ result['message']} #{result['data']}")
      puts "added new order #{ result['message']} #{result['data']}"

      tracking_number = result["data"]["tracking_number"]
      #test_result = data_e["test_result"]
      lab_id = data_e["lab_id"]

      #update tracking number
      source_conn.query("UPDATE case_samples
                        SET trackingno = '#{tracking_number}'
                        WHERE LabID = '#{lab_id}'")
                       
      
      @logger.info("Just updated the tracking number for  #{data_e["lab_id"].to_s} with this value #{tracking_number}")
      puts "Just updated the tracking number for " + data_e["lab_id"].to_s

      if (!data_e["test_result"].blank?)
        @logger.info("This #{data_e["lab_id"].to_s} has a test result ")
        result_data = {
          "tracking_number" => tracking_number,
          "test_status" => 'verified',
          "test_name" => "Covid19",
          "result_date" => data_e["result_date"],
          "who_updated" => {
                              'id':'31',
                              'phone_number':'',
                              'first_name':'Bernard',
                              'last_name':'Kanfosi'
                           },
          "results" => {
                        "Covid19":"#{data_e["test_result"]}"   
                       }  
        }

        dataJSON = JSON.generate(result_data)
        @logger.info("sending reult data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
        result_update = send_json(dataJSON, "update_result")
        @logger.info("#{result_update.to_yaml}")

        if (result_update["error"] == false)
          @logger.info("Just updated the result in NLIMS for #{data_e["lab_id"].to_s}")
            puts "Just updated the result for " + data_e["lab_id"].to_s
        else
          @logger.info("Failed to update the result in NLIMS for #{data_e["lab_id"].to_s}")
            puts "Failed to update the result for " + data_e["lab_id"].to_s
        end

      else
        @logger.info("attempting updating result for  #{data_e["lab_id"].to_s}, with message #{result['message']} , #{result['data']}")
        status_data = {
          "tracking_number" => tracking_number,
          "test_status" => data_e["test_status"],
          "test_name" => "Covid19",
          "result_date" => data_e["test_result_date"],
          "who_updated" => {
                              'id':'31',
                              'phone_number':'',
                              'first_name':'Bernard',
                              'last_name':'Kanfosi'
                           }
        }

        dataJSON = JSON.generate(status_data)
        @logger.info("sending reult data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
        status_update = send_json(dataJSON, "update_status")
        @logger.info("#{status_update.to_yaml}")

        if (status_update["error"] == false)
          @logger.info("Just updated the status for  #{data_e["lab_id"].to_s}")
          puts "Just updated the status for " + data_e["lab_id"].to_s
        else
          @logger.info("Failed to update the status for  #{data_e["lab_id"].to_s}")
          puts "Failed to update the status for " + data_e["lab_id"].to_s
        end
      end
    end
  else
    if (!data_e["test_result"].blank?)
      @logger.info("This #{data_e["lab_id"].to_s} has a test result ")
      res_data = {
          "tracking_number" => data_e["tracking_number"],
          "test_status" => 'verified',
          "test_name" => "Covid19",
          "result_date" => data_e["test_result_date"],
          "who_updated" => {
                                'id':'31',
                                'phone_number':'',
                                'first_name':'Bernard',
                                'last_name':'Kanfosi'
                            },
          "results" => {
                        "Covid19":"#{data_e["test_result"]}"   
                       }  
        }

      dataJSON = JSON.generate(res_data)
      @logger.info("sending reult data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
      res_update = send_json(dataJSON, "update_result")
      @logger.info("#{res_update.to_yaml}")

      if (res_update["error"] == false)
        @logger.info("Just updated the result for  #{data_e["lab_id"].to_s}")
        puts "Just updated the result for " + data_e["lab_id"].to_s
      else
        @logger.info("Failed to update the result for  #{data_e["lab_id"].to_s}")
          puts "Failed to update the result for " + data_e["lab_id"].to_s
      end
    else
      @logger.info("attempting updating result for  #{data_e["lab_id"].to_s}, with message #{result['message']} , #{result['data']}")

      stat_data = {
          "tracking_number" => data_e["tracking_number"],
          "test_status" => data_e["test_status"],
          "test_name" => "Covid19",
          "result_date" => data_e["result_date"],
          "who_updated" => {
                                'id':'31',
                                'phone_number':'',
                                'first_name':'Bernard',
                                'last_name':'Kanfosi'
                            }
        }

      dataJSON = JSON.generate(stat_data)

      @logger.info("sending reult data to send_json -- Update Result    #{data_e["lab_id"].to_s}" )
      stat_update = send_json(dataJSON, "update_status")
      @logger.info("#{status_update.to_yaml}")

      if (stat_update["error"] == false)
        @logger.info("Just updated the status for  #{data_e["lab_id"].to_s}")
        puts "Just updated the status for " + data_e["lab_id"].to_s
      else
        @logger.info("Failed to update the status for  #{data_e["lab_id"].to_s}")
        puts "Failed to update the status for " + data_e["lab_id"].to_s
      end
    end
  end 
 
end

#Print results of the migration

puts "Done!!"