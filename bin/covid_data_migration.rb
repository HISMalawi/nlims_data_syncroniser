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
# RestClient.get "192.168.43.10/covid_api/data/extract.php?token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwOlwvXC9sb2NhbGhvc3Q6ODBcL2VpZF9hcGlcLyIsImF1ZCI6Imh0dHA6XC9cL2xvY2FsaG9zdDo4MFwvZWlkX2FwaVwvIiwiZGF0YSI6eyJpZCI6IktDSCJ9fQ.SmyFrt8gbaeB9CO1q9Aa31QR570KgHBtpDQocJubaH0"

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
 # @source_target_lab = covid_conf['source']['testing_lab']

  @target_db_host = covid_conf['target_db']['host']
  @target_db_username = covid_conf['target_db']['user']
  @target_db_password = covid_conf['target_db']['password']
  @target_db_database = covid_conf['target_db']['database']
 # @target_db_target_lab = covid_conf['target_db']['testing_lab']

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
              '1' => 'started',
              '2' => 'test_rejected',
              '3' => 'drawn',
              '4' => 'started',
              '5' => 'completed',
              '6' => 'verified',
              '7' => 'completed'
            }
  @results = {
              '1' => 'Negative',
              '2' => 'Positive',
              '3' => 'Indeterminate',
              '4' => 'Invalid',
              '5' => 'Discordant',
              '6' => 'Collect New Sample'
            }
  #for data missing the district data
  @mapped_district_to_lab = {
              'KCH' => 'Blantyre',
              'QECH' => 'Blantyre',
              'MCH' => 'Mzimba',
              'MDH' => 'Mzimba',
              'THY' => 'Thyolo',
              'NDH' => 'Nsanje',
              'ZCH' => 'Zomba',
              'DRM' => 'Blantyre',
              'DRMB' => 'Balaka',
              'PIH' => 'Lilongwe',
              'NRL' => 'Lilongwe',
              'BWL' => 'Lilongwe',
              'MHD' => 'Machinga',
              'MGD' => 'Mangochi',
              'PED' => 'Phalombe',
              'MJD' => 'Mulanje',
              'NDX' => 'Blantyre',
              'CKD' => 'Chikwawa',
              'CPD' => 'Chitipa',
              'RUD' => 'Rumphi',
              'KAD' => 'Karonga',
              'MZH' => 'Mzimba',
              'BT' => 'Blantyre',
              'NHRL' => 'Lilongwe',
              'QCH' => 'Blantyre'
              }

  #for data missing the district data
  @mapped_facility_to_lab = {
              'KCH' => 'Kamuzu Central Hospital',
              'QECH' => 'Queen Elizabeth Central Hospital',
              'MCH' => 'Mzuzu Central Hospital',
              'MDH' => 'Mzimba District Hospital',
              'THY' => 'Thyolo District Hospital',
              'NDH' => 'Nsanje District Hospital',
              'ZCH' => 'Zomba Central Hospital',
              'DRM' => 'Dream Blantyre',
              'DRMB' => 'Balaka District Hospital',
              'PIH' => 'Partners in Hope',
              'NRL' => 'Bwaila Hospital',
              'BWL' => 'Bwaila Hospital',
              'MHD' => 'Machinga District Hospital',
              'MGD' => 'Mangochi District Hospital',
              'PED' => 'Phalombe District Hospital',
              'MJD' => 'Mulanje District Hospital',
              'NDX' => 'Ndirande Health Centre',
              'CKD' => 'Chikwawa District Hospital',
              'CPD' => 'Chitipa District Hospital',
              'RUD' => 'Rumphi District Hospital',
              'KAD' => 'Karonga District Hospital',
              'MZH' => 'Mzuzu Health Centre',
              'BT' => 'Blantyre Getway',
              'NHRL' => 'Bwaila Hospital',
              'QCH' => 'Queen Elizabeth Central Hospital'
              }
  #load health facility names from excel file
  @health_facilities = load_facilities
  @districts = load_districts
  @lab_codes = load_labcodes

  @records_to_exclude = get_already_processed_records - get_records_to_include
  @total_records = 0
  #@records_to_include = get_records_to_include
  #to add to setings
  @method_of_pulling_data = 0 
  @covid_data_url =  covid_conf['source']['url']
  @covid_data_token = covid_conf['source']['token']
  #end
end

def load_districts
  # Method to create a load districts
  # input: nothing 
  # output: list of districts
  # Developer: Precious Bondwe

  district_list = {}

  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                 :username => @target_db_username,
                                 :password => @target_db_password,
                                 :database => @target_db_database)
  
  districts = source_conn.query("SELECT ID, name from districts")
  districts.each do |row|
    district_list.store("#{row["ID"]}", "#{row["name"]}")
  end

  source_conn.close 

  return district_list
end
def get_records_to_include #getting all data that needs fixing
  records_to_include = []
  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                  :username => @target_db_username,
                                  :password => @target_db_password,
                                  :database => @target_db_database)
  
  codes = source_conn.query("SELECT labID, labcode, trackingno FROM migrated_data")

  codes.each do |row|
    if row["trackingno"].to_s.strip == row["labID"].to_s.strip
      records_to_include << row["labID"]
    elsif row["trackingno"].to_s.strip == row["labcode"].to_s.strip
      records_to_include << row["labID"]
    elsif row["trackingno"].to_s.strip.length < 11
      records_to_include << row["labID"]
    end
  end
  
  source_conn.close
  return records_to_include
end

def load_labcodes
  # Method to create a load labcodes
  # input: nothing 
  # output: list of lab codes
  # Developer: Precious Bondwe

  lab_codes = {}

  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                  :username => @target_db_username,
                                  :password => @target_db_password,
                                  :database => @target_db_database)
  
  codes = source_conn.query("SELECT labcode, labname from labcodes")
  codes.each do |row|
    lab_codes.store("#{row["labcode"]}", "#{row["labname"]}")
  end

  source_conn.close

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

def load_facilities_excel_book
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
def load_facilities
  # Method to create a load facilities
  # input: nothing 
  # output: list of lab codes
  # Developer: Precious Bondwe

  facilities = {}

  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                 :username => @target_db_username,
                                 :password => @target_db_password,
                                 :database => @target_db_database)
  
  codes = source_conn.query("SELECT facility_id, facility_name from malawi_facility_lists")

  codes.each do |row|
    facilities.store("#{row["facility_id"]}", "#{row["facility_name"]}")
  end

  source_conn.close

  return facilities
end

def get_already_processed_records_excel_book
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
def get_already_processed_records
  processed_records = []

  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                 :username => @target_db_username,
                                 :password => @target_db_password,
                                 :database => @target_db_database)
  
  records = source_conn.query("SELECT labid from migrated_data WHERE result IS NOT NULL AND trackingno IS NOT NULL")

  records.each do |row|
    processed_records << row["labid"]
  end

  source_conn.close

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

def get_covid_data_from_API #gets data through the API provided by CHAI
  headers = {
      token: @covid_data_token
  }
  
  raw_data = RestClient.get(@covid_data_url,headers)
  
  #sanitize response

  string_to_remove = raw_data.body.split(")")[0][-2,2] 
  new_data = raw_data.body.split("#{string_to_remove})")[1].strip
  new_data[0] = ""
  new_data[new_data.length - 1] = ""

  #Parse JSON
  res = JSON.parse(new_data)
  
  #return body
  return res["body"]

end
def verify_trackingno(lab_id, lab_code, tracking_no) #added after the first pass on the data being migrated
  if tracking_no.to_s.strip == lab_id.to_s.strip
    return "Invalid"
  elsif tracking_no.to_s.strip == lab_code.to_s.strip
    return "Invalid"
  elsif tracking_no.to_s.length < 11
    return "Invalid"
  else
    return "Valid"
  end 
end

def get_data_from_db
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
                                    cs.datetested AS datetested,
                                    cs.LabID AS LabID,
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
  
  source_conn.close

  return covid_data
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
    #debugger
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
def record_exists(lab_id)
  res = ""
  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                  :username => @target_db_username,
                                  :password => @target_db_password,
                                  :database => @target_db_database)

  codes = source_conn.query("SELECT labid from migrated_data WHERE labid = '#{lab_id}'")  

  if codes.first.nil?
    res = "False"
  else
    res = "True"
  end

  source_conn.close

  return res
end
def get_tracking_number(lab_id)
  res = ""
  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                  :username => @target_db_username,
                                  :password => @target_db_password,
                                  :database => @target_db_database)

  codes = source_conn.query("SELECT trackingno from migrated_data WHERE labID = '#{lab_id}'")

  if codes.first.nil?
    res = ""
  else
    res = codes.first["trackingno"]
  end
  source_conn.close

  return res
end
def update_tracking_number(lab_id, tracking_no)
  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                  :username => @target_db_username,
                                  :password => @target_db_password,
                                  :database => @target_db_database)

  codes = source_conn.query("UPDATE migrated_data  SET trackingno = '#{tracking_no}' WHERE labid = '#{lab_id}'")

  source_conn.close

end
def update_migration_status(rep)
  source_conn = Mysql2::Client.new(:host => @target_db_host,
                                  :username => @target_db_username,
                                  :password => @target_db_password,
                                  :database => @target_db_database)

  codes = source_conn.query("UPDATE migrated_data  SET trackingno = '#{rep['trackingno']}',
                                                        result = '#{rep['result']}',
                                                        status = '#{rep['status']}',
                                                        createdorder = '#{rep['createorder']}',
                                                        updatedtrackingno = '#{rep['updatetrackingno']}',
                                                        updatedresult = '#{rep['updateresult']}',
                                                        updatedstatus = '#{rep['updatestatus']}'
                                                 WHERE labid = '#{rep['labid']}'")

  source_conn.close

end

puts "Starting time =>>>>> #{Time.now}"

load_defaults
covid_data = "" # initialise the 

if @method_of_pulling_data == 1 #source_database
  covid_data = get_data_from_db
else # From API
  covid_data = get_covid_data_from_API
end

rec_count = 0
@total_records = covid_data.count
puts "Total records ===> #{@total_records}"

covid_data.each do |data_e|
  rec_count += 1
  puts "Processing record ==> #{rec_count}  of  #{@total_records}" 

  if @records_to_exclude.include?("#{data_e["LabID"]}") == false
    #!@records_to_exclude["#{data_e["lab_id"]}"] #only do this if the lab_id is not an=mong those to be skipped
    if @method_of_pulling_data == 1 #source_database
        data_district = data_e["district"]
        if @districts["#{data_district}"].nil?
          data_district = @mapped_district_to_lab["#{data_e["labcode"]}"]
        else
          data_district = @districts["#{data_district}"]
        end
        data_facility_code = data_e["health_facility_name"]
        if @health_facilities["#{data_facility_code}"].nil?
          data_facility_code = @mapped_facility_to_lab["#{data_e["labcode"]}"]
        else
          data_facility_code = @health_facilities["#{data_facility_code}"]
        end
        if data_e["first_name"].to_s.length == 0
          data_firstname = data_e["LabID"] 
        else
          data_firstname = data_e["first_name"].to_s.gsub("'", "''") 
        end
        if data_e["last_name"].to_s.length == 0
          data_surname = data_e["labcode"] 
        else
          data_surname = data_e["last_name"].to_s.gsub("'", "''") 
        end
        data_dateofbirth = data_e["date_of_birth"]
        data_date_tested = data_e["datetested"] 
        data_reason_for_test = data_e["reason_for_test"]
        if data_reason_for_test.to_s.length == 0
          data_reason_for_test = "Not feeling well"
        end
        data_gender = data_e["gender"]
        if data_gender.to_s.length == 0
          data_gender = "Unknown"
        end
        data_patient_id = data_e["national_patient_id"] 
        if !data_e["who_order_test_first_name"].nil?
          data_fname_completing_form = data_e["who_order_test_first_name"].split(" ")[1].to_s.gsub("'", "''")
          if data_fname_completing_form.to_s.length == 0
            data_fname_completing_form = "Data"
          end
          data_sname_completing_form = data_e["who_order_test_last_name"].split(" ")[0].to_s.gsub("'", "''")
          if data_sname_completing_form.to_s.length == 0
            data_sname_completing_form = "Migrator"
          end
        else
          data_fname_completing_form = "Data" #data_e["who_order_test_first_name"]
          data_sname_completing_form = "Migrator" #data_e["who_order_test_last_name"]
        end
        data_who_order_test_id = ""
        data_lab_code = data_e["labcode"]
        if data_lab_code == "QCH"
          data_lab_code = "QECH"
        end
        data_date_collected = data_e["datecollected"]
        if data_date_collected.to_s.length == 0 
          data_date_collected = data_date_tested
        end
        data_lab_id = data_e["lab_id"]
        data_state = @statuses["#{data_e["test_status"]}"]
        data_date_specimen_received = data_e["date_received"]
        data_date_specimen_sent = data_e["date_dispatched"]
        data_result = @results["#{data_e["test_result"]}"]
        data_date_approved = data_e["test_result_date"]
        data_trackingno = data_e["tracking_number"]
    else # From API
        data_district = data_e["patientResidenceDistrict"]
        if @districts["#{data_district}"].nil?
          data_district = @mapped_district_to_lab["#{data_e["labcode"]}"]
        else
          data_district = @districts["#{data_district}"]
        end
        data_facility_code = data_e["facilitycode"]
        if @health_facilities["#{data_facility_code}"].nil?
          data_facility_code = @mapped_facility_to_lab["#{data_e["labcode"]}"]
        else
          data_facility_code = @health_facilities["#{data_facility_code}"]
        end
        if data_e["firstname"].to_s.length == 0
          data_firstname = data_e["LabID"] 
        else
          data_firstname = data_e["firstname"].to_s.gsub("'", "''")
        end
        if data_e["surname"].to_s.length == 0
          data_surname = data_e["labcode"] 
        else
          data_surname = data_e["surname"].to_s.gsub("'", "''") 
        end
        data_dateofbirth = data_e["dob"] 
        data_date_tested = data_e["datetested"]
        data_reason_for_test = data_e["riskFactor1"]
        if data_reason_for_test.to_s.length == 0
          data_reason_for_test = "Not feeling well"
        end
        data_gender = data_e["gender"]
        if data_gender.to_s.length == 0
          data_gender = "Unknown"
        end
        data_patient_id = data_e["patientID"]
        if !data_e["nameOfPersonCompletingForm"].nil?
          data_fname_completing_form = data_e["nameOfPersonCompletingForm"].split(" ")[1].to_s.gsub("'", "''") 
          if data_fname_completing_form.to_s.length == 0
            data_fname_completing_form = "Data"
          end
          data_sname_completing_form = data_e["nameOfPersonCompletingForm"].split(" ")[0].to_s.gsub("'", "''")
          if data_sname_completing_form.to_s.length == 0
            data_sname_completing_form = "Migrator"
          end
        else
          data_fname_completing_form = "Data" #data_e["nameOfPersonCompletingForm"]
          data_sname_completing_form = "Migrator" #data_e["nameOfPersonCompletingForm"]
        end
        data_who_order_test_id = ""
        data_lab_code = data_e["labcode"]
        if data_lab_code == "QCH" #patching up for crazy data
          data_lab_code = "QECH"
        end
        data_date_collected = data_e["datecollected"]
        if data_date_collected.to_s.length == 0 
          data_date_collected = data_date_tested
        end
        data_lab_id = data_e["LabID"]
        data_state = @statuses["#{data_e["status"]}"]
        data_date_specimen_received = data_e["datespecimenreceivedatlab"]
        data_date_specimen_sent = data_e["datespecimensenttolab"]
        data_result = @results["#{data_e["result"]}"]
        data_date_approved = data_e["dateapproved"]
        data_trackingno = data_e["trackingno"]
    end

    if record_exists(data_lab_id) == "True"
      #update record in the DB
      existing_tracking_number = get_tracking_number(data_lab_id)
      if !existing_tracking_number.empty?
        data_trackingno = existing_tracking_number
      end
    else
      #INSERT the record into the migration_db
      source_conn = Mysql2::Client.new(:host => @target_db_host,
                                  :username => @target_db_username,
                                  :password => @target_db_password,
                                  :database => @target_db_database)

      source_conn.query("INSERT INTO migrated_data (labid, trackingno, result, status, 
                                                    migrationdate, labcode, patientid, 
                                                    patientfname, patientsname, patientgender,
                                                    patientdob, completingformfname, completingformsname,
                                                    dateapproved, reasonfortest) 
                                              VALUES ('#{data_lab_id}','#{data_trackingno}','#{data_result}',
                                                      '#{data_state}', '#{Date.today.strftime("%Y-%m-%d")}', '#{data_lab_code}','#{data_patient_id}',
                                                      '#{data_firstname}','#{data_surname}','#{data_gender}','#{data_dateofbirth}',
                                                      '#{data_fname_completing_form}','#{data_sname_completing_form}',
                                                      '#{data_date_approved}','#{data_reason_for_test}')")
      source_conn.close
    end

    #if tracking no is invalid, then reset it so that it can be processed
    if verify_trackingno(data_lab_id, data_lab_code, data_trackingno) == "Invalid"
        data_trackingno = ""
    end

    report = {'labid' => data_lab_id, 
              'trackingno' => data_trackingno, 
              'result' => data_result, 
              'status' => data_state, 
              'createorder' => 'No', 
              'updatetrackingno' => 'No',
              'updateresult' => 'No', 
              'updatestatus' => 'No'
            }
    @logger_debug.info("working on this record  #{data_lab_id.to_s}, #{data_trackingno.to_s}, #{data_result.to_s} ")
    
    if (data_trackingno.blank?) #check if the record has a tracking number
      data = {
        "district" => data_district,
        "health_facility_name" => data_facility_code, #already assigned the facility in initialization above
        "first_name" => data_firstname,
        "last_name" => data_surname,
        "middle_name" => "",
        "date_of_birth" => data_dateofbirth,
        "gender" => data_gender,
        "national_patient_id" => data_patient_id,
        "phone_number" => "",
        "reason_for_test" => data_reason_for_test,
        "who_order_test_last_name" => data_sname_completing_form,
        "who_order_test_first_name" => data_fname_completing_form,
        "who_order_test_phone_number" => "",
        "who_order_test_id" => data_who_order_test_id,
        "order_location" => data_facility_code, #already assigned the facility in initialization above
        "sample_type" => "Swab", #to check the type of sample that they are using for gene xpert
        "date_sample_drawn" => data_date_collected,
        "tests" => ["Covid19"],
        "sample_status" => data_state, 
        "sample_priority" => 'Routine',
        "target_lab" => @lab_codes["#{data_lab_code}"],
        "date_received" => data_date_specimen_received,
        "date_dispatched" => data_date_specimen_sent, 
        "requesting_clinician" => 'Migration Script'
        }
      dataJSON = JSON.generate(data)
      puts "sending orderdata to send_json"
      result = send_json(dataJSON, "create_order")
      
      if (result["error"] == false)
        
        @logger_debug.info("Just added a new order for  #{data_lab_id.to_s}, with #{result['message']} #{result['data']}")
        puts "added new order #{ result['message']} #{result['data']}"
        
        tracking_number = result["data"]["tracking_number"]
        report["trackingno"] = tracking_number
        report["createorder"] = 'Yes'
         
        lab_id = data_lab_id

        if @method_of_pulling_data == 1 #using the source DB
          #update tracking number
          source_conn = Mysql2::Client.new(:host => @source_host,
                                          :username => @source_username,
                                          :password => @source_password,
                                          :database => @source_database) 
          source_conn.query("UPDATE case_samples
                            SET trackingno = '#{tracking_number}'
                            WHERE labID = '#{lab_id}'")

          source_conn.close
        else #using API
          #update tracking number
          source_conn = Mysql2::Client.new(:host => @target_db_host,
                                          :username => @target_db_username,
                                          :password => @target_db_password,
                                          :database => @target_db_database)
          source_conn.query("UPDATE migrated_data
                            SET trackingno = '#{tracking_number}'
                            WHERE labID = '#{lab_id}'")
           
          source_conn.close                    
        end 

        report["updatetrackingno"] = 'Yes'

        @logger_debug.info("Just updated the tracking number for  #{data_lab_id.to_s} with this value #{tracking_number}")
        puts "Just updated the tracking number for " + data_lab_id.to_s

        if (!data_result.blank?)
          @logger_debug.info("This #{data_lab_id.to_s} has a test result ")
          result_data = {
            "tracking_number" => tracking_number,
            "test_status" => 'verified',
            "test_name" => "Covid19",
            "result_date" => data_date_approved,
            "who_updated" => {
                                'id':'31',
                                'phone_number':'',
                                'first_name':"#{@user}",
                                'last_name':"#{@pass}" 
                            },                         
            "results" => {
                          "Covid19":"#{data_result}" 
                        }  
          }

          dataJSON = JSON.generate(result_data)
          @logger_debug.info("sending reult data to send_json -- Update Result    #{data_lab_id.to_s}" )
          result_update = send_json(dataJSON, "update_result")
          
          if (result_update["error"] == false)
            @logger_debug.info("Just updated the result in NLIMS for #{data_lab_id.to_s}")
            report["updateresult"] = 'Yes'
          else
            @logger_debug.info("Failed to update result for  #{data_lab_id.to_s}, with #{result_update['message']} #{result_update['data']}")
              report["updateresult"] = 'Failed'
          end

        else
          @logger_debug.info("attempting to update status for  #{data_lab_id.to_s}")

          status_data = {
            "tracking_number" => tracking_number,
            "test_status" => data_state,
            "test_name" => "Covid19",
            "result_date" => data_date_approved,
            "who_updated" => {
                                'id':'31',
                                'phone_number':'',
                                'first_name':"#{@user}",
                                'last_name':"#{@pass}"
                            }
                        }

          dataJSON = JSON.generate(status_data)
          @logger_debug.info("sending reult data to send_json -- Update Result    #{data_lab_id.to_s}" )
          status_update = send_json(dataJSON, "update_status")
          
          if (status_update["error"] == false)
            @logger_debug.info("Just updated the status for  #{data_lab_id.to_s}")
            puts "Just updated the status for " + data_lab_id.to_s
            report["updatestatus"] = 'Yes'
          else
            @logger_debug.info("Failed to update the status for  #{data_lab_id.to_s}, with #{status_update['message']} #{status_update['data']}")
            puts "Failed to update the status for " + data_lab_id.to_s
            report["updatestatus"] = 'Failed'
          end
        end
      else
        @logger_debug.info("Failed to create order for  #{data_lab_id.to_s}, with #{result['message']} #{result['data']}")
        report["createorder"] = 'Failed'
      end
    else
      if (!data_result.blank?)
        @logger_debug.info("This #{data_lab_id.to_s} has a test result ")
        res_data = {
            "tracking_number" => data_trackingno,
            "test_status" => 'verified',
            "test_name" => "Covid19",
            "result_date" => data_date_approved,
            "who_updated" => {
                                  'id':'31',
                                  'phone_number':'',
                                  'first_name':"#{@user}",
                                  'last_name':"#{@pass}"
                              },
            "results" => {
                          "Covid19":"#{data_result}"   
                        }  
          }

        dataJSON = JSON.generate(res_data)
        @logger_debug.info("sending result data to send_json -- Update Result    #{data_lab_id.to_s}" )
        res_update = send_json(dataJSON, "update_result")
        
        if (res_update["error"] == false)
          @logger_debug.info("Just updated the result for  #{data_lab_id.to_s}")
          puts "Just updated the result for " + data_lab_id.to_s
          report["updateresult"] = 'Yes'
        else
          @logger_debug.info("Failed to update the result for  #{data_lab_id.to_s}, with #{res_update['message']} #{res_update['data']}")
            puts "Failed to update the result for " + data_lab_id.to_s
            report["updateresult"] = 'Failed'
        end
      else
        @logger_debug.info("attempting to update status for  #{data_lab_id.to_s}")

        stat_data = {
            "tracking_number" => data_trackingno,
            "test_status" => data_state,
            "test_name" => "Covid19",
            "result_date" => data_date_approved,
            "who_updated" => {
                                  'id':'31',
                                  'phone_number':'',
                                  'first_name':"#{@user}",
                                  'last_name':"#{@pass}"
                              }
          }

        dataJSON = JSON.generate(stat_data)
        @logger_debug.info("sending reult data to send_json -- Update State    #{data_lab_id.to_s}" )
        stat_update = send_json(dataJSON, "update_status")
      
        if (stat_update["error"] == false)
          @logger_debug.info("Just updated the status for  #{data_lab_id.to_s}")
          puts "Just updated the status for " + data_lab_id.to_s
          report["updatestatus"] = 'Yes'
        else
          @logger_debug.info("Failed to update the status for  #{data_lab_id.to_s}, with #{stat_update['message']} #{stat_update['data']}")
          puts "Failed to update the status for " + data_lab_id.to_s
          report["updatestatus"] = 'Failed'
        end
      end
    end 
    #log_results(report)
    update_migration_status(report)
  end
end

#Print results of the migration
puts "Ending time =>>>>> #{Time.now}"
puts "Processed =>>>>>> #{@total_records}"

puts "Done!!"