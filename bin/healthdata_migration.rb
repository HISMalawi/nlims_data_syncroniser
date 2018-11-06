require 'io/console'
load "bin/tracking_number_service.rb"
$settings = YAML.load_file("#{Rails.root}/config/application.yml")
$configs = YAML.load_file("#{Rails.root}/config/couchdb.yml")[Rails.env]

puts "Enter MYSQL HOST e.g 0.0.0.0"
host = gets.chomp

puts "Enter MYSQL User e,g root"
user = gets.chomp

puts "Enter MYSQL password"
password = STDIN.noecho(&:gets).chomp

puts "Enter healthdata database name"
healthdata_db = gets.chomp

puts "Enter ART database name"
art_db = gets.chomp

$lims_db = "#{$configs['prefix']}_order_#{$configs['suffix']}"
puts "Data to be migrated to NLIMS database #{$lims_db}"

#$couch_query_url = "#{$configs['protocol']}://#{$configs['username']}:#{$configs['password']}@#{$configs['host']}:#{$configs['port']}/#{$lims_db}/_design/Order/_view/generic"
puts "ART Database: #{art_db} , HealthData database: #{healthdata_db},  SQL Username: #{user}, LIMS database: #{$lims_db}"

puts "Continue migration using details above? (y/n)"
proceed = gets.chomp

if proceed.downcase.strip != 'y'
  puts "Migration Stopped"
  Process.kill 9, Process.pid
end

puts "Initialising NLIMS database" 
url = "#{$configs['protocol']}://#{$configs['username']}:#{$configs['password']}@#{$configs['host']}:#{$configs['port']}/#{$lims_db}"
begin
    ress = JSON.parse(RestClient.get(url,:content_type => "application/json"))
    puts "NLIMS database initialised" 
rescue
    ress = JSON.parse(RestClient.put(url,:content_type => "application/json"))
    puts "NLIMS database initialised" 
end
con = Mysql2::Client.new(:host => host,
                         :username => user,
                         :password => password,
                         :database => healthdata_db)

bart2_con = Mysql2::Client.new(:host => host,
                               :username => user,
                               :password => password,
                               :database => art_db)

total = con.query("SELECT COUNT(*) total FROM Lab_Sample INNER JOIN LabTestTable ON LabTestTable.AccessionNum = Lab_Sample.AccessionNum").first["total"].to_i
samples = con.query("SELECT * FROM Lab_Sample INNER JOIN LabTestTable ON LabTestTable.AccessionNum = Lab_Sample.AccessionNum")

def create_concept(con)
  concept_id = con.query("SELECT concept.concept_id AS concept_id FROM concept_name 
                        INNER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE concept_name.name='HIV viral load'").as_json[0]['concept_id']
  return concept_id
end

def get_uuid(con)
  uuid = con.query("SELECT uuid()").as_json[0]['uuid()']
  return uuid
end

def create_encounter(con,patient_id,location_id,date_created,orderer)    
    encounter_date = date_created 
    creator = orderer
    date_created = date_created
    voided = 0
    provider = 92
    voided_by = 1
    date_voided = date_created
    void_reason = ""    
    changed_by = 1
    date_Changed = date_created
    encounter_id = order_counter = con.query("SELECT MAX(encounter_id) AS total FROM encounter").as_json[0]['total'].to_i +  1
    uuid = get_uuid(con)
    encounter_type = con.query("SELECT encounter_type_id AS encout_type FROM encounter_type WHERE name ='LAB'").as_json[0]['encout_type']
    con.query("INSERT INTO encounter (encounter_id,encounter_type,patient_id,provider_id,location_id,encounter_datetime,creator,date_created,voided,voided_by,date_voided,void_reason,uuid,changed_by,date_changed) 
                VALUES('#{encounter_id}','#{encounter_type}','#{patient_id}','#{provider}','#{location_id}','#{encounter_date}','#{creator}','#{date_created}','#{voided}','#{voided_by}','#{date_voided}','#{void_reason}','#{uuid}','#{changed_by}','#{date_created}')")
    return encounter_id
end

no_result_dates = []
orders_with_no_patients = []


samples.each_with_index do |row, i|
    #puts "#{(i + 1)}/#{total}" 
    patient = bart2_con.query(
                    "SELECT n.given_name, n.middle_name, n.family_name, p.birthdate, p.gender, pid2.identifier npid, pid2.patient_id npid2
            FROM patient_identifier pid
                        INNER JOIN person_name n ON n.person_id = pid.patient_id
                        INNER JOIN person p ON p.person_id = pid.patient_id
                        INNER JOIN patient_identifier pid2 ON pid2.patient_id = pid.patient_id AND pid2.voided = 0
                    WHERE pid.identifier = '#{row['PATIENTID']}' AND pid2.voided = 0
                    ").as_json[0] # rescue {}

    orderer = bart2_con.query(
                    "SELECT n.given_name, n.middle_name, n.family_name FROM users u
                        INNER JOIN person_name n ON n.person_id = u.person_id
                    WHERE u.user_id = '#{row['OrderedBy']}' AND n.voided = 0 
                                        ORDER BY u.date_created DESC
                    ").as_json[0] rescue {}

    tests = con.query("SELECT TestOrdered FROM LabTestTable WHERE AccessionNum = #{row['AccessionNum']}").as_json.collect{|h| 
            h["TestOrdered"] = "Viral Load" if h["TestOrdered"] == "HIV_viral_load"
            h["TestOrdered"]
    }

    results = con.query("SELECT * FROM Lab_Parameter       
        WHERE Sample_ID = #{row['Sample_ID']}").as_json
    
    order_date = "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}"
    formatted_results = {}
    time = ""
    formatted_results_value = {}
    sample_status = ""
    status_details = {}
    sample_statuses = {}
    test_statues = {}
    test_status = {}
    date_created = (row['OrderDate'].blank? ? "" : "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}")
    status_details[date_created] = {
        "status" => "Drawn",
        "updated_by":  {
                :first_name => "",
                :last_name => "",
                :phone_number =>  "",
                :id => ""
                }
    }
    test_statues[tests[0]] = status_details

    #getting results for the tests if available -----------------------------------
    counter_control = 1   
    results.each do |rst|        
        r = con.query("SELECT TestName AS test_name FROM codes_TestType WHERE TestType='#{rst['TESTTYPE']}'").as_json
        if !r.blank?
            rst['TestName'] = r[0]['test_name']
        else
            
        end
        
        formatted_results_value[rst['TestName']] = { 
                    :result_value => "",
                    :date_result_entered => ""
        } 
        
        rst['TestName'] = "Viral_Load" if rst['TestName'] == "HIV_DNA_PCR"
        if rst['TestName'] == "Viral_Load"
            sample_status = "DBS (Free drop to DBS card)"
        else
            sample_status = "Blood"
        end
        test_status[rst['TestName']] = status_details      
        next if rst['TESTVALUE'].blank?
        time = rst['TimeStamp'].to_datetime.strftime("%Y%m%d%H%M%S") if !rst['TimeStamp'].blank?
        if rst['TimeStamp'].blank?            
            details = {
            "sample_id": row['Sample_ID'],
            "test": rst['TestName']
            } 
            no_result_dates.push([details])
        end 
        time = Time.new.strftime("%Y%m%d%H%M%S") if rst['TimeStamp'].blank?
        time =  order_date if time < order_date            
            formatted_results_value[rst['TestName']] = {                
                :result_value => "#{rst['Range'].to_s.strip} #{rst['TESTVALUE'].to_s.strip}" ,
                :date_result_entered => time
            }    
        counter_control = counter_control + 1           
    end    
    # end getting results-------------------------------------------
        if counter_control < results.length
            test_statues[tests[0]][time] = {
                "status" => "started",
                "updated_by":  {
                        :first_name => "",
                        :last_name => "",
                        :phone_number =>  "",
                        :id => ""
                        }
            }
        else
            test_statues[tests[0]][time] = {
                "status" => "verified",
                "updated_by":  {
                        :first_name => "",
                        :last_name => "",
                        :phone_number =>  "",
                        :id => ""
                        }
            }
        end

        formatted_results[tests[0]] = {
                "results" => formatted_results_value,
                "result_entered_by" => {
                    :first_name => "",
                    :last_name => "",
                    :phone_number =>  "",
                    :id => ""
                    }
        }
 
        if !no_result_dates.blank?
            File.open("#{Rails.root}/public/no_result_dates", 'a') {|f|
            f.write(no_result_dates) }
            no_result_dates = []
        end
    
        if patient.blank?
            orders_with_no_patients.push(row['Sample_ID'])
            File.open("#{Rails.root}/public/orders_with_no_patients", 'a') {|f|    
            f.write(orders_with_no_patients) }
            orders_with_no_patients = []            
            next
        end
  
        t_num =  TrackingNumberService.generate_tracking_number()
        TrackingNumberService.prepare_next_tracking_number
        puts t_num     
        who_order = {}
        who_order["who_order_test"] = {
            "first_name"=> "",
            "last_name"=> "",
            "id_number"=> "",
            "phone_number"=> ""
        }    
        patient_ = {
                :first_name =>  patient['given_name'],
                :last_name =>patient['family_name'],
                :phone_number => "",
                :id => patient["npid"],
                :email => "",
                :date_of_birth => "#{patient['birthdate'].to_date.strftime('%Y%m%d')}000000",
                :gender => patient['gender'] 
        }      
        sample_statuses[date_created] = {
            "status": "specimen_accepted",
            "updated_by": {
                "first_name": "mwatha",
                "last_name": "mwatha",
                "phone_number": "992",
                "id": "283282"
            }
        }

        res =  Order.create(
                _id: t_num,
                sample_type: sample_status,
                date_created: (row['OrderDate'].blank? ? "" : "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}"),
                sending_facility: $settings['site_name'],
                receiving_facility: $settings['target_lab'],
                tests: tests,
                test_results: formatted_results,
                patient: patient_,
                order_location: row['Location'],
                district: $settings['district'],
                priority: "Routine",
                who_order_test: who_order,
                sample_statuses: sample_statuses,
                test_statuses: test_statues,
                sample_status: "specimen_accepted",
                art_start_date: (patient['start_date'].to_datetime.strftime("%Y%m%d%H%M%S") rescue nil), 
            ) 
        
    if res['_id'] == t_num
        order_type = "4" # standing for LAB order
        orderer_id = "4"  
        instructions = ""
        start_date = (row['OrderDate'].blank? ? "" : "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}")
        expiry_date = (row['OrderDate'].blank? ? "" : "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}")
        discountined = 0
        discountined_date = (row['OrderDate'].blank? ? "" : "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}")
        discountined_by = "4"
        discountined_reason = 1
        creator = "4"
        date_created = (row['OrderDate'].blank? ? "" : "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}")
        voided = 0
        voided_by = ""
        voided_date = (row['OrderDate'].blank? ? "" : "#{row['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{row['OrderTime'].to_time.strftime('%H%M%S')}")
        voided_reason = ""
        patient_id = patient['npid2']
        accession_number = t_num
        obs_id = 1
        
        discountined_reason_non_coded = ""
        order_location =  265

        concept_id = create_concept(bart2_con)
        encouter_id = create_encounter(bart2_con,patient_id,order_location,date_created,orderer_id)   

        order_counter = bart2_con.query("SELECT MAX(order_id) AS total FROM orders").as_json[0]['total'].to_i +  1
        uuid = get_uuid(con)
        bart2_con.query("INSERT INTO orders VALUES('#{order_counter}','#{order_type}','#{concept_id}','#{orderer_id}','#{encouter_id}','#{instructions}','#{start_date}','#{expiry_date}','#{discountined}','#{discountined_date}','#{discountined_by}','#{discountined_reason}','#{creator}','#{date_created}','#{voided}','#{voided_by}','#{voided_date}','#{voided_reason}','#{patient_id}','#{accession_number}','#{obs_id}','#{uuid}','#{discountined_reason_non_coded}')")
        
    end

end



def create_observation()

end


puts "Done!!"
