require 'io/console'
load "lib/order_service.rb"
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

puts "new migration or continuation? (n/c)"
opt = gets.chomp

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
if !File.exists?("#{Rails.root}/public/orders_with_no_patients.json")
    data = {}
    FileUtils.touch("#{Rails.root}/public/orders_with_no_patients.json")   
    data['Samples'] = []
    File.open("#{Rails.root}/public/orders_with_no_patients.json","w"){ |w|
        w.write(data.to_json)
    }
else
    data = {}
    data['Samples'] = []
    File.open("#{Rails.root}/public/orders_with_no_patients.json","w"){ |w|
        w.write(data.to_json)
    }
end

con = Mysql2::Client.new(:host => host,
                         :username => user,
                         :password => password,
                         :database => healthdata_db)

bart2_con = Mysql2::Client.new(:host => host,
                               :username => user,
                               :password => password,
                               :database => art_db)

    total = con.query("SELECT COUNT(*) total FROM Lab_Sample").first["total"].to_i # counting orders to be migrated

if opt.downcase.strip == "n"
    samples = con.query("SELECT * FROM Lab_Sample") # retrieving the orders
else
    previous_sam = File.read("#{Rails.root}/public/sample_tracker") 
    samples = con.query("SELECT * FROM Lab_Sample WHERE Sample_ID >='#{previous_sam}'")
    c = con.query("SELECT count(*) co FROM Lab_Sample WHERE Sample_ID >='#{previous_sam}'").first['co'].to_i
    total = total -  c
end

# creating observation for inserting order to openmrs
def get_concept(con,type)
    if type == "Blood"
        concept_id = con.query("SELECT concept.concept_id AS concept_id FROM concept_name 
                        INNER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE concept_name.name='Laboratory tests ordered'").as_json[0]['concept_id']
    else
        concept_id = con.query("SELECT concept.concept_id AS concept_id FROM concept_name 
                        INNER JOIN concept ON concept.concept_id = concept_name.concept_id
                        WHERE concept_name.name='Laboratory tests ordered'").as_json[0]['concept_id']
    end  
  return concept_id
end

def get_uuid(con)
  uuid = con.query("SELECT uuid()").as_json[0]['uuid()']
  return uuid
end

# creating observation for inserting order to openmrs
def create_encounter(con,patient_id,location_id,date_created,orderer)    
    encounter_date = date_created 
    creator = orderer
    date_created = date_created
    voided = 0
    provider = 92
    encounter_id = order_counter = con.query("SELECT MAX(encounter_id) AS total FROM encounter").as_json[0]['total'].to_i +  1
    uuid = get_uuid(con)
    encounter_type = con.query("SELECT encounter_type_id AS encout_type FROM encounter_type WHERE name ='LAB'").as_json[0]['encout_type']
    con.query("INSERT INTO encounter (encounter_id,encounter_type,patient_id,provider_id,location_id,encounter_datetime,creator,date_created,voided,uuid) 
                VALUES('#{encounter_id}','#{encounter_type}','#{patient_id}','#{provider}','#{location_id}','#{encounter_date}','#{creator}','#{date_created}','#{voided}','#{uuid}')")
    return encounter_id
end

def escape_characters(value)
    value = value.to_s.gsub(/'/,"")
    value = value.to_s.gsub(/,/," ")
    value = value.to_s.gsub(/;/," ")
    value = value.to_s.gsub(")"," ")
    value = value.to_s.gsub("("," ")
    value = value.to_s.gsub("/"," ")
    value = value.to_s.gsub('\\'," ")
  return value
end

# creating observation for inserting order to openmrs
def create_observation(con,person_id,encounter_id,location_id,concept_id,datetime)
    uuid = get_uuid(con)
    obs_datetime = datetime
    obs_id = obs_counter = con.query("SELECT MAX(obs_id) AS total FROM obs").as_json[0]['total'].to_i +  1
    con.query("INSERT INTO obs (obs_id,person_id,concept_id,encounter_id,obs_datetime,location_id,creator,date_created,voided,uuid)
        VALUES('#{obs_id}','#{person_id}','#{concept_id}','#{encounter_id}','#{obs_datetime}','#{location_id}','#{4}','#{obs_datetime}','#{0}','#{uuid}')
    ")
    return obs_id
end

no_result_dates = []
orders_with_no_patients = []
tests_without_results = []
orders_without_orderer = []
orders_without_tests = []
order = {}
migrated_orders = 0
results_checker = false
date_given = ''

samples.each_with_index do |row, i|
    
    puts "#{(i + 1)}/#{total}" # progress update       
    test_counter = 0 
    formatted_results_value = {}
    results_controller = 0
    tests_ordered = []
    status_details = {}
    test_status = {}
    formatted_results = {}
    test_statues = {}
    sample_type = ""
    sample_typees = {}
    time = ""
    orderer = ""
    patient = ""
    patient_id = ""
    order_date = Time.new.strftime("%Y%m%d%H%M%S")
    
    if !row['AccessionNum'].blank?
       
        tests = con.query("SELECT * FROM LabTestTable WHERE AccessionNum = #{row['AccessionNum']}").as_json
        tests.each do |test_details|
            tests_ordered.push(test_details['TestOrdered'])
            patient_id = test_details['Pat_ID']
            if test_counter == 0
                order_date = (test_details['OrderDate'].blank? ? "" : "#{test_details['OrderDate'].to_date.strftime('%Y%m%d')}" + "#{test_details['OrderTime'].to_time.strftime('%H%M%S')}")
                patient = bart2_con.query("SELECT n.given_name, n.middle_name, n.family_name, p.birthdate, p.gender, pid2.identifier npid, pid2.patient_id npid2
                                            FROM patient_identifier pid
                                            INNER JOIN person_name n ON n.person_id = pid.patient_id
                                            INNER JOIN person p ON p.person_id = pid.patient_id
                                            INNER JOIN patient_identifier pid2 ON pid2.patient_id = pid.patient_id AND pid2.voided = 0
					    WHERE pid.identifier = '#{patient_id}' AND (pid2.voided = 0 AND pid.identifier_type = 3)").as_json[0]

                id__ = test_details['OrderedBy']
                orderer = bart2_con.query("SELECT n.given_name, n.middle_name, n.family_name FROM users u
                                            INNER JOIN person_name n ON n.person_id = u.person_id
                                            WHERE u.user_id = '#{id__}' AND n.voided = 0 
                                            ORDER BY u.date_created DESC").as_json[0] rescue {}
                
                if orderer.blank?
                    orders_without_orderer.push(row['Sample_ID'].to_s);
                    orderer = {}
                    orderer['given_name'] = ""
                    orderer['family_name'] = ""
                    orderer['id'] = 1
                else
                    orderer['id'] = id__               
                end               
            end
         
            if !patient.blank?
                    status_details[order_date] = {
                        "status" => "Drawn",
                        "updated_by":  {
                                :first_name => orderer['given_name'],
                                :last_name => orderer['family_name'],
                                :phone_number =>  "",
                                :id => test_details['OrderedBy']
                                }
                    } 
                    test_statues[test_details['TestOrdered']] = status_details
                    results = con.query("SELECT * FROM Lab_Parameter WHERE Sample_ID = #{row['Sample_ID']}").as_json
                    results.each do |rst|        
                        r = con.query("SELECT TestName AS test_name FROM codes_TestType WHERE TestType='#{rst['TESTTYPE']}'").as_json
                        if !r.blank?
                            rst['TestName'] = r[0]['test_name']
                        end
                        rst['TestName'] = "Viral Load" if rst['TestName'] == "HIV_DNA_PCR"  || rst['TestName'] == "HIV_RNA_PCR"
                        formatted_results_value[rst['TestName']] = { 
                                    :result_value => "",
                                    :date_result_entered => ""
                        }                         
                    
                        test_status[rst['TestName']] = status_details
                            time = rst['TimeStamp'].to_datetime.strftime("%Y%m%d%H%M%S") if !rst['TimeStamp'].blank?
                            time = Time.new.strftime("%Y%m%d%H%M%S") if rst['TimeStamp'].blank?
                            time =  order_date if time < order_date            
                            formatted_results_value[rst['TestName']] = {                
                                :result_value => "#{rst['Range'].to_s.strip} #{rst['TESTVALUE'].to_s.strip}" ,
                                :date_result_entered => time
                            }  
                        results_controller = results_controller + 1                         
                    end   

                    if formatted_results_value.blank?
                        formatted_results[test_details['TestOrdered']] = {
                                "results" => formatted_results_value,
                                "result_entered_by" => {
                                    :first_name => "",
                                    :last_name => "",
                                    :phone_number =>  "",
                                    :id => ""
                                    }
                        }
                    else
                        formatted_results[test_details['TestOrdered']] = {
                                "results" => formatted_results_value,
                                "result_entered_by" => {
                                    :first_name => orderer['given_name'],
                                    :last_name => orderer['family_name'],
                                    :phone_number =>  "",
                                    :id => test_details['OrderedBy']
                                    }
                        }
                    end

                    if results_controller < results.length
                        test_statues[test_details['TestOrdered']][time] = {
                            "status" => "started",
                            "updated_by":  {
                                    :first_name => orderer['given_name'],
                                    :last_name => orderer['family_name'],
                                    :phone_number =>  "",
                                    :id =>test_details['OrderedBy']
                                    }
                        }
                    elsif  results_controller >= results.length
                        test_statues[test_details['TestOrdered']][time] = {
                            "status" => "verified",
                            "updated_by":  {
                                    :first_name => orderer['given_name'],
                                    :last_name => orderer['family_name'],
                                    :phone_number =>  "",
                                    :id => test_details['OrderedBy']
                                    }
                        }
                        results_checker = true
                        time = Time.new.strftime("%Y%m%d%H%M%S") if time.blank?
                        date_given = time 
                    end

                    if row['TestOrdered'] == "HIV_viral_load"
                        sample_type = "DBS (Free drop to DBS card)"
                    else
                        sample_type = "Blood"
                    end
                    
                    formatted_results_value = {}
                    test_counter = test_counter + 1
            else
                orders_with_no_patients.push(patient_id)
                count = JSON.parse(File.read("#{Rails.root}/public/orders_with_no_patients.json"))
                count['Samples'] = count['Samples'].push(row["Sample_ID"])

                File.open("#{Rails.root}/public/orders_with_no_patients.json", 'w') {|f|
                f.write(count.to_json)}
                # no patient
            end
        end
        if !patient.blank?
            t_num =  TrackingNumberService.generate_tracking_number()
            TrackingNumberService.prepare_next_tracking_number
            puts t_num   
            who_order = {}
            who_order = {
                "first_name"=> orderer['given_name'],
                "last_name"=> orderer['family_name'],
                "id_number"=> orderer['id'],
                "phone_number"=> ""
            }    
            patient_ = {
                    :first_name =>  patient['given_name'],
                    :last_name => patient['family_name'],
                    :phone_number => "",
                    :id => patient_id,
                    :email => "",
                    :date_of_birth => "#{patient['birthdate'].to_date.strftime('%Y%m%d')}000000",
                    :gender => patient['gender'] 
            }      
            sample_typees[order_date] = {
                "status": "specimen_accepted",
                "updated_by": {
                    "first_name": orderer['given_name'],
                    "last_name": orderer['family_name'],
                    "phone_number": "",
                    "id": orderer['id'],
                }
            }

            start_date =  order_date
            res =  Order.create(
                    tracking_number: t_num,
                    sample_type: sample_type,
                    date_created: order_date,
                    sending_facility: $settings['site_name'],
                    receiving_facility: $settings['target_lab'],
                    tests: tests_ordered,
                    test_results: formatted_results,
                    patient: patient_,
                    order_location: "OPD 1",
                    district: $settings['district'],
                    priority: "Routine",
                    who_order_test: who_order,
                    sample_statuses: sample_typees,
                    test_statuses: test_statues,
                    sample_status: "specimen_accepted",
                    art_start_date: (patient['start_date'].to_datetime.strftime("%Y%m%d%H%M%S") rescue nil), 
                ) 

            data = {
                    tracking_number: t_num,
                    sample_type: sample_type,
                    date_created: order_date,
                    sending_facility: $settings['site_name'],
                    receiving_facility: $settings['target_lab'],
                    tests: tests_ordered,
                    test_results: formatted_results,
                    patient: patient_,
                    order_location: "OPD 1",
                    district: $settings['district'],
                    requesting_clinician: '',
                    priority: "Routine",
                    who_order_test: who_order,
                    sample_statuses: sample_typees,
                    test_statuses: test_statues,
                    sample_status: "specimen_accepted",
                    art_start_date: (patient['start_date'].to_datetime.strftime("%Y%m%d%H%M%S") rescue nil), 
            }


            if res['tracking_number'] == t_num
                c_id = res['_id']
                order_type = "4" # standing for LAB order
                orderer_id =  orderer['id']  
                discontinued = 0       
                creator =   orderer['id']  
                date_created = order_date
                voided = 0      
                patient_id = patient['npid2']
                accession_number = t_num
                order_location =  265
                concept_id =  get_concept(bart2_con,sample_type)
                encouter_id = create_encounter(bart2_con,patient_id,order_location,date_created,orderer_id)   
                obs_id = create_observation(bart2_con,patient['npid2'],encouter_id,order_location,concept_id,date_created)
    
                order_counter = bart2_con.query("SELECT MAX(order_id) AS total FROM orders").as_json[0]['total'].to_i +  1
                uuid = get_uuid(con)
                if results_checker == false
                bart2_con.query("INSERT INTO orders (order_id,order_type_id,concept_id,orderer,encounter_id,instructions,start_date,discontinued,creator,date_created,voided,patient_id,accession_number,uuid)
                        VALUES('#{order_counter}','#{order_type}','#{concept_id}','#{orderer_id}','#{encouter_id}','#{c_id}','#{start_date}','#{discontinued}','#{creator}','#{date_created}','#{voided}','#{patient_id}','#{accession_number}','#{uuid}')")
                else
                    voided = 0
                    date_voided = date_given
                    voided_by =  orderer['id']  
                    void_reason = "result given"
                    #bart2_con.query("INSERT INTO orders (order_id,order_type_id,concept_id,orderer,encounter_id,instructions,start_date,discontinued,creator,date_created,voided,date_voided,voided_by,void_reason,patient_id,accession_number,uuid)
                    #VALUES('#{order_counter}','#{order_type}','#{concept_id}','#{orderer_id}','#{encouter_id}','#{c_id}','#{start_date}','#{discontinued}','#{creator}','#{date_created}','#{voided}','#{date_voided}','#{voided_by}','#{void_reason}','#{patient_id}','#{accession_number}','#{uuid}')")
                    bart2_con.query("INSERT INTO orders (order_id,order_type_id,concept_id,orderer,encounter_id,instructions,start_date,discontinued,creator,date_created,voided,patient_id,accession_number,uuid)
                        VALUES('#{order_counter}','#{order_type}','#{concept_id}','#{orderer_id}','#{encouter_id}','#{c_id}','#{start_date}','#{discontinued}','#{creator}','#{date_created}','#{voided}','#{patient_id}','#{accession_number}','#{uuid}')")
                end
                OrderService.create_order_v2(data,t_num,c_id)
    
                migrated_orders = migrated_orders + 1
            end         
        end
    else
        orders_without_tests.push(row['Sample_ID'])
            # order without tests
    end
    
    if !File.exists?("#{Rails.root}/public/sample_tracker") 
        FileUtils.touch("#{Rails.root}/public/sample_tracker") 
        File.open("#{Rails.root}/public/sample_tracker",'w') { |t|
            t.write(row['Sample_ID'])
        } 
    else
        File.open("#{Rails.root}/public/sample_tracker",'w') { |t|
            t.write(row['Sample_ID'])
        } 
    end
     
    
end

puts "Done!!"
puts "Total Orders: " + total.to_s
puts "Orders Migrated: " + migrated_orders.to_s
puts "Orders Not Migrated: " + (total.to_i - migrated_orders.to_i).to_s
puts " "
puts " "
puts "Orders without patients: " + orders_with_no_patients.length.to_s
puts "Migrated Orders without orderer:"        + orders_without_orderer.length.to_s
