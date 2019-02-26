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

if !File.exists?("#{Rails.root}/public/sample_tracker")
    FileUtils.touch("#{Rails.root}/public/sample_tracker")   
end

if !File.exists?("#{Rails.root}/public/orders_with_no_tests.json")
    data = {}
    FileUtils.touch("#{Rails.root}/public/orders_with_no_tests.json")   
    data['Samples'] = []
    File.open("#{Rails.root}/public/orders_with_no_tests.json","w"){ |w|
        w.write(data.to_json)
    }
else
    data = {}
    data['Samples'] = []
    File.open("#{Rails.root}/public/orders_with_no_tests.json","w"){ |w|
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

   

if opt.downcase.strip == "n"
    samples = con.query("SELECT * FROM Lab_Sample") # retrieving the orders
    total = con.query("SELECT COUNT(*) total FROM Lab_Sample").first["total"].to_i # counting orders to be migrated
else
    previous_sam = File.read("#{Rails.root}/public/sample_tracker") 
    samples = con.query("SELECT * FROM Lab_Sample WHERE Sample_ID >='#{previous_sam}'")
    c = con.query("SELECT count(*) co FROM Lab_Sample WHERE Sample_ID >='#{previous_sam}'").first['co'].to_i
    total = c
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
    provider = 30
    #encounter_id = order_counter = con.query("SELECT encounter_id AS total FROM encounter ORDER BY encounter_id desc limit 1").as_json[0]['total'].to_i +  1
    uuid = get_uuid(con)
    encounter_type = con.query("SELECT encounter_type_id AS encout_type FROM encounter_type WHERE name ='LAB'").as_json[0]['encout_type']
    con.query("INSERT INTO encounter (encounter_type,patient_id,provider_id,location_id,encounter_datetime,creator,date_created,voided,uuid) 
                VALUES('#{encounter_type}','#{patient_id}','#{provider}','#{location_id}','#{encounter_date}','#{creator}','#{date_created}','#{voided}','#{uuid}')")
    encounter_id  = con.query("SELECT encounter_id AS total FROM encounter ORDER BY encounter_id desc limit 1").as_json[0]['total'].to_i
    return encounter_id
end

def self.health_data_tests_types(name)
    if name == 'Hep'
      return "Hepatitis C Test"
    elsif  name == "LFT"
      return "Liver Function Tests"
    elsif  name  == "Creat"
      return "Creatinine Kinase"
    elsif  name  == "Urinanal"
      return "Urine Macroscopy"
    elsif  name  == "MP"
      return "Microprotein"
    elsif  name  == "Full CSF analysis"
      return "CSF Analysis"
    elsif  name  == "Full CSF"
      return "CSF Analysis"
    elsif  name  == "Lactate"
      return "Lipogram"
    elsif  name  == "Crypto AG"
      return "Cryptococcus Antigen Test"
    elsif  name  == "U/E"
      return "Uric Acid"
    elsif  name  == "Full stool analysis"
      return "Stool Analysis"
    elsif  name  == "VDRL"
      return "Viral Load"
    elsif name == "HIV_viral_load"
      return "Viral Load"
    elsif  name  == "Cholest"
      return "Urine Chemistries"
    elsif  name  == "RBS"
      return "FBC"
    elsif  name  == "Tg"
      return "TT"
    elsif  name  == "Uri C/S"
      return "Urine Microscopy"
    elsif  name  == "AAFB (2nd)"
      return "Uric Acid"
    elsif  name  == "AAFB (1nd)"
      return "Uric Acid"
    elsif  name  == "AAFB (3nd)"
      return "Uric Acid"
    elsif  name  == "AAFB 4nd)"
      return "Uric Acid"
    elsif  name  == "AAFB (5nd)"
      return "Uric Acid"
    elsif  name  == "Blood NOS"
      return "FBC"
    elsif  name  == "G/XM"
      return "Urine Microscopy"
    else          
      return name
    end
end


def self.check_health_data_measures(m)
    if m == "CD4_count"
      return "CD4 Count"
    elsif m == "Bilirubin_total"
      return "Bilirubin Total(BIT))"
    elsif m == "Bilirubin_total"
      return "Bilirubin Total(BIT))"
    elsif  m == "Gamma Glutamyl transpeptidase"
      return "Lipase"
    elsif m == "Alanine_Aminotransferase"
      return "Lipase"
    elsif m == "Aspartate_Transaminase"
      return "Lipase"
    elsif  m == "CD3_percent"
      return "CD4 %"
    elsif m == "HIV_RNA_PCR"
     return "Viral Load"
    elsif m == "CD4_percent"
      return "CD4 %" 
    elsif  m == "CD8_percent"
      return "CD4 %" 
    elsif  m == "CD8Tube"
      return "CD4 %" 
    elsif  m == "CD8Tube"
      return "CD4 Count" 
    elsif  m == "WBC_percent"
     return "WBC"
    elsif m == "RBC"
     return "RBC"
    elsif  m == "RDW"
      return "RDW-CV" 
    elsif  m == "Platelet_count"
      return "Platelet Comments"
    elsif  m == "Phosphorus"
      return "Phosphorus (PHOS)"
    elsif m == "Neutrophil_percent"
      return "Neutrophils" 
    elsif  m == "Neutrophil_count"
      return "Neutrophils" 
    elsif m == "Monocyte_count"
      return "Monocytes"
    elsif m == "Malaria_Parasite_count"
      return "Malaria Species" 
    elsif  m == "Lymphocyte_percent"
      return "Lymphocytes"
    elsif  m == "Lymphocyte_count"
      return "Lymphocyte Count"
    elsif m == "Lactate"
      return "Lactatedehydrogenase(LDH)"
    elsif  m == "HepBsAg"
      return "Hepatitis B"
    elsif  m == "Hemoglobin"
      return "HB"
    elsif m == "WBC_count"
      return "WBC"
    elsif m == "Glucose_CSF"
      return "Glucose"  
    elsif  m == "Glucose_blood"
      return "Glucose"
    elsif  m == "Eosinophil_percent"
      return "Eosinophils" 
    elsif m == "Eosinophil_count"
      return "Eosinophils" 
    elsif m == "Cryptococcal_Antigen"
      return "CrAg" 
    elsif  m == "Cholesterol"
      return "Cholestero l(CHOL)"
    elsif m == "Urea_Nitrogen_blood"
      return "Glucose" 
    elsif  m == "Basophil_percent"
      return "Basophils"
    elsif m == "Basophil_count"
      return "Basophils"
    elsif m == "Monocyte_percent"
      return "Monocytes"
    elsif m == "Hematocrit"
      return "HB"
    elsif  m == "Triglycerides"
      return "Triglycerides(TG)" 
    elsif m == "Toxoplasma_IgG"
      return "50:50 Normal Plasma" 
    elsif  m == "Protein_total"
      return "Total Proteins" 
    elsif  m == "Glucose_CSF"
     return "Glucose"
    elsif m == "CD8_count"
      return "CD8 Count"
    elsif m == "Albumin"
      return "Albumin(ALB)"
    else
      return m
    end
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

# creating observation for inserting order to openmrs6fba15f8e769
def create_observation(con,person_id,encounter_id,location_id,concept_id,datetime)
    uuid = get_uuid(con)
    obs_datetime = datetime
    #obs_id = obs_counter = con.query("SELECT obs_id AS total FROM obs ORDER BY encounter_id desc limit 1").as_json[0]['total'].to_i +  1
    con.query("INSERT INTO obs (person_id,concept_id,encounter_id,obs_datetime,location_id,creator,date_created,voided,uuid)
        VALUES('#{person_id}','#{concept_id}','#{encounter_id}','#{obs_datetime}','#{location_id}','#{4}','#{obs_datetime}','#{0}','#{uuid}')
    ")
    obs_id = con.query("SELECT obs_id AS total FROM obs ORDER BY encounter_id desc limit 1").as_json[0]['total'].to_i +  1
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
    rrr = ""
    order_date = Time.new.strftime("%Y%m%d%H%M%S")
    
    if !row['AccessionNum'].blank?
       
        tests = con.query("SELECT * FROM LabTestTable WHERE AccessionNum = #{row['AccessionNum']}").as_json
        tests.each do |test_details|
            rrr = health_data_tests_types(test_details['TestOrdered'])
            tests_ordered.push(rrr)
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
                    test_statues[rrr] = status_details
                    results = con.query("SELECT * FROM Lab_Parameter WHERE Sample_ID = #{row['Sample_ID']}").as_json
                    results.each do |rst|        
                        r = con.query("SELECT TestName AS test_name FROM codes_TestType WHERE TestType='#{rst['TESTTYPE']}'").as_json
                        if !r.blank?
                            rst['TestName'] = r[0]['test_name']
                        end
                        rst['TestName'] = "Viral Load" if rst['TestName'] == "HIV_DNA_PCR"  || rst['TestName'] == "HIV_RNA_PCR" || rst['TestName'] == "HIV_viral_load"
                        rst['TestName'] =  check_health_data_measures(rst['TestName'])
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
                        formatted_results[rrr] = {
                                "results" => formatted_results_value,
                                "result_entered_by" => {
                                    :first_name => "",
                                    :last_name => "",
                                    :phone_number =>  "",
                                    :id => ""
                                    }
                        }
                    else
                        formatted_results[rrr] = {
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
                        test_statues[rrr][time] = {
                            "status" => "started",
                            "updated_by":  {
                                    :first_name => orderer['given_name'],
                                    :last_name => orderer['family_name'],
                                    :phone_number =>  "",
                                    :id =>test_details['OrderedBy']
                                    }
                        }
                    elsif  results_controller >= results.length
                        test_statues[rrr][time] = {
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
                    :date_of_birth => (patient['birthdate'].to_date.strftime('%Y%m%d') rescue nil),
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
                    order_location: "ART",
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
                    order_location: "ART",
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
                bart2_con.query("INSERT INTO orders (order_type_id,concept_id,orderer,encounter_id,instructions,start_date,discontinued,creator,date_created,voided,patient_id,accession_number,uuid)
                        VALUES('#{order_type}','#{concept_id}','#{orderer_id}','#{encouter_id}','#{c_id}','#{start_date}','#{discontinued}','#{creator}','#{date_created}','#{voided}','#{patient_id}','#{accession_number}','#{uuid}')")
                else
                    voided = 0
                    date_voided = date_given
                    voided_by =  orderer['id']  
                    void_reason = "result given"
                    #bart2_con.query("INSERT INTO orders (order_id,order_type_id,concept_id,orderer,encounter_id,instructions,start_date,discontinued,creator,date_created,voided,date_voided,voided_by,void_reason,patient_id,accession_number,uuid)
                    #VALUES('#{order_counter}','#{order_type}','#{concept_id}','#{orderer_id}','#{encouter_id}','#{c_id}','#{start_date}','#{discontinued}','#{creator}','#{date_created}','#{voided}','#{date_voided}','#{voided_by}','#{void_reason}','#{patient_id}','#{accession_number}','#{uuid}')")
                    bart2_con.query("INSERT INTO orders (order_type_id,concept_id,orderer,encounter_id,instructions,start_date,discontinued,creator,date_created,voided,patient_id,accession_number,uuid)
                        VALUES('#{order_type}','#{concept_id}','#{orderer_id}','#{encouter_id}','#{c_id}','#{start_date}','#{discontinued}','#{creator}','#{date_created}','#{voided}','#{patient_id}','#{accession_number}','#{uuid}')")
                end
                OrderService.create_order_v2(data,t_num,c_id)
    
                migrated_orders = migrated_orders + 1
            end         
        end
    else
            orders_without_tests.push(row['Sample_ID'])
            # order without tests
            count = JSON.parse(File.read("#{Rails.root}/public/orders_with_no_tests.json"))
            count['Samples'] = count['Samples'].push(row["Sample_ID"])

            File.open("#{Rails.root}/public/orders_with_no_tests.json", 'w') {|f|
            f.write(count.to_json)}
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
puts "Orders without tests: "        + orders_without_tests.length.to_s
