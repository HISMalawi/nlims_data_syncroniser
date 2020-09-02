module  OrderService

    def self.create_order_v2(params, tracking_number, couch_id)
      couch_order = 0
      ActiveRecord::Base.transaction do 
 
            npid = params[:patient][:id]
            patient_obj = Patient.where(:patient_number => npid)          
            patient_obj = patient_obj.first unless patient_obj.blank?

                  if patient_obj.blank?
                        patient_obj = patient_obj.create(
                                          patient_number: npid,
                                          name: params[:patient][:first_name] +" "+ params[:patient][:last_name],
                                          email: '' ,
                                          dob: params[:patient][:date_of_birth],
                                          gender: params[:patient][:gender],
                                          phone_number: params[:patient][:phone_number],
                                          address: "",
                                          external_patient_number:  "" 
                                          )
                           
                  end

                              
            who_order = {
                  :first_name => params[:who_order_test][:first_name],
                  :last_name => params[:who_order_test][:last_name],
                  :phone_number => params[:who_order_test][:phone_number],
                  :id => params[:who_order_test][:id_number]
            }

            patient = {
                  :first_name => params[:patient][:first_name],
                  :last_name => params[:patient][:last_name],
                  :phone_number => params[:patient][:phone_number],
                  :id => npid,
                  :email => params[:patient][:email],
                  :gender => params[:patient][:gender] 
            }
            sample_status =  {}
            test_status = {}
            time = params[:date_created] 
            sample_status[time] = {
                  "status" => "Drawn",
                        "updated_by":  {
                              :first_name => params[:who_order_test][:first_name],
                              :last_name => params[:who_order_test][:last_name],
                              :phone_number => params[:patient][:phone_number],
                              :id => params[:who_order_test][:id] 
                              }
            }


            sample_type_id = SpecimenType.get_specimen_type_id(params[:sample_type])
            sample_status_id = SpecimenStatus.get_specimen_status_id(params[:sample_status])
           

      sp_obj =  Speciman.create(
                  :tracking_number => tracking_number,
                  :specimen_type_id =>  sample_type_id,
                  :specimen_status_id =>  sample_status_id,
                  :couch_id => '',
                  :ward_id => Ward.get_ward_id(params[:order_location]),
                  :priority => params[:priority],
                  :drawn_by_id =>  params[:who_order_test][:id],
                  :drawn_by_name =>  params[:who_order_test][:first_name].to_s + " " + params[:who_order_test][:last_name].to_s,
                  :drawn_by_phone_number => params[:patient][:phone_number], 
                  :target_lab => params[:receiving_facility],
                  :art_start_date => Time.now,
                  :sending_facility => params[:sending_facility],
                  :requested_by =>  params[:requesting_clinician],
                  :district => params[:district],
                  :date_created => time
            )

            
                  res = Visit.create(
                           :patient_id => npid,
                           :visit_type_id => '',
                           :ward_id => Ward.get_ward_id(params[:order_location])
                        )
                  visit_id = res.id
            var_checker = false
                  
            params[:tests].each do |tst|
                  tst = tst.gsub("&amp;",'&')                 
                  tstt = health_data_tests_types(tst)                 
                  status = check_test(tstt)
                  if status == false
                        details = {}
                        details[time] = {
                              "status" => "Drawn",
                              "updated_by":  {
                                :first_name => params[:who_order_test][:first_name],
                                :last_name => params[:who_order_test][:last_name],
                                :phone_number => params[:patient][:phone_number],
                                :id => params[:who_order_test][:id] 
                                }
                        }
                        test_status[tst] = details                  
                        rst = TestType.get_test_type_id(tstt)
                        rst2 = TestStatus.get_test_status_id('drawn')

                        t = Test.create(
                              :specimen_id => sp_obj.id,
                              :test_type_id => rst,
                              :patient_id => patient_obj.id,
                              :created_by => params[:who_order_test][:first_name].to_s + " " + params[:who_order_test][:last_name].to_s,
                              :panel_id => '',
                              :time_created => time,
                              :test_status_id => rst2
                        )
                        if !params[:test_results].blank?
                          if !r = params[:test_results][tst].blank?
                            r = params[:test_results][tst]
                            r = r['results']      
                                         
                            measure_name = r.keys
                            measure_name.each do |m|            
                                          
                              v = r[m]
                              r_value = v[:result_value]
                              date = v[:date_result_entered]
                              if !m.blank?
				mm = check_health_data_measures(m)
                              	puts m
			      	puts "hello------------"
			      	m = Measure.where(name: mm).first
                              	m = m.id
                              	TestResult.create(
                                 :test_id => t.id,
                                 :measure_id => m,
                                 :result => r_value,
                                 :time_entered => date,
                                 :device_name => ''
                               )
			     end
                            end
                            var_checker = true
                          end
                        end

                        if var_checker == true
                          ts_st = TestStatus.where(name: 'verified').first
                          tv = Test.find_by(:id => t.id)
                          tv.test_status_id =  ts_st.id
                          tv.save()
                          var_checker = false
                        end
                  else
                        pa_id = PanelType.where(name: tstt).first
                        res = TestType.find_by_sql("SELECT test_types.id FROM test_types INNER JOIN panels 
                                                      ON panels.test_type_id = test_types.id
                                                      INNER JOIN panel_types ON panel_types.id = panels.panel_type_id
                                                      WHERE panel_types.id ='#{pa_id.id}'")
                        res.each do |tt|
                              details = {}
                              details[time] = {
                                    "status" => "Drawn",
                                    "updated_by":  {
                                        :first_name => params[:who_order_test][:first_name],
                                        :last_name => params[:who_order_test][:last_name],
                                        :phone_number => params[:patient][:phone_number],
                                        :id => params[:who_order_test][:id] 
                                        }
                                }
                              test_status[tst] = details                  
                              #rst = TestType.get_test_type_id(tt)
                              rst2 = TestStatus.get_test_status_id('drawn')
                              updater =  params[:who_order_test][:first_name] + " " + params[:who_order_test][:last_name]  rescue nil
                              t= Test.create(
                                    :specimen_id => sp_obj.id,
                                    :test_type_id => tt.id,
                                    :patient_id => patient_obj.id,
                                    :created_by => updater,
                                    :panel_id => '',
                                    :time_created => time,
                                    :test_status_id => rst2
                              )

                              if !params[:test_results].blank?
                                if !params[:test_results][tst].blank?
                                  r = params[:test_results][tst]
                                  r = r['results']
                                  measure_name = r.keys
                                  measure_name.each do |m|
                                    v = r[m]
                                    r_value = v[:result_value]
                                    date = v[:date_result_entered]
				    if !m.blank?
					puts m
					puts "hello"
	                                    mm = check_health_data_measures(m)                                    
        	                            m = Measure.where(name: mm).first
                	                    m = m.id
                        	        TestResult.create(
                                	   :test_id => t.id,
                                           :measure_id => m,
                                     	   :result => r_value,
                                           :time_entered => date,
                                           :device_name => ''
                                         )
				   end
                                  end
                                  var_checker = true
                                end
                              end
      
                              if var_checker == true
                                ts_st = TestStatus.where(name: 'verified').first
                                tv = Test.find_by(:id => t.id)
                                tv.test_status_id =  ts_st.id
                                tv.save()
                                var_checker = false
                              end
                        end
                  end
            end

           

            sp = Speciman.find_by(:tracking_number => tracking_number)
            sp.couch_id = couch_id
            sp.save()
            couch_order = couch_id
      end              

      return [true,tracking_number,couch_order]
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
        return "CD4` Count"
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
     elsif m == "India_Ink"
        return "India Ink"
     elsif m == "Alkaline_Phosphatase"
	return "Alkaline Phosphate(ALP)"
     elsif m == "Bilirubin_direct"
	return "Bilirubin Direct(BID)"
     elsif m == "Glutamyl_Transferase"
	return "GGT/r-GT" 
     elsif m == "CD8_CD3_ratio"
	return "CD8 Count"
     elsif m == "CD4_CD3_ratio"
	return "CD4 Count"
     elsif m == "CD4_CD8_ratio"
        return "CD8 Count"
     elsif  m == "Carbon_Dioxide"
	return "Other"
     elsif m == "CD3_count"
        return "CD3 Count"
    elsif m == "RPR_Syphilis"
	return "VDRL"
    elsif m  == "CD4 lube" 
        return "CD4 Count" 
    elsif m == "ControlRunControlLotID"
	return "Control"
     else
        return m
      end
    end

    def self.check_test(tst)

      res = PanelType.find_by_sql("SELECT * FROM panel_types WHERE name ='#{tst}'")

      if res.length > 0
            return true
      else
            return false
      end

    end

    def self.create_order(document,tracking_number,couch_id)
    
            document = document['doc']            
            patient_id = document['patient']['id']
            patient_f_name = document['patient']['first_name']
            patient_l_name = document['patient']['last_name']
            patient_gender = document['patient']['gender']
            patient_email = document['patient']['email']
            patient_phone = document['patient']['phone_number']

            ward = document['order_location']
            district  = document['district']
            date_created = document['date_created']
            priority = document['priority']
            receiving_facility = document['receiving_facility']
            sample_status = document['sample_status']
            sample_type = document['sample_type']
            sending_facility = document['sending_facility']

            who_order_id = document['who_order_test']['id']
            who_order_f_name = document['who_order_test']['first_name']
            who_order_l_name = document['who_order_test']['last_name']
            who_order_phone_number = document['who_order_test']['phone_number']
            
            ward_id = OrderService.get_ward_id(ward)
            sample_type_id = OrderService.get_specimen_type_id(sample_type)
            sample_status_id = OrderService.get_specimen_status_id(sample_status)
            
          sp = Speciman.create(
                  :tracking_number => tracking_number,
                  :specimen_type_id =>  sample_type_id,
                  :specimen_status_id =>  sample_status_id,
                  :couch_id => couch_id,
                  :priority => priority,
                  :drawn_by_id => who_order_id,
                  :drawn_by_name =>  who_order_f_name + " " + who_order_l_name,
                  :drawn_by_phone_number => who_order_phone_number,
                  :target_lab => receiving_facility,
                  :art_start_date => Time.now,
                  :sending_facility => sending_facility,
                  :requested_by => "",
                  :ward_id => 1,
                  :district => district,
                  :date_created => date_created
            )
     
            tests = document['test_statuses']
            patient_obj = Patient.where(:patient_number => patient_id)                
            patient_obj = patient_obj.first unless patient_obj.blank?
                  if patient_obj.blank?
                        patient_obj = patient_obj.create(
                                          patient_number: patient_id,
                                          name:  patient_f_name  +" "+  patient_l_name,
                                          email:  patient_email,
                                          dob: Time.new.strftime("%Y%m%d%H%M%S"),
                                          gender: patient_gender,
                                          phone_number: patient_phone,
                                          address: "",
                                          external_patient_number:  "" 

                                          )                           
                  end
            p_id = patient_obj.id
            tests.each do |tst_name,tst_value|              
              test_id = OrderService.get_test_type_id(tst_name)
              test_status = tst_value[tst_value.keys[tst_value.keys.count - 1]]['status']
              test_status_id = OrderService.get_status_id(test_status)
              updated_by_id = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['id']
              updated_by_first_name = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['first_name']
              updated_by_last_name = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['last_name']
              updated_by_phone_number = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['phone_number']
              
              tst_obj =  Test.create(
                        :specimen_id => sp.id,
                        :test_type_id => test_id,
                        :patient_id => p_id,
                        :created_by => who_order_f_name + " " + who_order_l_name,
                        :panel_id => '',
                        :time_created => date_created,
                        :test_status_id => test_status_id 
                  )

              tst_value.each do |updated_at, value|                    
                  status = value['status']
                  updated_by_id = value['updated_by']['id']
                  updated_by_f_name = value['updated_by']['first_name']
                  updated_by_l_name = value['updated_by']['last_name']
                  updated_by_phone_number = value['updated_by']['phone_number']
                  test_status_id = OrderService.get_status_id(status)    
                  test_i = test_id
                  TestStatusTrail.create(
                    test_id: tst_obj.id,
                    time_updated: date_created, # updated at
                    test_status_id: test_status_id,
                    who_updated_id:  updated_by_id.to_s,
                    who_updated_name: updated_by_f_name.to_s + " " + updated_by_l_name.to_s,
                    who_updated_phone_number: updated_by_phone_number		       
                  )
              end
             
              test_results = document['test_results'][tst_name]
              
              unless test_results.blank?
                if test_results['results'].keys.count > 0
                  test_results['results'].keys.each do |ms|                  
                    measur_id = OrderService.get_measure_id(ms)
                    rst = test_results['results'][ms]                              
                    TestResult.create(
                            measure_id: measur_id,
                            test_id: tst_obj.id,
                            result: rst['result_value'],	
                            device_name: '',						
                            time_entered: '2018-09-21 04:38:02' # ms['date_result_given']
                    )
                  end
                end   
              end                
            end
       puts "---------done------------"
    end

    def self.check_order(tracking_number)
      res =  Speciman.find_by_sql("SELECT id AS track_id FROM specimen WHERE tracking_number='#{tracking_number}'")
      if !res.blank?
        return true
      else
        return false
      end
    end

    def self.save_visit(npid,visit_type, ward)
      obj = Visit.create(
            patient_id: npid,
            visit_type_id:  visit_type,
            ward_id: ward
      )
      return obj.id
    end

    def self.get_ward_id(ward_name)
      res  = Ward.find_by_sql("SELECT id AS ward_id FROM wards WHERE name='#{ward_name}'")
      if !res.blank?
         return res[0]['ward_id']
      else

      end       
    end


    def self.get_test_type_id(name)
      res = TestType.find_by_sql("SELECT id AS test_id FROM test_types WHERE name='#{name}'")
      if !res.blank?
        return res[0]['test_id']
      end
    end

    def self.get_status_id(name)
      res = TestStatus.find_by_sql("SELECT id AS status_id FROM test_statuses WHERE name='#{name}'")
      if !res.blank?
        return res[0]['status_id']
      end
    end

    def self.get_measure_id(name)
      res = Measure.find_by_sql("SELECT id AS measure_id FROM measures WHERE name='#{name}'")
      if !res.blank?
        return res[0]['measure_id']
      end
    end

    def self.get_specimen_type_id(name)
      res = SpecimenType.find_by_sql("SELECT id AS spc_id FROM specimen_types WHERE name='#{name}'")
      if !res.blank?
        return res[0]['spc_id']
      end
    end


    def self.get_specimen_status_id(name)
      res = SpecimenStatus.find_by_sql("SELECT id AS spc_id FROM specimen_statuses WHERE name='#{name}'")
      if !res.blank?
        return res[0]['spc_id']
      end
    end


    def self.update_order(document,tracking_number)
            puts "migrating v2--------------------------------"
            document = document['doc']            
            patient_id = document['patient']['id']
            patient_f_name = document['patient']['first_name']
            patient_l_name = document['patient']['last_name']
            patient_gender = document['patient']['gender']
            patient_email = document['patient']['email']
            patient_phone = document['patient']['phone_number']

            ward = document['order_location']
            district  = document['districy']
            date_created = document['date_created']
            priority = document['priority']
            receiving_facility = document['receiving_facility']
            sample_status = document['sample_status']
            sample_type = document['sample_type']
            sending_facility = document['sending_facility']

            who_order_id = document['who_order_test']['id']
            who_order_f_name = document['who_order_test']['first_name']
            who_order_l_name = document['who_order_test']['last_name']
            who_order_phone_number = document['who_order_test']['phone_number']
            
            ward_id = OrderService.get_ward_id(ward)
                       
            sample_type_id = OrderService.get_specimen_type_id(sample_type)
            sample_status_id = OrderService.get_specimen_status_id(sample_status)

           Speciman.where(:tracking_number => tracking_number).update_all(
                        :tracking_number => tracking_number,
                        :specimen_type_id =>  sample_type_id,
                        :specimen_status_id =>  sample_status_id,
                        :priority => priority,
                        :drawn_by_id => who_order_id,
                        :drawn_by_name =>  who_order_f_name + " " + who_order_l_name,
                        :drawn_by_phone_number => who_order_phone_number,
                        :target_lab => receiving_facility,
                        :art_start_date => Time.now,
                        :sending_facility => sending_facility,
                        :ward_id => ward_id,
                        :requested_by => "",
                        :district => district,
                        :date_created => date_created
            )
            sp = Speciman.find_by(:tracking_number =>  tracking_number)
            patient_obj = Patient.where(:patient_number => patient_id)                
            patient_obj = patient_obj.first unless patient_obj.blank?
                  if patient_obj.blank?
                        patient_obj = patient_obj.create(
                                          patient_number: patient_id,
                                          name:  patient_f_name  +" "+  patient_l_name,
                                          email:  patient_email,
                                          dob: Time.new.strftime("%Y%m%d%H%M%S"),
                                          gender: patient_gender,
                                          phone_number: patient_phone,
                                          address: "",
                                          external_patient_number:  "" 

                                          )                           
                  end
            p_id = patient_obj.id
           
            tests = document['test_statuses']
            
            tests.each do |tst_name,tst_value|              
              test_id = OrderService.get_test_type_id(tst_name)
              test_status = tst_value[tst_value.keys[tst_value.keys.count - 1]]['status']
              test_status_id = OrderService.get_status_id(test_status)
              updated_by_id = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['id']
              updated_by_first_name = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['first_name']
              updated_by_last_name = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['last_name']
              updated_by_phone_number = tst_value[tst_value.keys[tst_value.keys.count - 1]]['updated_by']['phone_number']
              tst_obj =  Test.where(:specimen_id => sp.id, :test_type_id => test_id).first
              Test.where(:specimen_id => sp.id, :test_type_id => test_id).update_all(
                      :specimen_id => sp.id,
                      :test_type_id => test_id,
                      :patient_id => p_id,
                      :created_by => who_order_f_name + " " + who_order_l_name,
                      :panel_id => '',
                      :time_created => date_created,
                      :test_status_id => test_status_id 
              )
              
              count = tst_value.keys.count
              t_count = TestStatusTrail.find_by_sql("SELECT count(*) AS t_count FROM test_status_trails WHERE test_id='#{tst_obj.id}'")[0]['t_count']
   
              if ((count - t_count) == 1) && count > t_count
                value = tst_value[tst_value.keys[count - 1 ]]
                status = value['status']
                  updated_by_id = value['updated_by']['id']
                  updated_by_f_name = value['updated_by']['first_name']
                  updated_by_l_name = value['updated_by']['last_name']
                  updated_by_phone_number = value['updated_by']['phone_number']
                  test_status_id = OrderService.get_status_id(status)    
                  test_i = test_id
                  TestStatusTrail.create(
                    test_id: tst_obj.id,
                    time_updated: date_created, # updated at Test.where()
                    test_status_id: test_status_id,
                    who_updated_id:  updated_by_id.to_s,
                    who_updated_name: updated_by_f_name.to_s + " " + updated_by_l_name.to_s,
                    who_updated_phone_number: updated_by_phone_number		       
                  )
              elsif ((count - t_count) > 1) && count > t_count
                control = 0
                tst_value.each do |updated_at, value|
                  control = control + 1
                  next if control <= t_count                    
                    status = value['status']
                    updated_by_id = value['updated_by']['id']
                    updated_by_f_name = value['updated_by']['first_name']
                    updated_by_l_name = value['updated_by']['last_name']
                    updated_by_phone_number = value['updated_by']['phone_number']
                    test_status_id = OrderService.get_status_id(status)    
                    test_i = test_id
                    TestStatusTrail.create(
                      test_id: tst_obj.id,
                      time_updated: date_created, # updated at
                      test_status_id: test_status_id,
                      who_updated_id:  updated_by_id.to_s,
                      who_updated_name: updated_by_f_name.to_s + " " + updated_by_l_name.to_s,
                      who_updated_phone_number: updated_by_phone_number		       
                    )
                    
                end
              end
             
              test_results = document['test_results'][tst_name]
              unless test_results.blank?
                if test_results['results'].keys.count > 0               
                  test_results['results'].keys.each do |ms|                  
                    measur_id = OrderService.get_measure_id(ms)
                    rst = test_results['results'][ms]  
                    res = TestResult.find_by_sql("SELECT count(*) AS t_count FROM test_results WHERE measure_id='#{measur_id}' AND test_id='#{tst_obj.id}'")[0]                           
                    if res['t_count'] != 0
                      TestResult.where(:measure_id => measur_id, :test_id => tst_obj.id).update_all(
                              measure_id: measur_id,
                              test_id: tst_obj.id,
                              result: rst['result_value'],	
                              device_name: '',						
                              time_entered: rst['date_result_entered'] || test_results['date_result_entered']
                        )  
                    else
                      TestResult.create(
                              measure_id: measur_id,
                              test_id: tst_obj.id,
                              result: rst['result_value'],	
                              device_name: '',						
                              time_entered: rst['date_result_entered'] || test_results['date_result_entered']
                        )                     
                    end
                  end
                end   
              end                
            end
    end

end
