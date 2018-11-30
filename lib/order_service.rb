module  OrderService

    def self.create_order(document,tracking_number,couch_id)
            puts "migrating--------------------------------"
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
                            time_entered: rst['date_result_entered']	 # ms['date_result_given']
                      )  
                  else
                    TestResult.create(
                            measure_id: measur_id,
                            test_id: tst_obj.id,
                            result: rst['result_value'],	
                            device_name: '',						
                            time_entered: rst['date_result_entered'] # ms['date_result_given']
                      )                     
                  end
                end    
              end                
            end
    end

end