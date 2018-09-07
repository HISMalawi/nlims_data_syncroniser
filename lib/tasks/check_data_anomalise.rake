namespace :check_data_anomalise do
  desc "TODO"
  task check_orders: :environment do
    res =  SlaveOrder.where.not(order_status_resolved_to: "not-resolved", sending_facility_resolved_to: "not-resolved", receiving_facility_resolved_to: "not-resolved", sample_type_resolved_to: "not-resolved", order_location_resolved_to: "not-resolved")
    if res.length > 0
      res.each do |data|
        next if data.order_resolve_status == "resolved"
       
        tracking_number = data.id
        patient_id = 1
        sample_id = SpecimenType.where(name: data.sample_type_resolved_to)[0].id
        sample_order_status_id = SpecimenStatus.where(name: data.order_status_resolved_to)[0].id
        order_location_id = Ward.where(name: data.order_location_resolved_to)[0].id
        sending_facility = data.sending_facility_resolved_to
        receiving_facility = data.receiving_facility_resolved_to
        date_created = data.date_drawn
        priority = data.priority
        sample_drawn_by_id = data.who_order_id
        sample_drawn_by_name = data.who_order_first_name + " " + data.who_order_last_name
        sample_drawn_by_phone_number = data.who_order_phone
        art_start_date = data.art_start_date
        requested_by = "user"
        date_sample_drawn = data.date
        health_facility_district = data.district
        dispatcher_id = 1
        dispatcher_name = "user" + " user"
        dispatcher_phone = 0000
        date_dispatched = data.dispatched_date
        created_at = date_created
        updated_at = date_created

        order  = Order.new
        order.id = tracking_number
        order.patient_id = patient_id
        order.specimen_type_id = sample_id
        order.ward_id = order_location_id
        order.specimen_status_id = sample_order_status_id
        order.target_lab = receiving_facility
        order.health_facility = sending_facility
        order.priority = priority
        order.date_created = date_created
        order.sample_drawn_by_id = sample_drawn_by_id
        order.sample_drawn_by_name = sample_drawn_by_name
        order.sample_drawn_by_phone_number = sample_drawn_by_phone_number
        order.art_start_date = art_start_date
        order.requested_by = requested_by
        order.date_sample_drawn = date_sample_drawn
        order.health_facility_district = health_facility_district
        order.dispatcher_id = dispatcher_id
        order.dispatcher_name = dispatcher_name
        order.dispatcher_phone_number = dispatcher_phone
        order.date_dispatched = date_dispatched
        order.created_at = created_at
        order.updated_at = updated_at
        order.save()
        
        SlaveOrder.where(id: tracking_number).update_all(order_resolve_status: "resolved")
        puts "----" + tracking_number
      end
    end
  end

  desc "TODO"
  task check_tests: :environment do
    res =  SlaveTest.where.not(test_type_resolved_to: "not-resolved",test_status_resolved_to: "not-resolved")
   
    if res.length > 0
      res.each do |data|
       
        next if data.resolving_status == "resolved" || Order.where(id: data.id)[0].blank?
        test_status_id = TestStatus.where(name: data.test_status_resolved_to)[0].id 
        test_type_id =  TestType.where(name: data.test_type)[0].id
        remarks = data.remarks
        tracking_number = data.id
        time_created = data.date_time_started
        created_at = time_created
        updated_at = time_created

        tst = Test.new
        tst.order_id = tracking_number
        tst.test_type_id = test_type_id
        tst.test_status_id = test_status_id
        tst.time_created = time_created
        tst.created_at = created_at
        tst.updated_at = updated_at
        tst.doc_id = "1"
        tst.save()

        SlaveTest.where(id: tracking_number, test_type: data.test_type).update_all(resolving_status: "resolved")
        puts tracking_number
      end
    end
  end

  desc "TODO"
  task check_test_results: :environment do
    res = SlaveTestResult.where.not(measure_resolved_to: "not-resolved")
   
    if res.length > 0
      res.each do |data|
        test_type_id = TestType.where(name: data.test_type)[0]
        measure_id = Measure.where(name: data.measure_resolved_to)[0]
        next if data.resolving_status == "resolved" || test_type_id.blank? || measure_id.blank?
        test_type_id = test_type_id.id
        result = Test.where(id: data.id, test_type_id: test_type_id)[0]
        next if result.blank?
        test_id = result.id
        value = data.measure_value
        time_entered = result.time_created

        tst = TestResult.new
        tst.test_id = test_id
        tst.measure_id = measure_id
        tst.result = measure_value
        tst.time_entered = time_entered
        tst.doc_id = "1"
        tst.created_at = time_entered
        tst.updated_at = time_created
        puts test_id
        SlaveTestResult.where(id: data.id, measure: data.measure_resolved_to).update_all(resolving_status: "resolved")
      
      end
    end
  end
end