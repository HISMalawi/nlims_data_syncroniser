module TestCatelog

    def self.extract_catelog
        puts "this is good"
        
        data = {}
        tests = ""
        spc = SpecimenType.find_by_sql("SELECT * FROM specimen_types")
        if !spc.blank?
            spc.each do |d|
               
                specimen_name = d['name']
                specimen_id = d['id']
                tsts = TestType.find_by_sql("SELECT test_types.name,test_types.id FROM test_types 
                                             INNER JOIN testtype_specimentypes ON 
                                             test_types.id = testtype_specimentypes.test_type_id
                                             WHERE testtype_specimentypes.specimen_type_id ='#{specimen_id}'")
                tests = tsts.collect do |t|
                        t['name']
                    end 
            
                data[specimen_name] = tests
                tests = ""
            end
           
        end
        puts data
        puts "------------checkkiug checkuibg---------"
        return data if !data.blank?
        return [] if data.blank?     
    end

end
