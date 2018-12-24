require "rest-client"
require 'test_catelog_service.rb'
class TestCatelogExtractor
  include SuckerPunch::Job
  workers 1
 
  def perform
  
    begin
       
      dat = TestCatelog.extract_catelog
      if !dat.blank?
        if !File.exists?("#{Rails.root}/../nlims_controller/public/test_catelog.json")
          FileUtils.touch("#{Rails.root}/../nlims_controller/public/test_catelog.json")          
        end

        File.open("#{Rails.root}/../nlims_controller/public/test_catelog.json",'w'){ |f|
          f.write(dat.to_json)
        }  
      end

      puts "hello-----------"

        TestCatelogExtractor.perform_in(0)
    rescue      
        TestCatelogExtractor.perform_in(0)
    end   
  end
 
end
