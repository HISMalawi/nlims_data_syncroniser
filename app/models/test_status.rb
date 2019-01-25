class TestStatus < ApplicationRecord
    def self.get_test_status_id(type)
        res = TestStatus.where(:name => type)
        if !res.blank?
            return res[0]['id']
        end
    end
end
