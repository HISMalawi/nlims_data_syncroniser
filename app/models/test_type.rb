class TestType < ApplicationRecord

    def self.get_test_type_id(type)
        res = TestType.where(:name => type)
        if !res.blank?
            return res[0]['id']
        end
    end
end
