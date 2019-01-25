class Ward < ApplicationRecord
    def self.get_ward_id(type)
        res = Ward.where(:name => type)
        if !res.blank?
            return res[0]['id']
        end
    end
end
