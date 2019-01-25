class SpecimenStatus < ApplicationRecord
    def self.get_specimen_status_id(type)
        res = SpecimenStatus.where(:name => type)
        if !res.blank?
            return res[0]['id']
        end
    end
end
