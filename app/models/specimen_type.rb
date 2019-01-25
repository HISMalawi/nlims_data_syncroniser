class SpecimenType < ApplicationRecord
    def self.get_specimen_type_id(type)
        res = SpecimenType.where(:name => type)
        if !res.blank?
            return res[0]['id']
        end
    end
end
