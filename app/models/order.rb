class Order < CouchRest::Model::Base
    use_database "order"

    property :tracking_number, String
    property :sample_status, String
    property :sample_source, String # sample_souce: Added to satisfy for TB lab request
    property :sample_date, String #sample date: Added to satis
    property :date_created, String
    property :sending_facility, String
    property :receiving_facility, String
    property :tests, {}
    property :recommended_examination, String #recommended_examination: Added to satisfy for TB Lab request
    property :test_results, {}
    property :patient, {}
    property :treatment_history, String #treatment_history: Added to satisfy for TB Lab request
    property :order_location, String
    property :district, String
    property :priority, String
    property :who_order_test, {}
    property :who_dispatched_test, String
    property :sample_type, String
    property :sample_statuses, {}
    property :test_statuses, {}
end
