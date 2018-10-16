class CreateTestStatusTrails < ActiveRecord::Migration[5.2]
  def change
    create_table :test_status_trails do |t|

      t.timestamps
    end
  end
end
