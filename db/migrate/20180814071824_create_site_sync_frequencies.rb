class CreateSiteSyncFrequencies < ActiveRecord::Migration[5.2]
  def change
    create_table :site_sync_frequencies do |t|
      t.string :site     
      t.boolean :status
      t.timestamps
    end
  end
end
