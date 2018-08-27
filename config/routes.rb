Rails.application.routes.draw do
  root "home#home"

  post "/add_site"            => "home#add_site"
  get  "/add_site"             => "home#add_site"
  get  "/get_site_details"     => "home#get_site_details"
  get  "/edit_site"     => "home#edit_site_details"
  post "/edit_site"     => "home#edit_site_details"
  get  "/disable"        => "home#disable"
  post "/disable"        => "home#disable"

end
