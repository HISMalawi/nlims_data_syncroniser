Rails.application.routes.draw do
  root "home#home"
  post "/" => "home#home"

  post "/add_site"            => "home#add_site"
  get  "/add_site"             => "home#add_site"
  get  "/get_site_details"     => "home#get_site_details"
  get  "/edit_site"     => "home#edit_site_details"
  post "/edit_site"     => "home#edit_site_details"
  get  "/disable"        => "home#disable"
  post "/disable"        => "home#disable"
  post "/save_new_site" => "home#save_new_site"
  post '/get_sites' => "home#get_sites"
end
