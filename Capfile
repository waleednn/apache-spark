require 'bundler/setup'
require 'capistrano_recipes/deploy/packserv'

set :application, "spark"
set :user, "deploy"
set :shared_work_path, "/u/apps/spark/shared/work"
set :shared_logs_path, "/u/apps/spark/shared/log"
set :shared_conf_path, "/u/apps/spark/shared/conf"
set :gateway, nil

MAINTENANCE = ["dn04.chi.shopify.com", "dn07.chi.shopify.com", "dn08.chi.shopify.com", "dn11.chi.shopify.com", "dn14.chi.shopify.com"] # Node is up but should not be part of the production cluster
DECOMISSIONED = ["dn06.chi.shopify.com", "dn37.chi.shopify.com"] # Node is down don't try to send code
BROKEN = MAINTENANCE + DECOMISSIONED

task :production do
  role :app, *((4..47).map {|i| "dn%02d.chi.shopify.com" % i } - (["dn05.chi.shopify.com"] + BROKEN))
  role :master, "dn05.chi.shopify.com"
  role :history, "dn05.chi.shopify.com"
  role :code, "hadoop-etl1.chi.shopify.com", "spark-etl1.chi.shopify.com", "reports-reportify-etl3.chi.shopify.com", "reports-reportify-skydb4.chi.shopify.com", "platfora2.chi.shopify.com", *MAINTENANCE
end

task :staging do
  role :app, "54.197.179.141", "107.21.65.199", "54.166.211.228"
  role :master, "54.167.53.104"
end

namespace :deploy do
  task :prevent_gateway do
    set :gateway, nil
  end

  task :restart_master, :roles => :master, :on_no_matching_servers => :continue do
    run "sv-sudo restart spark-master"
  end

  task :restart_history, :roles => :history, :on_no_matching_servers => :continue do
    run "sv-sudo restart spark-history"
  end

  task :restart, :roles => :app do
    run "sv-sudo restart spark-worker"
  end

  task :symlink_shared do
    run "ln -nfs #{shared_work_path} #{release_path}/work"
    run "ln -nfs #{shared_logs_path} #{release_path}/logs"
    run "rm -rf #{release_path}/conf && ln -nfs #{shared_conf_path} #{release_path}/conf"
  end

  after 'deploy:initialize_variables', 'deploy:prevent_gateway' # capistrano recipes packserv deploy always uses a gateway
  before  'deploy:symlink_current', 'deploy:symlink_shared'
  before 'deploy:restart', 'deploy:restart_master', 'deploy:restart_history'
end
