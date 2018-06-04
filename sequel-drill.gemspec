# -*- encoding: utf-8 -*-
require File.expand_path('../lib/sequel-drill/version', __FILE__)

Gem::Specification.new do |gem|
  gem.name          = "sequel-drill"
  gem.version       = Sequel::Drill::VERSION

  gem.authors       = ["Joe Auty"]
  gem.email         = ["joe@thinkdataworks.com"]
  gem.description   = %q{Sequel adapter for Apache Drill}
  gem.summary       = %q{Sequel adapter for Apache Drill based on sequel-vertica}
  gem.homepage      = "https://github.com/joeauty/sequel-drill"
  gem.license       = "MIT"

  gem.requirements  = "Apache Drill (tested on v1.10.x)"
  gem.required_ruby_version = '>= 1.9.3'

  gem.add_runtime_dependency "sequel", "~> 4.14"

  gem.add_development_dependency "rake", ">= 10"
  gem.add_development_dependency "rspec" , "~> 3.1"
  gem.add_development_dependency "pry"
  gem.add_development_dependency "webhdfs", "0.8.0"
  gem.add_development_dependency "http-cookie"

  gem.executables   = `git ls-files -- bin/*`.split("\n").map{ |f| File.basename(f) }
  gem.files         = `git ls-files`.split("\n")
  gem.test_files    = `git ls-files -- {spec}/*`.split("\n")

  gem.require_paths = ["lib"]
end
