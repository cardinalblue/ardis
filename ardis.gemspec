# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ardis/version'

Gem::Specification.new do |spec|
  spec.name          = "ardis"
  spec.version       = Ardis::VERSION
  spec.authors       = ["Jaime Cham"]
  spec.email         = ["jaime.cham@cardinalblue.com"]

  spec.summary       = %q{ Allows easy integration to store indicies to ActiveRecord objects in Redis }
  spec.homepage      = "https://github.com/cardinalblue/ardis"
  spec.license       = "MIT"

  # Prevent pushing this gem to RubyGems.org by setting 'allowed_push_host', or
  # delete this section to allow pushing this gem to any host.
  if spec.respond_to?(:metadata)
    spec.metadata['allowed_push_host'] = "TODO: Set to 'http://mygemserver.com'"
  else
    raise "RubyGems 2.0 or newer is required to protect against public gem pushes."
  end

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler",        ">= 1.10"
  spec.add_development_dependency "rake",           ">= 10.0"
  spec.add_development_dependency "minitest"
  spec.add_development_dependency "sqlite3"
  spec.add_development_dependency "rr"

  spec.add_runtime_dependency     "activerecord",   ">= 4.2"
  spec.add_runtime_dependency     "activemodel-serializers-xml"
  spec.add_runtime_dependency     "redis",          ">= 3.2"
  spec.add_runtime_dependency     "redis-objects",  ">= 1.2.1"
  spec.add_runtime_dependency     "kaminari",       ">= 0.16"
  spec.add_runtime_dependency     "draper",         ">= 3.0.0.pre1"
end
