# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'ardis/version'

Gem::Specification.new do |spec|
  spec.name          = "ardis"
  spec.version       = Ardis::VERSION
  spec.authors       = ["Jaime Cham"]
  spec.email         = ["jaime.cham@cardinalblue.com"]

  spec.summary       = %q{TODO: Write a short summary, because Rubygems requires one.}
  spec.description   = %q{TODO: Write a longer description or delete this line.}
  spec.homepage      = "TODO: Put your gem's website or public repo URL here."
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

  spec.add_development_dependency "bundler",        "~> 1.10"
  spec.add_development_dependency "rake",           "~> 10.0"
  spec.add_development_dependency "minitest"
  spec.add_development_dependency "sqlite3"
  spec.add_development_dependency "rr"

  spec.add_runtime_dependency     "activerecord",   "~> 4.2"
  spec.add_runtime_dependency     "redis",          "~> 3.2"
  spec.add_runtime_dependency     "redis-objects",  "1.1"     # There is an issue with 1.2
                                                              # see https://github.com/nateware/redis-objects/issues/185
  spec.add_runtime_dependency     "kaminari",       "~> 0.16"
  spec.add_runtime_dependency     "draper",         "~> 2.1"

end
