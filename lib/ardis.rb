
require_relative 'ardis/version'
require_relative 'ardis/attr_strategy'
require_relative 'ardis/container_proxy'

require_relative 'ardis/base_series'
require_relative 'ardis/redis_adapter'

require_relative 'ardis/dsl'

module Ardis
  include DSL

  def self.included(klass)
    klass.extend ListDSL
    klass.extend SortedSetDSL
    klass.extend SetDSL
    klass.extend ReferenceDSL
  end
end
