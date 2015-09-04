module Ardis

class ContainerProxy

  attr_accessor :klass, :id

  def initialize klass, id
    self.klass = klass
    self.id    = id
  end

  def proxy_for? klass
    klass.ancestors.include?(self.klass)
  end

end

end